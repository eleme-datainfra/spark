/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.orc

import java.io.IOException
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.util.StringUtils
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object OrcUtil extends Logging {

  case class StripeSplit(path: String, offset: Long, length: Long, hosts: Array[String])

  def getSplit(broadcastedConf: SerializableConfiguration, path: String): Array[StripeSplit] = {
    val conf = broadcastedConf.value
    val fs = FileSystem.get(conf)
    val inputPath = new Path(path)
    var currentOffset = -1L
    var currentLength = 0L
    val blockSize = conf.getLong("hive.exec.orc.default.block.size", 256 * 1024 * 1024)
    var splits = new ArrayBuffer[StripeSplit]()
    val inputFiles = new ArrayBuffer[Path]()
    if (fs.isDirectory(inputPath)) {
      inputFiles ++= SparkHadoopUtil.get.listLeafStatuses(fs, inputPath)
        .filter(f => !f.getPath.getName.startsWith("_") && !f.getPath.getName.startsWith("."))
        .map(f => f.getPath)
    } else {
      inputFiles += inputPath
    }
    inputFiles.foreach { file =>
      val orcReader = OrcFile.createReader(fs, file)
      val locations = ShimLoader.getHadoopShims.getLocationsWithOffset(fs, fs.getFileStatus(file))

      locations.values().asScala.foreach { block =>
        logDebug(s"${file.toString} {${block.getOffset}, ${block.getLength}")
      }

      orcReader.getStripes.asScala.foreach { stripe =>
        currentOffset = stripe.getOffset()
        currentLength = stripe.getLength()
        logDebug(s"Stripe {${currentOffset}, ${currentLength}")
        splits += createSplit(file.toString, currentOffset, currentLength, blockSize, locations)
      }
    }

    splits.toArray
  }

  def createSplit(path: String,
                  offset: Long,
                  length: Long,
                  blockSize: Long,
                  locations: util.TreeMap[java.lang.Long, BlockLocation]): StripeSplit = {
    var hosts: Array[String] = null
    val startEntry = locations.floorEntry(offset)
    val start = startEntry.getValue
    if ((offset + length) <= (start.getOffset + start.getLength)) {
      hosts = start.getHosts
    } else {
      val endEntry = locations.floorEntry(offset + length)
      // get the submap
      val navigableMap = locations.subMap(startEntry.getKey, true, endEntry.getKey, true)
      // Calculate the number of bytes in the split that are local to each host
      val sizes = new util.HashMap[String, LongWritable]()
      var maxSize = 0L
      navigableMap.values().asScala.foreach { block =>
        val overlap = getOverlap(offset, length, block.getOffset(), block.getLength())
        if (overlap > 0) {
          block.getHosts.foreach { host =>
            var size = sizes.get(host)
            if (size == null) {
              size = new LongWritable()
              sizes.put(host, size)
            }
            size.set(size.get() + overlap)
            maxSize = Math.max(maxSize, size.get())
          }
        } else {
          throw new IOException(s"File ${path}  {${offset}, ${length}} " +
            s"should have had overlap on block {${block.getOffset}, ${block.getLength})")
        }
      }

      // filter the list of locations to those that have at least 70% of the max
      val threshold = (maxSize * 0.7).toLong
      val hostList = new ArrayBuffer[String]()
      navigableMap.values().asScala.foreach { block =>
        block.getHosts.foreach { host =>
          if (sizes.containsKey(host)) {
            if (sizes.get(host).get() >= threshold) {
              hostList += host
            }
            sizes.remove(host)
          }
        }
      }
      hosts = hostList.toArray
    }
    StripeSplit(path, offset, length, hosts)
  }

  def getOverlap(offset1: Long, length1: Long,
                 offset2: Long, length2: Long): Long = {
    val end1 = offset1 + length1
    val end2 = offset2 + length2
    if (end2 <= offset1 || end1 <= offset2) {
      return 0
    } else {
      return Math.min(end1, end2) - Math.max(offset1, offset2)
    }
  }

  /**
    * Get the list of input {@link Path}s for the map-reduce job.
    *
    * @param conf The configuration of the job
    * @return the list of input { @link Path}s for the map-reduce job.
    */
  def getInputPaths (conf: Configuration): Array[String] = {
    val dirs = conf.get ("mapreduce.input.fileinputformat.inputdir")
    if (dirs == null) {
      throw new IOException ("mapreduce.input.fileinputformat.inputdir is not defined.")
    }
    val list = StringUtils.split(dirs)
    val result = new Array[String](list.length)
    for (i <- 0 until list.size) {
      result(i) = StringUtils.unEscapeString(list(i))
    }
    result
  }
}
