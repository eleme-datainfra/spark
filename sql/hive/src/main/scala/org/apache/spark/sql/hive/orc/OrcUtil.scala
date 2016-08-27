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
import org.apache.hadoop.fs.{PathFilter, BlockLocation, FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.OrcFile
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.util.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object OrcUtil {

  case class StripeSplit(path: String, offset: Long, length: Long, hosts: Array[String])

  def getSplit(broadcastedConf: SerializableConfiguration, path: String): Array[StripeSplit] = {
    val conf = broadcastedConf.value
    val fs = FileSystem.get(conf)
    val inputPath = new Path(path)
    var currentOffset = -1L
    var currentLength = 0L
    val minSize = conf.getLong("mapred.min.split.size", 16 * 1024 * 1024)
    val maxSize = conf.getLong("mapred.max.split.size", 256 * 1024 * 1024)
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
      val locations = ShimLoader.getHadoopShims.getLocations(fs, fs.getFileStatus(file))

      orcReader.getStripes.asScala.foreach { stripe =>
        // if we are working on a stripe, over the min stripe size, and
        // crossed a block boundary, cut the input split here.
        if (currentOffset != -1 && currentLength > minSize &&
          (currentOffset / blockSize != stripe.getOffset() / blockSize)) {
          splits += createSplit(path, currentOffset, currentLength, blockSize, locations)
          currentOffset = -1
        }

        // if we aren't building a split, start a new one.
        if (currentOffset == -1) {
          currentOffset = stripe.getOffset()
          currentLength = stripe.getLength()
        } else {
          currentLength += stripe.getLength()
        }

        if (currentLength >= maxSize) {
          splits += createSplit(path, currentOffset, currentLength, blockSize, locations)
          currentOffset = -1
        }

        if (currentOffset != -1) {
          splits += createSplit(path, currentOffset, currentLength, blockSize, locations)
        }
      }
    }


    splits.toArray
  }

  def createSplit(path: String,
                  offset: Long,
                  length: Long,
                  blockSize: Long,
                  locations: Array[BlockLocation]): StripeSplit = {
    var hosts: Array[String] = null
    if ((offset % blockSize) + length <= blockSize) {
      hosts = locations((offset / blockSize).toInt).getHosts()
    } else {
      // Calculate the number of bytes in the split that are local to each host
      val sizes = new util.HashMap[String, LongWritable]()
      var maxSize = 0L
      locations.foreach { block =>
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
        }
      }

      // filter the list of locations to those that have at least 70% of the max
      val threshold = (maxSize * 0.7).toLong
      val hostList = new ArrayBuffer[String]()
      locations.foreach { block =>
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
