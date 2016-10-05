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

package org.apache.spark.rdd

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{InputSplit, JobConf, InputFormat}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{SerializableWritable, Partition, SparkContext}

import scala.reflect.ClassTag


case class PartitionInfo(path: String, ifc: Class[InputFormat[Writable, Writable]])

private[spark] class ParallelUnionRDD[T: ClassTag](
    sc: SparkContext,
    rdds: Seq[RDD[T]],
    broadcastedConf: Broadcast[SerializableConfiguration],
    initLocalJobConfFuncOpt: Option[(String, JobConf) => Unit],
    partitions: Seq[PartitionInfo]) extends UnionRDD[T](sc, rdds) {

  val threshold = sc.conf.getInt("spark.rdd.parallelPartitionsThreshold", 16)

  override def getPartitions: Array[Partition] = {
    if (partitions.size > threshold) {
      val rddIdMap = rdds.zipWithIndex.map(x => x._2 -> x._1.firstParent.id).toMap

      val rddIndexWithPartitions =
        sc.parallelize(partitions.zipWithIndex, partitions.size).map { case (part, index) =>
          val jobConfCacheKey = "rdd_%d_job_conf".format(rddIdMap(index))
          val conf = broadcastedConf.value.value
          val jobConf = new JobConf(conf)
          initLocalJobConfFuncOpt.map(f => f(part.path, jobConf))
          HadoopRDD.putCachedMetadata(jobConfCacheKey, jobConf)
          SparkHadoopUtil.get.addCredentials(jobConf)

          val inputFormat =
            ReflectionUtils.newInstance(part.ifc.asInstanceOf[Class[_]], jobConf)
              .asInstanceOf[InputFormat[Writable, Writable]]
          inputFormat match {
            case c: Configurable => c.setConf(jobConf)
            case _ =>
          }
          val inputSplits = inputFormat.getSplits(jobConf, 1)
          val array = new Array[Partition](inputSplits.size)
          for (i <- 0 until inputSplits.size) {
            array(i) = new HadoopPartition(rddIdMap(index), i,
              new SerializableWritable[InputSplit](inputSplits(i)))
          }
          (index, array)
        }.collect()

      val array = new Array[Partition](rddIndexWithPartitions.map(_._2.size).sum)
      var pos = 0

      rddIndexWithPartitions.foreach { case (rddIndex, parts) =>
        val rdd = rdds(rddIndex)
        rdds(rddIndex).setPartitions(parts)
        parts.foreach { part =>
          array(pos) = new UnionPartition(pos, rdd, rddIndex, part.index)
          pos += 1
        }
      }
      array
    } else {
      super.getPartitions
    }
  }

}

