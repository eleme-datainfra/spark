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

package org.apache.spark.util.collection

import java.io.File

import org.apache.spark.SparkEnv
import org.geirove.exmeso.{ChunkSizeIterator, ExternalMergeSort}
import org.geirove.exmeso.kryo.KryoSerializer

import scala.collection.JavaConverters._

import com.google.common.collect.{Ordering => GuavaOrdering}

import scala.util.Random

/**
 * Utility functions for collections.
 */
private[spark] object Utils {

  /**
   * Returns the first K elements from the input as defined by the specified implicit Ordering[T]
   * and maintains the ordering.
   */
  def takeOrdered[T](input: Iterator[T], num: Int)(implicit ord: Ordering[T]): Iterator[T] = {
    val ordering = new GuavaOrdering[T] {
      override def compare(l: T, r: T): Int = ord.compare(l, r)
    }
    val conf = SparkEnv.get.conf
    val chunkSize = conf.getInt("spark.takeOrdered.chunkSize", 50000)
    val csi = new ChunkSizeIterator(input.asJava, chunkSize)
    if (csi.isMultipleChunks) {
      val serializer = new KryoSerializer[T]()
      val tempDirs = org.apache.spark.util.Utils.getOrCreateLocalRootDirs(conf)
      val tempDir = new File(tempDirs(new Random().nextInt(tempDirs.size)))
      val sort = ExternalMergeSort.newSorter(serializer, ordering)
        .withChunkSize(chunkSize)
        .withTempDirectory(tempDir)
        .build()
      sort.mergeSort(csi).asScala.take(num)
    } else {
      ordering.leastOf(csi, num).iterator().asScala
    }
  }
}
