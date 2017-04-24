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

package org.apache.spark.sql.kafka08

import kafka.common.TopicAndPartition
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

/**
 * An [[Offset]] for the [[KafkaSource]]. This one tracks all partitions of subscribed topics and
 * their offsets.
 */
case class KafkaSourceOffset(partitionToOffsets: Map[TopicAndPartition, LeaderOffset])
  extends Offset {
  override def toString(): String = {
    partitionToOffsets.toSeq.sortBy(_._1.toString).mkString("[", ", ", "]")
  }

  override val json = KafkaSourceOffset.toJson(partitionToOffsets)
}

/** Companion object of the [[KafkaSourceOffset]] */
object KafkaSourceOffset {

  private implicit val formats = Serialization.formats(NoTypeHints)

  def toJson(partitionToOffsets: Map[TopicAndPartition, LeaderOffset]): String = {
    Serialization.write(partitionToOffsets)
  }

  def getPartitionOffsets(offset: Offset): Map[TopicAndPartition, LeaderOffset] = {
    offset match {
      case o: KafkaSourceOffset => o.partitionToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to KafkaSourceOffset")
    }
  }
}

