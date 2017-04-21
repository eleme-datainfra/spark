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

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import kafka.common.TopicAndPartition
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

/**
 * Utilities for converting Kafka related objects to and from json.
 */
object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def toJson(partitionToOffsets: Map[TopicAndPartition, LeaderOffset]): String = {
      val result = new HashMap[String, HashMap[Int, String]]()
      implicit val ordering = new Ordering[TopicAndPartition] {
        override def compare(x: TopicAndPartition, y: TopicAndPartition): Int = {
          Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
        }
      }
      val partitions = partitionToOffsets.keySet.toSeq.sorted  // sort for more determinism
      partitions.foreach { tp =>
        val lo = partitionToOffsets(tp)
        val parts = result.getOrElse(tp.topic, new HashMap[Int, String])
        parts += tp.partition -> s"${lo.host}|${lo.port}|${lo.offset}"
        result += tp.topic -> parts
      }
      Serialization.write(result)
  }

  def fromJson(json: String): Map[TopicAndPartition, LeaderOffset] = {
    try {
      Serialization.read[Map[String, Map[Int, String]]](json).flatMap { case (topic, partOffsets) =>
        partOffsets.map { case (part, leaderOffset) =>
          val strs = leaderOffset.split("|")
          val host = strs(0)
          val port = strs(1).toInt
          val offset = strs(2).toLong
          new TopicAndPartition(topic, part) -> LeaderOffset(host, port, offset)
        }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $json""")
    }
  }
}
