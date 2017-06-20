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

package org.apache.spark.sql.hive

import java.io.IOException

import scala.util.control.Breaks.{break, breakable}

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils

case class DeterminePartitionedTableStats(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case filter@Filter(condition, relation: MetastoreRelation)
      if DDLUtils.isHiveTable(relation.catalogTable) &&
        !relation.catalogTable.partitionColumnNames.isEmpty &&
        sparkSession.sessionState.conf.fallBackToHdfsForStatsEnabled &&
        sparkSession.sessionState.conf.metastorePartitionPruning =>
      val predicates = splitConjunctivePredicates(condition)
      val partitionSet = AttributeSet(relation.partitionKeys)
      val pruningPredicates = predicates.filter { predicate =>
        !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionSet)
      }
      if (pruningPredicates.nonEmpty) {
        val threshold = sparkSession.sessionState.conf.autoBroadcastJoinThreshold
        val prunedPartitions = sparkSession.sharedState.externalCatalog.listPartitionsByFilter(
          relation.catalogTable.database,
          relation.catalogTable.identifier.table,
          pruningPredicates)
        var sizeInBytes = 0L
        var hasError = false
        val partitions = prunedPartitions.filter(p => p.storage.locationUri.isDefined)
          .map(p => new Path(p.storage.locationUri.get))
        val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
        breakable {
          partitions.foreach { partition =>
            try {
              val fs = partition.getFileSystem(hadoopConf)
              sizeInBytes += fs.getContentSummary(partition).getLength
              if (sizeInBytes > threshold) {
                break()
              }
            } catch {
              case e: IOException =>
                logWarning("Failed to get table size from hdfs.", e)
                hasError = true
            }
          }
        }

        if (hasError && sizeInBytes == 0) {
          sizeInBytes = sparkSession.sessionState.conf.defaultSizeInBytes
        }
        relation.catalogTable.stats = Some(Statistics(sizeInBytes = BigInt(sizeInBytes)))
        Filter(condition, relation)
      } else {
        filter
      }
  }
}
