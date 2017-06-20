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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.{DDLUtils, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.hive.execution._

private[hive] trait HiveStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SparkPlanner =>

  val sparkSession: SparkSession

  object Scripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(input, script, output, child, ioschema) =>
        val hiveIoSchema = HiveScriptIOSchema(ioschema)
        ScriptTransformation(input, script, output, planLater(child), hiveIoSchema) :: Nil
      case _ => Nil
    }
  }

  object DataSinks extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.InsertIntoTable(
          table: MetastoreRelation, partition, child, overwrite, ifNotExists) =>
        InsertIntoHiveTable(
          table, partition, planLater(child), overwrite.enabled, ifNotExists) :: Nil

      case CreateTable(tableDesc, mode, Some(query)) if tableDesc.provider.get == "hive" =>
        val newTableDesc = if (tableDesc.storage.serde.isEmpty) {
          // add default serde
          tableDesc.withNewStorage(
            serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
        } else {
          tableDesc
        }

        // Currently we will never hit this branch, as SQL string API can only use `Ignore` or
        // `ErrorIfExists` mode, and `DataFrameWriter.saveAsTable` doesn't support hive serde
        // tables yet.
        if (mode == SaveMode.Append || mode == SaveMode.Overwrite) {
          throw new AnalysisException(
            "CTAS for hive serde tables does not support append or overwrite semantics.")
        }

        val dbName = tableDesc.identifier.database.getOrElse(sparkSession.catalog.currentDatabase)
        val cmd = CreateHiveTableAsSelectCommand(
          newTableDesc.copy(identifier = tableDesc.identifier.copy(database = Some(dbName))),
          query,
          mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil

      case _ => Nil
    }
  }

  /**
   * Retrieves data using a HiveTableScan.  Partition pruning predicates are also detected and
   * applied.
   */
  object HiveTableScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        // Filter out all predicates that only deal with partition keys, these are given to the
        // hive table scan operator to be used for partition pruning.
        val partitionKeyIds = AttributeSet(relation.partitionKeys)
        val (pruningPredicates, otherPredicates) = predicates.partition { predicate =>
          !predicate.references.isEmpty &&
          predicate.references.subsetOf(partitionKeyIds)
        }

        pruneFilterProject(
          projectList,
          otherPredicates,
          identity[Seq[Expression]],
          HiveTableScanExec(_, relation, pruningPredicates)(sparkSession)) :: Nil
      case _ =>
        Nil
    }
  }

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
}
