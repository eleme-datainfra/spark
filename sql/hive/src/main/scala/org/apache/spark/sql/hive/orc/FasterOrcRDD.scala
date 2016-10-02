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

import java.text.SimpleDateFormat
import java.util.Date

import com.facebook.presto.`type`.TypeRegistry
import com.facebook.presto.hive.{HiveType, HiveColumnHandle}
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference
import com.facebook.presto.spi.`type`.Type
import org.apache.hadoop.conf.{Configuration, Configurable}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileSplit}
import org.apache.hadoop.mapreduce._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.sql.hive.HiveMetastoreTypes
import org.apache.spark.sql.types.DataType
import org.apache.spark.{TaskKilledException, Partition, TaskContext, Logging}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.{ShutdownHookManager, SerializableConfiguration}

import scala.collection.mutable
import scala.reflect.ClassTag

case class SerializableColumnInfo(outputCols: Seq[OutputColumn], partitionCols: Seq[PartitionColumn])
  extends Serializable
case class OutputColumn(name: String, dataTypeJson: String, index: Int) extends Serializable
case class PartitionColumn(index: Int, dataTypeJson: String, value: String) extends Serializable

case class ColumnInfo(output: Array[(Int, DataType, Type)],
    partitions: Map[Int, (DataType, String)],
    columnReferences: java .util.List[ColumnReference[HiveColumnHandle]])

private[hive] class FasterOrcRDD[V: ClassTag](
    sqlContext: SQLContext,
    broadcastedConf: Broadcast[SerializableConfiguration],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    columnInfo: SerializableColumnInfo,
    inputFormatClass: Class[_ <: InputFormat[NullWritable, V]],
    valueClass: Class[V])
  extends RDD[V](sqlContext.sparkContext, Nil)
    with SparkHadoopMapReduceUtil
    with Logging {

  if (initLocalJobConfFuncOpt.isDefined) {
    sparkContext.clean(initLocalJobConfFuncOpt.get)
  }

  def this(
      sqlContext: SQLContext,
      jobConf: Configuration,
      columnInfo: SerializableColumnInfo,
      inputFormatClass: Class[_ <: InputFormat[NullWritable, V]],
      valueClass: Class[V]) {

    this(sqlContext,
      sqlContext.sparkContext.broadcast(new SerializableConfiguration(jobConf)),
      initLocalJobConfFuncOpt = None,
      columnInfo,
      inputFormatClass,
      valueClass
    )
  }

  protected val jobConfCacheKey = "rdd_%d_job_conf".format(id)

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)

  val useFasterOrcReader = sqlContext.conf.useFasterOrcReader

  /**
    * Implemented by subclasses to return the set of partitions in this RDD. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    */
  override protected def getPartitions: Array[Partition] = {
    val jobConf = getConf()
    val jobContext = newJobContext(jobConf, jobId)
    val inputFormat = inputFormatClass.newInstance
    inputFormat match {
      case configurable: Configurable =>
        configurable.setConf(jobConf)
      case _ =>
    }

    val rawSplits = inputFormat.getSplits(jobContext).toArray
    val result = new Array[Partition](rawSplits.size)

    for (i <- 0 until rawSplits.size) {
      result(i) =
        new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }

  protected def getJob(): Job = {
    val conf: Configuration = broadcastedConf.value.value
    // "new Job" will make a copy of the conf. Then, it is
    // safe to mutate conf properties with initLocalJobFuncOpt
    val newJob = new Job(conf)
    initLocalJobConfFuncOpt.map(f => f(newJob))
    newJob
  }

  def getConf(): Configuration = {
    val job = getJob()
    initLocalJobConfFuncOpt.map(f => f(job))
    SparkHadoopUtil.get.getConfigurationFromJobContext(job)
  }

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to compute a given partition.
    */
  override def compute(split: Partition, context: TaskContext): Iterator[V] = {
    val iter = new Iterator[V] {
      val inputSplit = split.asInstanceOf[NewHadoopPartition]
      val conf = getConf()
      val inputMetrics = context.taskMetrics
        .getInputMetricsForReadMethod(DataReadMethod.Hadoop)

      // Sets the thread local variable for the file's name
      inputSplit.serializableHadoopSplit.value match {
        case fs: FileSplit => SqlNewHadoopRDDState.setInputFileName(fs.getPath.toString)
        case _ => SqlNewHadoopRDDState.unsetInputFileName()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = inputMetrics.bytesReadCallback.orElse {
        inputSplit.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
          case _ => None
        }
      }
      inputMetrics.setBytesReadCallback(bytesReadCallback)

      val format = inputFormatClass.newInstance
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }
      val attemptId = newTaskAttemptID(jobTrackerId, id, isMap = true, split.index, 0)
      val hadoopAttemptContext = newTaskAttemptContext(conf, attemptId)
      private[this] var reader: RecordReader[NullWritable, V] = null

      /**
        * If the format is OrcInputFormat, try to create the optimized RecordReader. If this
        * fails (for example, unsupported schema), try with the normal reader.
        * TODO: plumb this through a different way?
        */
      if (useFasterOrcReader) {
        val columns = deserializeColumnInfo(columnInfo)
        val orcReader = new FasterOrcRecordReader(columns.output, columns.partitions,
          columns.columnReferences)
        if (!orcReader.tryInitialize(inputSplit.serializableHadoopSplit.value,
          hadoopAttemptContext)) {
          orcReader.close()
        } else {
          reader = orcReader.asInstanceOf[RecordReader[NullWritable, V]]
        }
      }

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(context => close())
      private[this] var havePair = false
      private[this] var finished = false

      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        }
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          if (finished) {
            // Close and release the reader here; close() will also be called when the task
            // completes, but for tasks that read from many files, it helps to release the
            // resources early.
            close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): V = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        reader.getCurrentValue
      }

      private def close() {
        if (reader != null) {
          // Close the reader and release it. Note: it's very important that we don't close the
          // reader more than once, since that exposes us to MAPREDUCE-5918 when running against
          // Hadoop 1.x and older Hadoop 2.x releases. That bug can lead to non-deterministic
          // corruption issues when reading compressed input.
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (bytesReadCallback.isDefined) {
            inputMetrics.updateBytesRead()
          } else if (inputSplit.serializableHadoopSplit.value.isInstanceOf[FileSplit] ||
            inputSplit.serializableHadoopSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(inputSplit.serializableHadoopSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    iter
  }

  def deserializeColumnInfo(serializableColumnInfo: SerializableColumnInfo): ColumnInfo = {
    val outputCols = serializableColumnInfo.outputCols
    val partitionCols = serializableColumnInfo.partitionCols
    val typeManager = new TypeRegistry()
    val columnReferences = new java.util.ArrayList[ColumnReference[HiveColumnHandle]]
    var nonPartitionOutputAttrs = new mutable.ArrayBuffer[(Int, DataType, Type)]

    outputCols.foreach { case c: OutputColumn =>
      val dt = DataType.fromJson(c.dataTypeJson)
      val mType = HiveMetastoreTypes.toMetastoreType(dt)
      val hiveType = HiveType.valueOf(mType)
      val pType = typeManager.getType(hiveType.getTypeSignature)
      columnReferences.add(new ColumnReference(
        new HiveColumnHandle("", c.name, hiveType, hiveType.getTypeSignature, c.index, false),
        c.index,
        pType))
      nonPartitionOutputAttrs += ((c.index, dt, pType))
    }

    var partitionOutputAttrs = new mutable.HashMap[Int, (DataType, String)]
    partitionCols.foreach { case p: PartitionColumn =>
      val dt = DataType.fromJson(p.dataTypeJson)
      partitionOutputAttrs += p.index -> (dt, p.value)
    }

    ColumnInfo(nonPartitionOutputAttrs.toArray,
      partitionOutputAttrs.toMap, columnReferences)
  }

  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val split = hsplit.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value
    val locs = HadoopRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val infos = c.newGetLocationInfo.invoke(split).asInstanceOf[Array[AnyRef]]
          Some(HadoopRDD.convertSplitLocationInfo(infos))
        } catch {
          case e : Exception =>
            logDebug("Failed to use InputSplit#getLocationInfo.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(split.getLocations.filter(_ != "localhost"))
  }
}
