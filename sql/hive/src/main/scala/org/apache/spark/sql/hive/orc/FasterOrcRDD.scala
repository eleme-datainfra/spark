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
import java.util.concurrent.{ExecutorCompletionService, Callable, Executors}

import com.facebook.presto.hive.HiveColumnHandle
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference
import com.facebook.presto.spi.`type`.Type
import org.apache.hadoop.conf.{Configuration, Configurable}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileSplit}
import org.apache.hadoop.mapreduce._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.DataReadMethod
import OrcUtil.StripeSplit
import org.apache.spark.sql.types.DataType
import org.apache.spark.{TaskKilledException, Partition, TaskContext, Logging}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.{ShutdownHookManager, SerializableConfiguration}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[hive] case class SerializableColumnInfo(
    @transient var output: Array[(Int, DataType, Type)],
    @transient var partitions: Map[Int, (DataType, String)],
    @transient var columnReferences: java.util.List[ColumnReference[HiveColumnHandle]])
  extends Serializable

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

  /**
    * Implemented by subclasses to return the set of partitions in this RDD. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    */
  override protected def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    if (sqlContext.conf.useStripeBasedSplitStrategy) {
      val inputPaths = OrcUtil.getInputPaths(jobConf)
      var result: Array[Partition] = null
      var splits: Seq[StripeSplit] = null
      val wrappedConf = new SerializableConfiguration(conf)

      if (inputPaths.size <= 10) {
        val exec = Executors.newFixedThreadPool(inputPaths.size)
        val completionService = new ExecutorCompletionService[Array[StripeSplit]](exec)
        inputPaths.foreach { path =>
          completionService.submit(new Callable[Array[StripeSplit]] {
            override def call(): Array[StripeSplit] = {
              OrcUtil.getSplit(wrappedConf, path)
            }
          })
        }

        splits = new ArrayBuffer[StripeSplit]()
        for (i <- 0 until inputPaths.size) {
          splits ++= completionService.take().get()
        }

        result = new Array[Partition](splits.size)
        exec.shutdown()
      } else {
        val splits = sqlContext.sparkContext.parallelize(inputPaths, inputPaths.size).map { path =>
          OrcUtil.getSplit(wrappedConf, path)
        }.collect().flatten
        result = new Array[Partition](splits.size)
      }

      for (i <- 0 until splits.size) {
        result(i) = new NewHadoopPartition(id, i,
          new FileSplit(new Path(splits(i).path),
            splits(i).offset,
            splits(i).length,
            splits(i).hosts))
      }
      result

    } else {
      val inputFormat = inputFormatClass.newInstance
      inputFormat match {
        case configurable: Configurable =>
          configurable.setConf(jobConf)
        case _ =>
      }

      val jobContext = newJobContext(jobConf, jobId)
      val rawSplits = inputFormat.getSplits(jobContext).toArray
      val result = new Array[Partition](rawSplits.size)

      for (i <- 0 until rawSplits.size) {
        result(i) =
          new NewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
      }
      result
    }
  }

  private val shouldCloneJobConf = sparkContext.conf.getBoolean("spark.hadoop.cloneConf", false)

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    val conf = broadcastedConf.value.value
    if (shouldCloneJobConf) {
      // Hadoop Configuration objects are not thread-safe, which may lead to various problems if
      // one job modifies a configuration while another reads it (SPARK-2546).  This problem occurs
      // somewhat rarely because most jobs treat the configuration as though it's immutable.  One
      // solution, implemented here, is to clone the Configuration object.  Unfortunately, this
      // clone can be very expensive.  To avoid unexpected performance regressions for workloads and
      // Hadoop versions that do not suffer from these thread-safety issues, this cloning is
      // disabled by default.
      HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
        logDebug("Cloning Hadoop Configuration")
        val newJobConf = new JobConf(conf)
        if (!conf.isInstanceOf[JobConf]) {
          initLocalJobConfFuncOpt.map(f => f(newJobConf))
        }
        newJobConf
      }
    } else {
      if (conf.isInstanceOf[JobConf]) {
        logDebug("Re-using user-broadcasted JobConf")
        conf.asInstanceOf[JobConf]
      } else if (HadoopRDD.containsCachedMetadata(jobConfCacheKey)) {
        logDebug("Re-using cached JobConf")
        HadoopRDD.getCachedMetadata(jobConfCacheKey).asInstanceOf[JobConf]
      } else {
        // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in the
        // local process. The local cache is accessed through HadoopRDD.putCachedMetadata().
        // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary objects.
        // Synchronize to prevent ConcurrentModificationException (SPARK-1097, HADOOP-10456).
        HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
          logDebug("Creating new JobConf and caching it for later re-use")
          val newJobConf = new JobConf(conf)
          initLocalJobConfFuncOpt.map(f => f(newJobConf))
          HadoopRDD.putCachedMetadata(jobConfCacheKey, newJobConf)
          newJobConf
        }
      }
    }
  }

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to compute a given partition.
    */
  override def compute(split: Partition, context: TaskContext): Iterator[V] = {
    val iter = new Iterator[V] {
      val inputSplit = split.asInstanceOf[NewHadoopPartition]
      val conf = getJobConf()
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
      if (sqlContext.conf.useFasterOrcReader) {
        val orcReader = new FasterOrcRecordReader(columnInfo.output, columnInfo.partitions,
          columnInfo.columnReferences)
        if (!orcReader.tryInitialize(inputSplit.serializableHadoopSplit.value,
          hadoopAttemptContext)) {
          orcReader.close()
        } else {
          reader = orcReader.asInstanceOf[RecordReader[NullWritable, V]]
        }
      }

      if (reader == null) {
        reader = format.createRecordReader(
          inputSplit.serializableHadoopSplit.value, hadoopAttemptContext)
        reader.initialize(inputSplit.serializableHadoopSplit.value, hadoopAttemptContext)
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
