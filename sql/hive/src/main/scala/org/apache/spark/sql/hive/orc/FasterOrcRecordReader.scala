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

import com.facebook.presto.hive.HiveColumnHandle
import com.facebook.presto.hive.orc.HdfsOrcDataSource
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference
import com.facebook.presto.orc._
import com.facebook.presto.orc.memory.AggregatedMemoryContext
import com.facebook.presto.orc.metadata.{OrcMetadataReader, MetadataReader}
import com.facebook.presto.spi.`type`.Type
import com.facebook.presto.spi.block.Block
import com.facebook.presto.spi.predicate.TupleDomain
import io.airlift.units.DataSize
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions
import org.apache.hadoop.hive.ql.io.orc.ReaderImpl
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.joda.time.DateTimeZone
import java.io.FileNotFoundException
import java.io.IOException
import java.util.TimeZone
import com.google.common.base.Strings.nullToEmpty
import scala.collection.JavaConverters._


class FasterOrcRecordReader(
    output: Array[(Int, DataType, Type)],
    // includedColumns: java.util.Map[Integer, Type],
    columnReferences: java .util.List[ColumnReference[HiveColumnHandle]])
  extends RecordReader[NullWritable, UnsafeRow] {

  /**
    * Batch of unsafe rows that we assemble and the current index we've returned. Everytime this
    * batch is used up (batchIdx == numBatched), we populated the batch.
    */
  private var rows: Array[UnsafeRow] = new Array[UnsafeRow](1024)
  private var batchIdx: Int = 0
  private var numBatched: Int = 0

  /**
    * Used to write variable length columns. Same length as `rows`.
    */
  private var rowWriters: Array[UnsafeRowWriter] = null

  /**
    * The number of bytes in the fixed length portion of the row.
    */
  private var fixedSizeBytes: Int = 0

  /**
    * True if the row contains variable length fields.
    */
  private var containsVarLenFields = false

  /**
    * The number of rows that have been returned.
    */
  private var rowsReturned: Long = 0L

  /**
    * The total number of rows this RecordReader will eventually read. The sum of the
    * rows of all the row groups.
    */
  protected var totalRowCount: Long = 0L

  private var recordReader: OrcRecordReader = _

  /**
    * The default size for varlen columns. The row grows as necessary to accommodate the
    * largest column.
    */
  private val DEFAULT_VAR_LEN_SIZE: Int = 32

  /**
    * Tries to initialize the reader for this split. Returns true if this reader supports reading
    * this split and false otherwise.
    */
  def tryInitialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Boolean = {
    try {
      initialize(inputSplit, taskAttemptContext)
      return true
    }
    catch {
      case e: Exception => {
        return false
      }
    }
  }

  /**
    * Implementation of RecordReader API.
    */
  def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {
    val fileSplit: FileSplit = inputSplit.asInstanceOf[FileSplit]
    val path: Path = fileSplit.getPath
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(taskAttemptContext)
    initialize(path, conf)
  }

  def initialize(path: Path, conf: Configuration): Unit = {

    var orcDataSource: OrcDataSource = null
    val metadataReader: MetadataReader = new OrcMetadataReader
    val maxMergeDistance: DataSize = new DataSize(1, DataSize.Unit.MEGABYTE)
    val maxBufferSize: DataSize = new DataSize(8, DataSize.Unit.MEGABYTE)
    val streamBufferSize: DataSize = new DataSize(8, DataSize.Unit.MEGABYTE)
    val hiveStorageTimeZone: DateTimeZone = DateTimeZone.forTimeZone(
      TimeZone.getTimeZone(TimeZone.getDefault.getID))

    try {
      val fileSystem = path.getFileSystem(conf)
      val size = fileSystem.getFileStatus(path).getLen
      var inputStream: FSDataInputStream = null
      if (fileSystem.isDirectory(path)) {
        val childPaths = fileSystem.listStatus(path)
        val childPath = childPaths(1).getPath
        inputStream = fileSystem.open(childPath)
        orcDataSource = new HdfsOrcDataSource(childPath.toString, childPaths(1).getLen,
          maxMergeDistance, maxBufferSize, streamBufferSize, inputStream)
      } else {
        inputStream = fileSystem.open(path)
        orcDataSource = new HdfsOrcDataSource(path.toString, size, maxMergeDistance,
          maxBufferSize, streamBufferSize, inputStream)
      }
    } catch {
      case e: Exception => {
        if ((nullToEmpty(e.getMessage).trim == "Filesystem closed")
          || e.isInstanceOf[FileNotFoundException]) {
          throw new IOException("Error open split " + path.toString, e.getCause)
        }
        throw new IOException(s"Error opening Hive split $path ")
      }
    }

    val systemMemoryUsage: AggregatedMemoryContext = new AggregatedMemoryContext

    val reader: OrcReader = new OrcReader(orcDataSource, metadataReader,
      maxMergeDistance, maxBufferSize)

    val effectivePredicate: TupleDomain[HiveColumnHandle] = TupleDomain.all()
    val predicate = new TupleDomainOrcPredicate[HiveColumnHandle](effectivePredicate,
      columnReferences)
    val columns = output.map(x => (x._1: Integer, x._3)).toMap.asJava

    recordReader = reader.createRecordReader(columns, predicate,
      hiveStorageTimeZone, systemMemoryUsage)
    totalRowCount = recordReader.getReaderRowCount

    /**
      * Initialize rows and rowWriters. These objects are reused across all rows in the relation.
      */
    var rowByteSize: Int = UnsafeRow.calculateBitSetWidthInBytes(output.size)
    rowByteSize += 8 * output.size
    fixedSizeBytes = rowByteSize
    var numVarLenFields = 0
    output.foreach { x =>
      if (!x._2.isInstanceOf[NumericType] && !x._2.isInstanceOf[BooleanType]) {
        numVarLenFields += 1
      }
    }

    rowByteSize += numVarLenFields * DEFAULT_VAR_LEN_SIZE
    containsVarLenFields = numVarLenFields > 0

    rowWriters = new Array[UnsafeRowWriter](rows.length)
    for (i <- 0 until rows.length) {
      rows(i) = new UnsafeRow
      rowWriters(i) = new UnsafeRowWriter
      val holder: BufferHolder = new BufferHolder(rowByteSize)
      rowWriters(i).initialize(rows(i), holder, output.size)
      rows(i).pointTo(holder.buffer, Platform.BYTE_ARRAY_OFFSET, output.size, holder.buffer.length)
    }
  }


  def nextKeyValue: Boolean = {
    if (batchIdx >= numBatched) {
      if (!loadBatch) return false
    }
    batchIdx += 1
    return true
  }

  def getCurrentValue: UnsafeRow = {
    return rows(batchIdx - 1)
  }

  def getProgress: Float = {
    return rowsReturned.toFloat / totalRowCount
  }

  def getCurrentKey: NullWritable = {
    return NullWritable.get
  }

  def close: Unit = {
    recordReader.close()
  }

  /**
    * Decodes a batch of values into `rows`. This function is the hot path.
    */
  private def loadBatch: Boolean = {
    if (rowsReturned >= totalRowCount) {
      return false
    }

    try {
      batchIdx = 0
      numBatched = recordReader.nextBatch
      if (numBatched <= 0) {
        close()
        return false
      }

      rowsReturned += numBatched
      if (containsVarLenFields) {
        for (i <- 0 until rowWriters.size) {
          rowWriters(i).holder.resetTo(fixedSizeBytes)
        }
      }

      var block: Block = null
      var length: Int = 0
      for(col <- 0 until output.size) {
        block = recordReader.readBlock(output(col)._3, output(col)._1)

        for (row <- 0 until numBatched) {
          if (block.isNull(row)) {
            rows(row).setNullAt(col)
          } else {
            output(col)._2 match {
              case BinaryType =>
                length = block.getLength(row)
                rowWriters(row).write(col, block.getSlice(row, 0, length).getBytes)
                rows(row).setNotNullAt(col)

              case StringType =>
                length = block.getLength(row)
                rowWriters(row).write(col,
                  UTF8String.fromBytes(block.getSlice(row, 0, length).getBytes, 0, length))
                rows(row).setNotNullAt(col)

              case BooleanType =>
                rows(row).setBoolean(col, block.getByte(row, 0) != 0)

              case ByteType =>
                rows(row).setByte(col, block.getByte(row, 0))

              case DateType =>
                rows(row).setInt(col, block.getLong(row, 0).toInt)

              case dt: DecimalType =>
                rows(row).setDecimal(
                  col,
                  Decimal.apply(block.getLong(row, 0), dt.precision, dt.scale),
                  dt.precision)

              case DoubleType =>
                rows(row).setDouble(col, block.getDouble(row, 0))

              case FloatType =>
                rows(row).setFloat(col, block.getDouble(row, 0).toFloat)

              case IntegerType =>
                rows(row).setInt(col, block.getInt(row, 0))

              case TimestampType =>
              case LongType =>
                rows(row).setLong(col, block.getLong(row, 0))

              case ShortType =>
                rows(row).setShort(col, block.getShort(row, 0))

              case dt: MapType =>
              case dt: ArrayType =>
              case dt: StructType =>
              case _ =>
                throw new SparkException("Unsupported Type " + output(col)._2)
            }
          }
        }
      }

      // Update the total row lengths if the schema contained variable length. We did not maintain
      // this as we populated the columns.
      if (containsVarLenFields) {
        for (i <- 0 until rowWriters.length) {
          rows(i).setTotalSize(rowWriters(i).holder.totalSize)
        }
      }

    } catch {
      case e: Exception =>
        throw e
    }
    return true
  }

}
