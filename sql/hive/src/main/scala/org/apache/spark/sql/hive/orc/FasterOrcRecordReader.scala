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

import java.util

import com.facebook.presto.hive.HiveColumnHandle
import com.facebook.presto.hive.orc.HdfsOrcDataSource
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference
import com.facebook.presto.orc._
import com.facebook.presto.orc.memory.AggregatedMemoryContext
import com.facebook.presto.orc.metadata.{OrcMetadataReader, MetadataReader}
import com.facebook.presto.spi.`type`.Type
import com.facebook.presto.spi.block.{ArrayBlock, BlockEncoding, BlockBuilder, Block}
import com.facebook.presto.spi.predicate.TupleDomain
import io.airlift.slice.Slice
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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{MutableRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.joda.time.DateTimeZone
import java.io.FileNotFoundException
import java.io.IOException
import java.util.TimeZone
import com.google.common.base.Strings.nullToEmpty
import scala.collection.JavaConverters._


class FasterOrcRecordReader(
    output: Array[(Int, DataType, Type)],
    columnReferences: java .util.List[ColumnReference[HiveColumnHandle]])
  extends RecordReader[NullWritable, InternalRow] {

  private var batchIdx: Int = 0
  private var numBatched: Int = 0
  private val columns = new Array[Block](output.size)

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

  }


  def nextKeyValue: Boolean = {
    if (batchIdx >= numBatched) {
      if (!loadBatch) return false
    }
    batchIdx += 1
    return true
  }

  def getCurrentValue: InternalRow = {
    BlockRow.getRow(batchIdx)
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

      for (col <- 0 until output.size) {
        columns(col) = recordReader.readBlock(output(col)._3, output(col)._1)
      }

      return true
    }
  }

  object BlockRow {
    var row: BlockRow = new BlockRow(0)
    def getRow(batchIdx: Int): BlockRow = {
      row.setBatchIndex(batchIdx)
      row
    }
  }

  class MutableBlock(block: Block) extends Block {

    override def getLength(i: Int): Int = {
      block.getLength(i)
    }

    override def getSlice(position: Int, offset: Int, length: Int): Slice = {
      block.getSlice(position, offset, length)
    }

    override def getPositionCount: Int = {
      block.getPositionCount
    }

    override def bytesEqual(position: Int, offset: Int, otherSlice: Slice,
                            otherOffset: Int, length: Int): Boolean = {
      block.bytesEqual(position, offset, otherSlice, otherOffset, length)
    }

    override def getDouble(position: Int, offset: Int): Double = {
      block.getDouble(position, offset)
    }

    override def copyPositions(list: util.List[Integer]): Block = {
      block.copyPositions(list)
    }

    override def assureLoaded(): Unit = {
      block.assureLoaded()
    }

    override def getSizeInBytes: Int = {
      block.getSizeInBytes
    }

    override def writePositionTo(position: Int, blockBuilder: BlockBuilder): Unit = {
      block.writePositionTo(position, blockBuilder)
    }

    override def hash(position: Int, offset: Int, length: Int): Long = {
      block.hash(position, offset, length)
    }

    override def copyRegion(position: Int, length: Int): Block = {
      block.copyRegion(position, length)
    }

    override def getRetainedSizeInBytes: Int = {
      block.getRetainedSizeInBytes
    }

    override def getFloat(position: Int, offset: Int): Float = {
      block.getFloat(position, offset)
    }

    override def getLong(position: Int, offset: Int): Long = {
      block.getLong(position, offset)
    }

    override def getEncoding: BlockEncoding = {
      block.getEncoding
    }

    override def getSingleValueBlock(position: Int): Block = {
      block.getSingleValueBlock(position)
    }

    override def compareTo(position: Int, offset: Int, length: Int, otherBlock: Block,
                           otherPosition: Int, otherOffset: Int, otherLength: Int): Int = {
      block.compareTo(position, offset, length, otherBlock, otherPosition, otherOffset, otherLength)
    }

    override def getByte(position: Int, offset: Int): Byte = {
      block.getByte(position, offset)
    }

    override def getShort(position: Int, offset: Int): Short = {
      block.getShort(position, offset)
    }

    override def bytesCompare(position: Int, offset: Int, length: Int,
                              otherSlice: Slice, otherOffset: Int, otherLength: Int): Int = {
      block.bytesCompare(position, offset, length, otherSlice, otherOffset, otherLength)
    }

    override def isNull(postion: Int): Boolean = {
      block.isNull(postion)
    }

    override def getRegion(position: Int, offset: Int): Block = {
      block.getRegion(position, offset)
    }

    override def writeBytesTo(position: Int, offset: Int, length: Int,
                              blockBuilder: BlockBuilder): Unit = {
      block.writeBytesTo(position, offset, length, blockBuilder)
    }

    override def getInt(position: Int, offset: Int): Int = {
      block.getInt(position, offset)
    }

    override def equals(position: Int, offset: Int, otherBlock: Block,
                        otherPosition: Int, otherOffset: Int, length: Int): Boolean = {
      block.equals(position, offset, otherBlock, otherPosition, otherOffset, length)
    }
  }

  class BlockRow(var batchIdx: Int) extends MutableRow {
    var length = 0

    def setBatchIndex(batchIdx: Int): Unit = {
      this.batchIdx = batchIdx
    }

    override def setNullAt(ordinal: Int): Unit = {
      throw new UnsupportedOperationException("Operation is not supported!")
    }

    override def update(ordinal: Int, value: Any): Unit = {
      throw new UnsupportedOperationException("Operation is not supported!")
    }

    /** Returns true if there are any NULL values in this row. */
    override def anyNull: Boolean = {
      !columns.filter(b => b.isNull(batchIdx - 1)).isEmpty
    }

    /**
      * Make a copy of the current [[InternalRow]] object.
      */
    override def copy(): InternalRow = {
      throw new UnsupportedOperationException("Operation is not supported!")
    }

    override def numFields: Int = output.size

    override def getUTF8String(ordinal: Int): UTF8String = {
      length = columns(ordinal).getLength(batchIdx - 1)
      UTF8String.fromBytes(columns(ordinal).getSlice(batchIdx - 1, 0, length).getBytes)
    }

    override def get(ordinal: Int, dataType: DataType): Object = {
      throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString)
    }

    override def getBinary(ordinal: Int): Array[Byte] = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).getBytes
    }

    override def getDouble(ordinal: Int): Double = {
      columns(ordinal).getDouble(batchIdx - 1, 0)
    }

    override def getArray(ordinal: Int): ArrayData = {
      null
    }

    override def getInterval(ordinal: Int): CalendarInterval = {
      null
    }

    override def getFloat(ordinal: Int): Float = {
      columns(ordinal).getDouble(batchIdx - 1, 0).toFloat
    }

    override def getLong(ordinal: Int): Long = {
      columns(ordinal).getLong(batchIdx - 1, 0)
    }

    override def getMap(ordinal: Int): MapData = {
      null
    }

    override def getByte(ordinal: Int): Byte = {
      columns(ordinal).getByte(batchIdx - 1, 0)
    }

    override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
      Decimal.apply(columns(ordinal).getLong(batchIdx - 1, 0), precision, scale)
    }

    override def getBoolean(ordinal: Int): Boolean = {
      columns(ordinal).getByte(batchIdx - 1, 0) != 0
    }

    override def getShort(ordinal: Int): Short = {
      columns(ordinal).getShort(batchIdx - 1, 0)
    }

    override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
      null
    }

    override def getInt(ordinal: Int): Int = {
      columns(ordinal).getInt(batchIdx - 1, 0)
    }

    override def isNullAt(ordinal: Int): Boolean = {
      columns(ordinal).isNull(batchIdx - 1)
    }
  }

}


