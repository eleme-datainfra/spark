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
import io.airlift.slice.{Slices, Slice}
import io.airlift.units.DataSize
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericMutableRow, MutableRow}
import org.apache.spark.sql.types._
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
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(taskAttemptContext)
    initialize(fileSplit, conf)
  }

  def initialize(fileSplit: FileSplit, conf: Configuration): Unit = {
    output.foreach { col =>
      val dt = col._2
      if (dt.isInstanceOf[UserDefinedType[_]]) {
        throw new IOException("Unsupported type: " + dt)
      }
    }

    var orcDataSource: OrcDataSource = null
    val metadataReader: MetadataReader = new OrcMetadataReader
    val maxMergeDistance: DataSize = new DataSize(1, DataSize.Unit.MEGABYTE)
    val maxBufferSize: DataSize = new DataSize(8, DataSize.Unit.MEGABYTE)
    val streamBufferSize: DataSize = new DataSize(8, DataSize.Unit.MEGABYTE)
    val hiveStorageTimeZone: DateTimeZone = DateTimeZone.forTimeZone(
      TimeZone.getTimeZone(TimeZone.getDefault.getID))
    val path = fileSplit.getPath
    try {
      val fileSystem = path.getFileSystem(conf)
      val size = fileSystem.getFileStatus(path).getLen
      var inputStream: FSDataInputStream = null
      if (fileSystem.isDirectory(path)) {
        // for test
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
      fileSplit.getStart, fileSplit.getLength, hiveStorageTimeZone, systemMemoryUsage)
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

  object BlockRow {
    var row: BlockRow = null
    def getRow(batchIdx: Int): BlockRow = {
      row = new BlockRow(batchIdx)
      row.init()
      row
    }
  }

  class BlockRow(var batchIdx: Int) extends MutableRow {
    var length = 0
    var valueIsNull: Slice = Slices.allocate(output.size)

    def setBatchIndex(batchIdx: Int): Unit = {
      this.batchIdx = batchIdx
    }

    def init(): Unit = {
      for (i <- 0 to out.size) {
        valueIsNull.setByte(i, 0)
      }
    }

    override def setNullAt(ordinal: Int): Unit = {
      valueIsNull.setByte(ordinal, 1)
    }

    override def update(ordinal: Int, value: Any): Unit = {
      if (value == null) {
        setNullAt(ordinal)
      } else {
        val dt = output(ordinal)._2
        if (dt.isInstanceOf[BooleanType]) {
          setBoolean(ordinal, value.asInstanceOf[BooleanType])
        } else if (dt.isInstanceOf[IntegerType]) {
          setInt(ordinal, value.asInstanceOf[Int])
        } else if (dt.isInstanceOf[ShortType]) {
          setShort(ordinal, value.asInstanceOf[Short])
        } else if (dt.isInstanceOf[LongType]) {
          setLong(ordinal, value.asInstanceOf[Long])
        } else if (dt.isInstanceOf[FloatType]) {
          setFloat(ordinal, value.asInstanceOf[Float])
        } else if (dt.isInstanceOf[DoubleType]) {
          setDouble(ordinal, value.asInstanceOf[Double])
        } else if (dt.isInstanceOf[DecimalType]) {
          val t = dt.asInstanceOf[DecimalType]
          setDecimal(ordinal, Decimal.apply(value.asInstanceOf[BigDecimal],
            t.precision(), t.scale()), t.precision());
        } else {
          throw new UnsupportedOperationException("Datatype not supported " + dt)
        }
      }
    }

    /** Returns true if there are any NULL values in this row. */
    override def anyNull: Boolean = {
      for (i <- 0 to out.size) {
        if (valueIsNull.getByte(i) == 1) {
          return true
        }
      }
      !columns.filter(b => b.isNull(batchIdx - 1)).isEmpty
    }

    override def numFields: Int = output.size

    override def setBoolean(ordinal: Int, value: Boolean): Unit = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).setByte(0, if (value) 1 else 0)
    }

    override def setByte(ordinal: Int, value: Byte): Unit = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).setByte(0, value)
    }

    override def setShort(ordinal: Int, value: Short): Unit = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).setShort(0, value)
    }

    override def setInt(ordinal: Int, value: Int): Unit = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).setInt(0, value)
    }

    override def setLong(ordinal: Int, value: Long): Unit = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).setLong(0, value)
    }

    override def setFloat(ordinal: Int, value: Float): Unit = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).setFloat(0, value)
    }

    override def setDouble(ordinal: Int, value: Double): Unit = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).setDouble(0, value)
    }

    override def setDecimal(ordinal: Int, value: Decimal, precision: Int): Unit = {
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).setLong(0, value.toUnscaledLong)
    }

    override def get(ordinal: Int, dataType: DataType): Object = {
      val block = columns(ordinal)
      val index = batchIdx - 1
      if (block.isNull(index) || dataType.isInstanceOf[NullType]) {
        return null
      } else {
        if (dataType.isInstanceOf[BooleanType]) {
          (block.getByte(index) != 0).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[ByteType]) {
          block.getByte(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[BinaryType]) {
          length = block.getLength(batchIdx - 1)
          block.getSlice(index, 0, length).getBytes
        } else if (dataType.isInstanceOf[IntegerType]) {
          block.getInt(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[ShortType]) {
          block.getShort(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[LongType]) {
          block.getLong(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[FloatType]) {
          block.getFloat(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[DoubleType]) {
          block.getDouble(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[DateType]) {
          block.getInt(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[DecimalType]) {
          val dt = dataType.asInstanceOf[DecimalType]
          Decimal.apply(block.getLong(index, 0), dt.precision, dt.scale)
        } else if (dataType.isInstanceOf[StringType]) {
          length = block.getLength(index)
          UTF8String.fromBytes(block.getSlice(index, 0, length).getBytes)
        } else if (dataType.isInstanceOf[TimestampType]) {
          block.getLong(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[ArrayType]) {
          val elementType = dataType.asInstanceOf[ArrayType].elementType
          val arrayBlock = block.getObject(index, classOf[Block])
          val array = new Array[Object](arrayBlock.getPositionCount)
          for (i <- 0 to arrayBlock.getPositionCount) {
            array(i) = get(arrayBlock, i, elementType)
          }
          new GenericArrayData(array)
        } else if (dataType.isInstanceOf[MapType]) {
          val dt = dataType.asInstanceOf[MapType]
          val mapBlock = block.getObject(position, classOf[Block])
          val keyArray = new Array[Object](mapBlock.getPositionCount)
          val valueArray = new Array[Object](mapBlock.getPositionCount)
          var i = 0
          var j = 0
          while (i < mapBlock.getPositionCount) {
            keyArray(j) = get(mapBlock, i, dt.keyType)
            valueArray(j) = get(mapBlock, i + 1, dt.valueType)
            j = j + 1
            i = i + 2
          }
          new ArrayBasedMapData(keyArray, valueArray)
        } else if (dataType.isInstanceOf[StructType]) {
          val dt = dataType.asInstanceOf[StructType]
          val block = block.getObject(index, classOf[Block])
          val values = new Array[Object](block.getPositionCount)
          for (i <- 0 to block.getPositionCount) {
            values(i) = get(block, i, dt.apply(i).dataType)
          }
          new GenericMutableRow(values)
        } else if (dataType.isInstanceOf[UserDefinedType]) {
          get(index, dataType.asInstanceOf[UserDefinedType].sqlType)
        } else {
          throw new UnsupportedOperationException("Datatype not supported " + dt)
        }
      }
    }

    def get(block: Block, index: Int, dataType: DataType): Object = {
      if (block.isNull(index) || dataType.isInstanceOf[NullType]) {
        return null
      } else {
        if (dataType.isInstanceOf[BooleanType]) {
          (block.getByte(index) != 0).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[ByteType]) {
          block.getByte(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[BinaryType]) {
          length = block.getLength(batchIdx - 1)
          block.getSlice(index, 0, length).getBytes
        } else if (dataType.isInstanceOf[IntegerType]) {
          block.getInt(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[ShortType]) {
          block.getShort(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[LongType]) {
          block.getLong(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[FloatType]) {
          block.getFloat(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[DoubleType]) {
          block.getDouble(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[DateType]) {
          block.getInt(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[DecimalType]) {
          val dt = dataType.asInstanceOf[DecimalType]
          Decimal.apply(block.getLong(index, 0), dt.precision, dt.scale)
        } else if (dataType.isInstanceOf[StringType]) {
          length = block.getLength(index)
          UTF8String.fromBytes(block.getSlice(index, 0, length).getBytes)
        } else if (dataType.isInstanceOf[TimestampType]) {
          block.getLong(index).asInstanceOf[AnyRef]
        } else if (dataType.isInstanceOf[ArrayType]) {
          val elementType = dataType.asInstanceOf[ArrayType].elementType
          val arrayBlock = block.getObject(index, classOf[Block])
          val array = new Array[Object](arrayBlock.getPositionCount)
          for (i <- 0 to arrayBlock.getPositionCount) {
            array(i) = get(arrayBlock, i, elementType)
          }
          new GenericArrayData(array)
        } else if (dataType.isInstanceOf[MapType]) {
          val dt = dataType.asInstanceOf[MapType]
          val mapBlock = block.getObject(position, classOf[Block])
          val keyArray = new Array[Object](mapBlock.getPositionCount/2)
          val valueArray = new Array[Object](mapBlock.getPositionCount/2)
          var i = 0
          var j = 0
          while (i < mapBlock.getPositionCount) {
            keyArray(j) = get(mapBlock, i, dt.keyType)
            valueArray(j) = get(mapBlock, i + 1, dt.valueType)
            j = j + 1
            i = i + 2
          }
          new ArrayBasedMapData(keyArray, valueArray)
        } else if (dataType.isInstanceOf[StructType]) {
          val dt = dataType.asInstanceOf[StructType]
          val block = block.getObject(index, classOf[Block])
          val values = new Array[Object](block.getPositionCount)
          for (i <- 0 to block.getPositionCount) {
            values(i) = get(block, i, dt.apply(i).dataType)
          }
          new GenericMutableRow(values)
        } else if (dataType.isInstanceOf[UserDefinedType]) {
          get(block, batchIdx - 1, dataType.asInstanceOf[UserDefinedType].sqlType)
        } else {
          throw new UnsupportedOperationException("Datatype not supported " + dt)
        }
      }
    }

    override def getBinary(ordinal: Int): Array[Byte] = {
      if (isNullAt(ordinal)) return null
      length = columns(ordinal).getLength(batchIdx - 1)
      columns(ordinal).getSlice(batchIdx - 1, 0, length).getBytes
    }

    override def getDouble(ordinal: Int): Double = {
      columns(ordinal).getDouble(batchIdx - 1, 0)
    }

    override def getArray(ordinal: Int): ArrayData = {
      if (isNullAt(ordinal)) return null
      val elementType = output(ordinal)._2.asInstanceOf[ArrayType].elementType
      val arrayBlock = columns(ordinal).getObject(batchIdx - 1, classOf[Block])
      val array = new Array[Object](arrayBlock.getPositionCount)
      for (i <- 0 to arrayBlock.getPositionCount) {
        array(i) = get(arrayBlock, i, elementType)
      }
      new GenericArrayData(array)
    }

    override def getUTF8String(ordinal: Int): UTF8String = {
      if (isNullAt(ordinal)) return null
      length = columns(ordinal).getLength(batchIdx - 1)
      UTF8String.fromBytes(columns(ordinal).getSlice(batchIdx - 1, 0, length).getBytes)
    }

    override def getInterval(ordinal: Int): CalendarInterval = {
      throw new UnsupportedOperationException("Unsupported data type CalendarInterval")
    }

    override def getFloat(ordinal: Int): Float = {
      columns(ordinal).getDouble(batchIdx - 1, 0).toFloat
    }

    override def getLong(ordinal: Int): Long = {
      columns(ordinal).getLong(batchIdx - 1, 0)
    }

    override def getMap(ordinal: Int): MapData = {
      if (isNullAt(ordinal)) return null
      val dt = output(ordinal)._2.asInstanceOf[MapType]
      val mapBlock = columns(ordinal).getObject(batchIdx - 1, classOf[Block])
      val keyArray = new Array[Object](mapBlock.getPositionCount/2)
      val valueArray = new Array[Object](mapBlock.getPositionCount/2)
      var i = 0
      var j = 0
      while (i < mapBlock.getPositionCount) {
        keyArray(j) = get(mapBlock, i, dt.keyType)
        valueArray(j) = get(mapBlock, i + 1, dt.valueType)
        j = j + 1
        i = i + 2
      }
      new ArrayBasedMapData(keyArray, valueArray)
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
      if (isNullAt(ordinal)) return null
      val dt = output(ordinal)._2.asInstanceOf[StructType]
      val block = columns(ordinal).getObject(batchIdx - 1, classOf[Block])
      val values = new Array[Object](block.getPositionCount)
      for (i <- 0 to block.getPositionCount) {
        values(i) = get(block, i, dt.apply(i).dataType)
      }
      new GenericMutableRow(values)
    }

    override def getInt(ordinal: Int): Int = {
      columns(ordinal).getInt(batchIdx - 1, 0)
    }

    override def isNullAt(ordinal: Int): Boolean = {
      valueIsNull.getByte(batchIdx - 1) != 0 || columns(ordinal).isNull(batchIdx - 1)
    }
  }

}


