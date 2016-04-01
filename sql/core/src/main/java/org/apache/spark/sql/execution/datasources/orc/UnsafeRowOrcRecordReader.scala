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


package org.apache.spark.sql.execution.datasources.orc

import com.facebook.presto.orc.OrcRecordReader
import org.apache.hadoop.mapreduce.{TaskAttemptContext, InputSplit, RecordReader}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow


class UnsafeRowOrcRecordReader extends RecordReader[Void, UnsafeRow] {

  val reader: OrcRecordReader;
  /**
    * Batch of unsafe rows that we assemble and the current index we've returned. Everytime this
    * batch is used up (batchIdx == numBatched), we populated the batch.
    */
  private var rows: Array[UnsafeRow] = new Array[UnsafeRow](64)
  private var batchIdx: Int = 0
  private var numBatched: Int = 0

  /**
    * True if the row contains variable length fields.
    */
  private var containsVarLenFields: Boolean = false
  /**
    * The number of bytes in the fixed length portion of the row.
    */
  private var fixedSizeBytes: Int = 0

  /**
    * Total number of rows.
    */
  private var totalRowCount: Long = 0L

  /**
    * The number of rows that have been returned.
    */
  private var rowsReturned: Long = 0L
  /**
    * The number of rows that have been reading, including the current in flight stripe.
    */
  private var totalCountLoadedSoFar: Long = 0L

  /**
    * The default size for varlen columns. The row grows as necessary to accommodate the
    * largest column.
    */
  private val DEFAULT_VAR_LEN_SIZE: Int = 32


  override def getProgress: Float = {
    reader.getProgress
  }

  override def nextKeyValue(): Boolean = {
    if (batchIdx >= numBatched) {
      if (!loadBatch) return false
    }
    batchIdx += 1
    return true
  }

  def loadBatch(): Boolean = {
    // no more records left
    if (rowsReturned >= totalRowCount) {
      return false
    }
    val num = Math.min(rows.length, totalCountLoadedSoFar - rowsReturned).toInt
    rowsReturned = rowsReturned + num
    return false
  }

  override def getCurrentValue: UnsafeRow = {

  }

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {

  }

  override def getCurrentKey: Void = {
    return null;
  }

  override def close(): Unit = {
    reader.close()
  }
}
