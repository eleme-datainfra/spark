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

package org.apache.spark.sql.execution.datasources.orc;


import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.orc.*;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.execution.datasources.parquet.CatalystRowConverter;
import org.apache.spark.sql.execution.datasources.parquet.CatalystSchemaConverter;
import org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;
import org.joda.time.DateTimeZone;
import parquet.schema.OriginalType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TimeZone;

import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static org.apache.parquet.column.ValuesType.*;

/**
 * A specialized RecordReader that reads into UnsafeRows directly using the presto column APIs.
 *
 * This is somewhat based on parquet-mr's ColumnReader.
 *
 * TODO: handle complex types, decimal requiring more than 8 bytes, INT96. Schema mismatch.
 * All of these can be handled efficiently and easily with codegen.
 */
public class UnsafeRowOrcRecordReader extends RecordReader<Void, UnsafeRow> {
  /**
   * Batch of unsafe rows that we assemble and the current index we've returned. Everytime this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private UnsafeRow[] rows = new UnsafeRow[1024];
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * Used to write variable length columns. Same length as `rows`.
   */
  private UnsafeRowWriter[] rowWriters = null;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * The total number of rows this RecordReader will eventually read. The sum of the
   * rows of all the row groups.
   */
  protected long totalRowCount;

  /**
   * The default size for varlen columns. The row grows as necessary to accommodate the
   * largest column.
   */
  private static final int DEFAULT_VAR_LEN_SIZE = 32;

  /**
   * Tries to initialize the reader for this split. Returns true if this reader supports reading
   * this split and false otherwise.
   */
  public boolean tryInitialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    try {
      initialize(inputSplit, taskAttemptContext);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit)inputSplit;
    OrcDataSource orcDataSource;
    Path path = fileSplit.getPath();
    MetadataReader metadataReader = new DwrfMetadataReader();
    DataSize maxMergeDistance = new DataSize(1, DataSize.Unit.MEGABYTE);
    DataSize maxBufferSize = new DataSize(8, DataSize.Unit.MEGABYTE);
    DataSize streamBufferSize = new DataSize(8, DataSize.Unit.MEGABYTE);
    DateTimeZone hiveStorageTimeZone = DateTimeZone.forTimeZone(
        TimeZone.getTimeZone(TimeZone.getDefault().getID()));
    try {
      FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
      long size = fileSystem.getFileStatus(path).getLen();
      FSDataInputStream inputStream = fileSystem.open(path);
      orcDataSource = new HdfsOrcDataSource(path.toString(),
          size,
          maxMergeDistance,
          maxBufferSize,
          streamBufferSize,
          inputStream);
    } catch (Exception e) {
      if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
          e instanceof FileNotFoundException) {
        throw new IOException("Error open split " + inputSplit.toString(), e.getCause());
      }
      throw new IOException(format("Error opening Hive split %s (offset=%s, length=%s): %s",
          path, ((FileSplit) inputSplit).getStart(), inputSplit.getLength(), e.getMessage()));
    }

    AggregatedMemoryContext systemMemoryUsage = new AggregatedMemoryContext();
    try {
      OrcReader reader = new OrcReader(orcDataSource, metadataReader, maxMergeDistance, maxBufferSize);

      List<HiveColumnHandle> physicalColumns =
          getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader, path);
      ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
      ImmutableList.Builder<TupleDomainOrcPredicate.ColumnReference<HiveColumnHandle>>
          columnReferences = ImmutableList.builder();
      for (HiveColumnHandle column : physicalColumns) {
        if (!column.isPartitionKey()) {
          Type type = typeManager.getType(column.getTypeSignature());
          includedColumns.put(column.getHiveColumnIndex(), type);
          columnReferences.add(new TupleDomainOrcPredicate.ColumnReference<>(column,
              column.getHiveColumnIndex(), type));
        }
      }

      OrcPredicate predicate = new TupleDomainOrcPredicate<>(effectivePredicate,
          columnReferences.build());

      OrcRecordReader recordReader = reader.createRecordReader(
          includedColumns.build(),
          predicate,
          hiveStorageTimeZone,
          systemMemoryUsage);
    } finally {

    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (batchIdx >= numBatched) {
      if (!loadBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public UnsafeRow getCurrentValue() throws IOException, InterruptedException {
    return rows[batchIdx - 1];
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float) rowsReturned / totalRowCount;
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Decodes a batch of values into `rows`. This function is the hot path.
   */
  private boolean loadBatch() throws IOException {
    // no more records left
    if (rowsReturned >= totalRowCount) { return false; }

    int num = (int)Math.min(rows.length, totalCountLoadedSoFar - rowsReturned);
    rowsReturned += num;

    reco

    return true;
  }
}
