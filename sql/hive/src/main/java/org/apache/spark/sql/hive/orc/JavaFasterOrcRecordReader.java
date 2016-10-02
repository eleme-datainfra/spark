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

package org.apache.spark.sql.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.*;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.predicate.TupleDomain;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.*;
import org.joda.time.DateTimeZone;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class JavaFasterOrcRecordReader extends RecordReader<NullWritable, InternalRow> {

    HiveColumnInfo[] output;
    Map<Integer, PartitionInfo> partitions;
    List<ColumnReference<HiveColumnHandle>> columnReferences;
    long totalRowCount;
    OrcRecordReader recordReader;
    Map<Integer, Block> partitionBlocks;
    int MAX_BATCH_SIZE = 1024;


    public JavaFasterOrcRecordReader(
        HiveColumnInfo[] output,
        Map<Integer, PartitionInfo> partitions,
        List<ColumnReference<HiveColumnHandle>> columnReferences) {
        this.output = output;
        this.partitions = partitions;
        this.columnReferences = columnReferences;
    }

    public static class HiveColumnInfo {
        public int fieldIndex;
        public DataType dataType;
        public Type type;

        public HiveColumnInfo(int fieldIndex, DataType dataType, Type type) {
            this.fieldIndex = fieldIndex;
            this.dataType = dataType;
            this.type = type;
        }
    }

    public static class PartitionInfo {
        public DataType dataType;
        public String value;
        public PartitionInfo(DataType dataType, String value) {
            this.dataType = dataType;
            this.value = value;
        }
    }
    /**
     * Tries to initialize the reader for this split. Returns true if this reader supports reading
     * this split and false otherwise.
     */
    public boolean tryInitialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
        throws Exception {
        try {
            initialize(inputSplit, taskAttemptContext);
            return true;
        } catch(Exception e) {
            throw e;
        }
    }


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)inputSplit;
        Configuration conf =
            SparkHadoopUtil.get().getConfigurationFromJobContext(taskAttemptContext);
        initialize(fileSplit, conf);
    }

    public void initialize(FileSplit fileSplit, Configuration conf)
        throws IOException, InterruptedException {
        OrcDataSource orcDataSource = null;
        MetadataReader metadataReader = new OrcMetadataReader();
        DataSize maxMergeDistance = new DataSize(1, DataSize.Unit.MEGABYTE);
        DataSize maxBufferSize = new DataSize(8, DataSize.Unit.MEGABYTE);
        DataSize streamBufferSize = new DataSize(8, DataSize.Unit.MEGABYTE);
        DateTimeZone hiveStorageTimeZone = DateTimeZone.forTimeZone(
            TimeZone.getTimeZone(TimeZone.getDefault().getID()));
        Path path = fileSplit.getPath();
        try {
            FileSystem fileSystem = path.getFileSystem(conf);
            long size = fileSystem.getFileStatus(path).getLen();
            FSDataInputStream inputStream = null;
            if (fileSystem.isDirectory(path)) {
                // for test
                FileStatus[] childPaths = fileSystem.listStatus(path);
                Path childPath = childPaths[1].getPath();
                inputStream = fileSystem.open(childPath);
                orcDataSource = new HdfsOrcDataSource(childPath.toString(), childPaths[1].getLen(),
                    maxMergeDistance, maxBufferSize, streamBufferSize, inputStream);
            } else {
                inputStream = fileSystem.open(path);
                orcDataSource = new HdfsOrcDataSource(path.toString(), size, maxMergeDistance,
                    maxBufferSize, streamBufferSize, inputStream);
            }
        } catch(Exception e) {
            if ((e.getMessage().trim() == "Filesystem closed")
                || e instanceof FileNotFoundException) {
                    throw new IOException("Error open split " + path.toString(), e.getCause());
            }
            throw new IOException("Error opening Hive split " + path );
        }

        AggregatedMemoryContext systemMemoryUsage = new AggregatedMemoryContext();

        OrcReader reader = new OrcReader(orcDataSource, metadataReader,
            maxMergeDistance, maxBufferSize);

        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.all();
        TupleDomainOrcPredicate<HiveColumnHandle> predicate =
            new TupleDomainOrcPredicate<>(effectivePredicate,
            columnReferences);

        HashMap<Integer, Type> columns = new HashMap<>();
        for (HiveColumnInfo info : output) {
            columns.put(info.fieldIndex, info.type);
        }

        recordReader = reader.createRecordReader(columns, predicate,
            fileSplit.getStart(), fileSplit.getLength(), hiveStorageTimeZone, systemMemoryUsage);
        totalRowCount = recordReader.getReaderRowCount();

        for (Map.Entry<Integer, PartitionInfo> p : partitions.entrySet()) {
            partitionBlocks.put(p.getKey(),
                buildSingleValueBlock(Slices.utf8Slice(p.getValue().value)));
        }
    }

    public Block buildSingleValueBlock(final Slice value) {
        SliceArrayBlock dictionary = new SliceArrayBlock(1, new Slice[]{value});
        return new DictionaryBlock(MAX_BATCH_SIZE, dictionary,
            Slices.wrappedIntArray(new int[MAX_BATCH_SIZE], 0, MAX_BATCH_SIZE));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public InternalRow getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}

