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
import org.apache.spark.sql.catalyst.expressions.MutableRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.joda.time.DateTimeZone;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class JavaFasterOrcRecordReader extends RecordReader<NullWritable, InternalRow> {

    private HiveColumnInfo[] output;
    private Map<Integer, PartitionInfo> partitions;
    private List<ColumnReference<HiveColumnHandle>> columnReferences;
    private long totalRowCount;
    private OrcRecordReader recordReader;
    private Map<Integer, Block> partitionBlocks;
    private int MAX_BATCH_SIZE = 1024;
    private OrcRow row = new OrcRow();
    private Block[] columns = new Block[partitions.size() + output.length];


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

    public OrcRow getRow(int batchIdx) {
        row.init(batchIdx);
        return row;
    }

    public class OrcRow extends MutableRow {
        int batchIdx = 0;
        long length = 0;
        int numFields = numFields();
        boolean[] valueIsNull = new boolean[numFields];

        @Override
        public int numFields() {
            return partitions.size() + output.length;
        }

        public void init(int batchIdx) {
            this.batchIdx = batchIdx;
            for (int i = 0; i < valueIsNull.length; i++) {
                valueIsNull[i] = false;
            }
        }

        @Override
        public InternalRow copy() {
            return null;
        }

        @Override
        public boolean anyNull() {
            return false;
        }

        @Override
        public void setDecimal(int i, Decimal value, int precision) {
            super.setDecimal(i, value, precision);
        }

        @Override
        public void setNullAt(int i) {
            valueIsNull[i] = true;
        }

        @Override
        public void update(int i, Object value) {

        }

        @Override
        public void setBoolean(int i, boolean value) {
            super.setBoolean(i, value);
        }

        @Override
        public void setByte(int i, byte value) {
            super.setByte(i, value);
        }

        @Override
        public void setShort(int i, short value) {
            super.setShort(i, value);
        }

        @Override
        public void setInt(int i, int value) {
            super.setInt(i, value);
        }

        @Override
        public void setLong(int i, long value) {
            super.setLong(i, value);
        }

        @Override
        public void setFloat(int i, float value) {
            super.setFloat(i, value);
        }

        @Override
        public void setDouble(int i, double value) {
            super.setDouble(i, value);
        }

        @Override
        public boolean isNullAt(int ordinal) {
            return valueIsNull[ordinal] || columns[ordinal] == null ||
                columns[ordinal].isNull(batchIdx - 1);
        }

        @Override
        public boolean getBoolean(int ordinal) {
            return false;
        }

        @Override
        public byte getByte(int ordinal) {
            return 0;
        }

        @Override
        public short getShort(int ordinal) {
            return 0;
        }

        @Override
        public int getInt(int ordinal) {
            return 0;
        }

        @Override
        public long getLong(int ordinal) {
            return 0;
        }

        @Override
        public float getFloat(int ordinal) {
            return 0;
        }

        @Override
        public double getDouble(int ordinal) {
            return 0;
        }

        @Override
        public Decimal getDecimal(int ordinal, int precision, int scale) {
            return null;
        }

        @Override
        public UTF8String getUTF8String(int ordinal) {
            return null;
        }

        @Override
        public byte[] getBinary(int ordinal) {
            return new byte[0];
        }

        @Override
        public CalendarInterval getInterval(int ordinal) {
            return null;
        }

        @Override
        public InternalRow getStruct(int ordinal, int numFields) {
            return null;
        }

        @Override
        public ArrayData getArray(int ordinal) {
            return null;
        }

        @Override
        public MapData getMap(int ordinal) {
            return null;
        }

        @Override
        public Object get(int ordinal, DataType dataType) {
            return null;
        }
    }
}

