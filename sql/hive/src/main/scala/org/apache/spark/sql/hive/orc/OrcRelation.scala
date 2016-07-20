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

import java.util.Properties

import com.facebook.presto.`type`.TypeRegistry
import com.facebook.presto.hive.{HiveColumnHandle, HiveType}
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference
import com.facebook.presto.spi.`type`.Type
import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfoUtils}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.mapred.{InputFormat => MapRedInputFormat, JobConf, OutputFormat => MapRedOutputFormat, RecordWriter, Reporter}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputFormat, Job, TaskAttemptContext}

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.rdd.{SqlNewHadoopRDD, HadoopRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.hive.{HiveContext, HiveInspectors, HiveMetastoreTypes, HiveShim}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.util.{Utils, SerializableConfiguration}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[sql] class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  override def shortName(): String = "orc"

  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    assert(
      sqlContext.isInstanceOf[HiveContext],
      "The ORC data source can only be used with HiveContext.")

    new OrcRelation(paths, dataSchema, None, partitionColumns, parameters)(sqlContext)
  }
}

private[orc] class OrcOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext)
  extends OutputWriter with SparkHadoopMapRedUtil with HiveInspectors {

  private val serializer = {
    val table = new Properties()
    table.setProperty("columns", dataSchema.fieldNames.mkString(","))
    table.setProperty("columns.types", dataSchema.map { f =>
      HiveMetastoreTypes.toMetastoreType(f.dataType)
    }.mkString(":"))

    val serde = new OrcSerde
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    serde.initialize(configuration, table)
    serde
  }

  // Object inspector converted from the schema of the relation to be written.
  private val structOI = {
    val typeInfo =
      TypeInfoUtils.getTypeInfoFromTypeString(
        HiveMetastoreTypes.toMetastoreType(dataSchema))

    OrcStruct.createObjectInspector(typeInfo.asInstanceOf[StructTypeInfo])
      .asInstanceOf[SettableStructObjectInspector]
  }

  // `OrcRecordWriter.close()` creates an empty file if no rows are written at all.  We use this
  // flag to decide whether `OrcRecordWriter.close()` needs to be called.
  private var recordWriterInstantiated = false

  private lazy val recordWriter: RecordWriter[NullWritable, Writable] = {
    recordWriterInstantiated = true

    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    val uniqueWriteJobId = conf.get("spark.sql.sources.writeJobUUID")
    val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
    val partition = taskAttemptId.getTaskID.getId
    val filename = f"part-r-$partition%05d-$uniqueWriteJobId.orc"

    new OrcOutputFormat().getRecordWriter(
      new Path(path, filename).getFileSystem(conf),
      conf.asInstanceOf[JobConf],
      new Path(path, filename).toString,
      Reporter.NULL
    ).asInstanceOf[RecordWriter[NullWritable, Writable]]
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  private def wrapOrcStruct(
      struct: OrcStruct,
      oi: SettableStructObjectInspector,
      row: InternalRow): Unit = {
    val fieldRefs = oi.getAllStructFieldRefs
    var i = 0
    while (i < fieldRefs.size) {
      oi.setStructFieldData(
        struct,
        fieldRefs.get(i),
        wrap(
          row.get(i, dataSchema(i).dataType),
          fieldRefs.get(i).getFieldObjectInspector,
          dataSchema(i).dataType))
      i += 1
    }
  }

  val cachedOrcStruct = structOI.create().asInstanceOf[OrcStruct]

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    wrapOrcStruct(cachedOrcStruct, structOI, row)

    recordWriter.write(
      NullWritable.get(),
      serializer.serialize(cachedOrcStruct, structOI))
  }

  override def close(): Unit = {
    if (recordWriterInstantiated) {
      recordWriter.close(Reporter.NULL)
    }
  }
}

private[sql] class OrcRelation(
    override val paths: Array[String],
    maybeDataSchema: Option[StructType],
    maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    parameters: Map[String, String])(
    @transient val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec, parameters)
  with Logging {

  private[sql] def this(
      paths: Array[String],
      maybeDataSchema: Option[StructType],
      maybePartitionSpec: Option[PartitionSpec],
      parameters: Map[String, String])(
      sqlContext: SQLContext) = {
    this(
      paths,
      maybeDataSchema,
      maybePartitionSpec,
      maybePartitionSpec.map(_.partitionColumns),
      parameters)(sqlContext)
  }

  override val dataSchema: StructType = maybeDataSchema.getOrElse {
    OrcFileOperator.readSchema(
      paths.head, Some(sqlContext.sparkContext.hadoopConfiguration))
  }

  override def needConversion: Boolean = false

  override def equals(other: Any): Boolean = other match {
    case that: OrcRelation =>
      paths.toSet == that.paths.toSet &&
        dataSchema == that.dataSchema &&
        schema == that.schema &&
        partitionColumns == that.partitionColumns
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(
      paths.toSet,
      dataSchema,
      schema,
      partitionColumns)
  }

  override private[sql] def buildInternalScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[InternalRow] = {
    val output = StructType(requiredColumns.map(dataSchema(_))).toAttributes
    OrcTableScan(output, this, filters, inputPaths, broadcastedConf).execute()
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    val jobConf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)
    parameters.foreach { p =>
      if (p._1.startsWith("orc.") && jobConf.get(p._1) != null) {
        jobConf.set(p._1, p._2)
      }
    }
    jobConf match {
      case conf: JobConf =>
        conf.setOutputFormat(classOf[OrcOutputFormat])
      case conf =>
        conf.setClass(
          "mapred.output.format.class",
          classOf[OrcOutputFormat],
          classOf[MapRedOutputFormat[_, _]])
    }

    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter = {
        new OrcOutputWriter(path, dataSchema, context)
      }
    }
  }
}

private[orc] case class OrcTableScan(
    attributes: Seq[Attribute],
    @transient relation: OrcRelation,
    filters: Array[Filter],
    @transient inputPaths: Array[FileStatus],
    broadcastedConf: Broadcast[SerializableConfiguration])
  extends Logging
  with HiveInspectors {

  @transient private val sqlContext = relation.sqlContext

  private def addColumnIds(
      output: Seq[Attribute],
      relation: OrcRelation,
      conf: Configuration): Unit = {
    val ids = output.map(a => relation.dataSchema.fieldIndex(a.name): Integer)
    val (sortedIds, sortedNames) = ids.zip(attributes.map(_.name)).sorted.unzip
    HiveShim.appendReadColumns(conf, sortedIds, sortedNames)
  }

  def addIncludeColumnsInfo(
      output: Seq[Attribute],
      dataSchema: StructType): SerializableColumnInfo = {
    val typeManager = new TypeRegistry()
    val columnReferences = new java.util.ArrayList[ColumnReference[HiveColumnHandle]]
    var outputAttrs = new mutable.ArrayBuffer[(Int, DataType, Type)]
    output.foreach { a =>
      val fieldIndex = dataSchema.fieldIndex(a.name)
      val mType = HiveMetastoreTypes.toMetastoreType(a.dataType)
      val hiveType = HiveType.valueOf(mType)
      val pType = typeManager.getType(hiveType.getTypeSignature)
      columnReferences.add(new ColumnReference(
        new HiveColumnHandle("", a.name, hiveType, hiveType.getTypeSignature, fieldIndex, false),
        fieldIndex,
        pType))
      outputAttrs += ((fieldIndex, a.dataType, pType))
    }

    SerializableColumnInfo(outputAttrs.toArray,
      Map.empty[Int, (DataType, String)],
      columnReferences)
  }

  private def mapDataTypeToType(dataType: DataType, typeManager: TypeRegistry): Type = {
    val mType = HiveMetastoreTypes.toMetastoreType(dataType)
    val hiveType = HiveType.valueOf(mType)
    typeManager.getType(hiveType.getTypeSignature)
  }

  // Transform all given raw `Writable`s into `InternalRow`s.
  private def fillObject(
      path: String,
      conf: Configuration,
      iterator: Iterator[Writable],
      nonPartitionKeyAttrs: Seq[Attribute]): Iterator[InternalRow] = {
    val deserializer = new OrcSerde
    val maybeStructOI = OrcFileOperator.getObjectInspector(path, Some(conf))
    val mutableRow = new SpecificMutableRow(attributes.map(_.dataType))
    val unsafeProjection = UnsafeProjection.create(StructType.fromAttributes(nonPartitionKeyAttrs))

    // SPARK-8501: ORC writes an empty schema ("struct<>") to an ORC file if the file contains zero
    // rows, and thus couldn't give a proper ObjectInspector.  In this case we just return an empty
    // partition since we know that this file is empty.
    maybeStructOI.map { soi =>
      val (fieldRefs, fieldOrdinals) = nonPartitionKeyAttrs.zipWithIndex.map {
        case (attr, ordinal) =>
          soi.getStructFieldRef(attr.name) -> ordinal
      }.unzip
      val unwrappers = fieldRefs.map(unwrapperFor)
      // Map each tuple to a row object
      iterator.map { value =>
        val raw = deserializer.deserialize(value)
        var i = 0
        while (i < fieldRefs.length) {
          val fieldValue = soi.getStructFieldData(raw, fieldRefs(i))
          if (fieldValue == null) {
            mutableRow.setNullAt(fieldOrdinals(i))
          } else {
            unwrappers(i)(fieldValue, mutableRow, fieldOrdinals(i))
          }
          i += 1
        }
        unsafeProjection(mutableRow)
      }
    }.getOrElse {
      Iterator.empty
    }
  }

  def execute(): RDD[InternalRow] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

    if (inputPaths.isEmpty) {
      // the input path probably be pruned, return an empty RDD.
      return sqlContext.sparkContext.emptyRDD[InternalRow]
    }

    FileInputFormat.setInputPaths(job, inputPaths.map(_.getPath): _*)

    if (sqlContext.conf.useFasterOrcReader) {
      Utils.withDummyCallSite(sqlContext.sparkContext) {
        val includedColumnsInfo = addIncludeColumnsInfo(attributes, relation.dataSchema)
        val inputFormatClass = classOf[OrcNewInputFormat]
          .asInstanceOf[Class[_ <: InputFormat[NullWritable, InternalRow]]]
        new FasterOrcRDD[InternalRow](
          sqlContext,
          broadcastedConf,
          includedColumnsInfo,
          inputFormatClass,
          classOf[InternalRow])
      }
    } else {
      val inputFormatClass = classOf[OrcInputFormat]
        .asInstanceOf[Class[_ <: MapRedInputFormat[NullWritable, Writable]]]
      // Tries to push down filters if ORC filter push-down is enabled
      if (sqlContext.conf.orcFilterPushDown) {
        OrcFilters.createFilter(filters).foreach { f =>
          conf.set(OrcTableScan.SARG_PUSHDOWN, f.toKryo)
          conf.setBoolean(ConfVars.HIVEOPTINDEXFILTER.varname, true)
        }
      }

      // Sets requested columns
      addColumnIds(attributes, relation, conf)

      val rdd = sqlContext.sparkContext.hadoopRDD(
        conf.asInstanceOf[JobConf],
        inputFormatClass,
        classOf[NullWritable],
        classOf[Writable]
      ).asInstanceOf[HadoopRDD[NullWritable, Writable]]

      val wrappedConf = new SerializableConfiguration(conf)

      rdd.mapPartitionsWithInputSplit { case (split: OrcSplit, iterator) =>
        val writableIterator = iterator.map(_._2)
        fillObject(split.getPath.toString, wrappedConf.value, writableIterator, attributes)
      }
    }
  }
}

private[orc] object OrcTableScan {
  // This constant duplicates `OrcInputFormat.SARG_PUSHDOWN`, which is unfortunately not public.
  private[orc] val SARG_PUSHDOWN = "sarg.pushdown"
}
