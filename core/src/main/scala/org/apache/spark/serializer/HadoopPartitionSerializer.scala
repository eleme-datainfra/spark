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

package org.apache.spark.serializer

import java.io.{ObjectInputStream, ObjectOutputStream, ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.commons.io.IOUtils
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.SerializableHadoopPartition
import org.apache.spark.{Logging, SparkEnv, SparkException}

import scala.collection.mutable

/**
 * Custom serializer used for generic Avro records. If the user registers the schemas
 * ahead of time, then the schema's fingerprint will be sent with each message instead of the actual
 * schema, as to reduce network IO.
 * Actions like parsing or compressing schemas are computationally expensive so the serializer
 * caches all previously seen values as to reduce the amount of work needed to do.
 */
private[spark] class HadoopPartitionSerializer()
  extends KSerializer[SerializableHadoopPartition] with Logging {

  override def write(kryo: Kryo, output: KryoOutput, s: SerializableHadoopPartition): Unit = {
    logInfo("Write SerializableHadoopPartition")
    val byteOutputStream = new ByteArrayOutputStream()
    val objectOutputStream = new ObjectOutputStream(byteOutputStream)
    objectOutputStream.writeObject(s)
    objectOutputStream.flush()
    val objBytes = byteOutputStream.toByteArray
    logInfo("Write SerializableHadoopPartition " + objBytes.length)
    output.writeInt(objBytes.length)
    output.writeBytes(objBytes)
    objectOutputStream.close()
  }

  override def read(kryo: Kryo, input: KryoInput, s: Class[SerializableHadoopPartition]):
      SerializableHadoopPartition = {
    logInfo("Read SerializableHadoopPartition")
    val length = input.readInt()
    val objBytes = input.readBytes(length)
    logInfo("Read SerializableHadoopPartition " + objBytes.length)
    val byteArrayInputStream = new ByteArrayInputStream(objBytes)
    val objectInputStream = new ObjectInputStream(byteArrayInputStream)
    objectInputStream.readObject().asInstanceOf[SerializableHadoopPartition]
  }
}

private[spark] class HadoopPartitionRegister extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    val classLoader = Thread.currentThread.getContextClassLoader
    // scalastyle:off classforname
    kryo.register(Class.forName(classOf[SerializableHadoopPartition].getName, true, classLoader),
      new HadoopPartitionSerializer())
    // scalastyle:on classforname
  }
}
