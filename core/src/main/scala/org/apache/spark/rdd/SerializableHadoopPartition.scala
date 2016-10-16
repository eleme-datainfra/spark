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

package org.apache.spark.rdd

import java.io.{ObjectOutputStream, ObjectInputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.mapred.InputSplit
import org.apache.spark.util.Utils

class SerializableHadoopPartition(var rddIndex: Int, var splits: Array[HadoopPartition])
  extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.writeInt(rddIndex)
    out.writeObject(splits)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    rddIndex = in.readInt()
    splits = in.readObject().asInstanceOf[Array[HadoopPartition]]
  }
}

class SerializablePartition(var rddId: Int, var idx: Int, @transient var s: InputSplit)
  extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.writeInt(rddId)
    out.writeInt(idx)
    new ObjectWritable(s).write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    rddId = in.readInt()
    idx = in.readInt()
    val ow = new ObjectWritable()
    ow.setConf(new Configuration(false))
    ow.readFields(in)
    s = ow.get().asInstanceOf[InputSplit]
  }
}
