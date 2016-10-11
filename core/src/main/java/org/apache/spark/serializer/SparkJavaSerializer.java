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

package org.apache.spark.serializer;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.SparkConf;
import org.apache.spark.annotation.Private;
import scala.reflect.ClassTag;


@Private
public class SparkJavaSerializer<T extends Object> extends Serializer {

    SparkConf conf;
    SerializerInstance serializer;
    SerializationStream serStream;
    DeserializationStream deserStream;
    ClassTag<T> OBJECT_CLASS_TAG = null;

    public SparkJavaSerializer(SparkConf conf, ClassTag<T> classTag) {
        this.conf = conf;
        serializer = new JavaSerializer(conf).newInstance();
        OBJECT_CLASS_TAG = classTag;
    }

    @Override
    public void write(Kryo kryo, Output output, Object object) {
        serStream = serializer.serializeStream(output.getOutputStream());
        try {
            serStream.writeObject((T)object, OBJECT_CLASS_TAG);
            serStream.flush();
        } catch (Exception ex) {
            throw new KryoException("Error during Java serialization.", ex);
        } finally {
            if (serStream != null) {
                serStream.close();
            }
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class type) {
        deserStream = serializer.deserializeStream(input.getInputStream());
        try {
            return deserStream.readObject(OBJECT_CLASS_TAG);
        } catch (Exception ex) {
            throw new KryoException("Error during Java deserialization.", ex);
        } finally {
            if (deserStream != null) {
                deserStream.close();
            }
        }
    }
}
