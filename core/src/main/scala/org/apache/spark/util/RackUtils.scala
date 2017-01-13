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

package org.apache.spark.util

import java.net.{Inet4Address, InetAddress}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkConf

object RackUtils {
  val host2Rack = new ConcurrentHashMap[String, String]()

  def getRackForHost(sparkConf: SparkConf, hostPort: String): Option[String] = {
    val host = Utils.parseHostPort(hostPort)._1
    Option(resolve(sparkConf, host))
  }

  def resolve(sparkConf: SparkConf, hostname: String): String = {
    if (sparkConf.getBoolean("spark.rack.disabled", false)) {
      return "/default-rack"
    }
    if (host2Rack.containsKey(hostname)) {
      host2Rack.get(hostname)
    } else {
      try {
        val address = InetAddress.getByName(hostname)
        if (address.isInstanceOf[Inet4Address]) {
          val ip = address.getHostAddress
          val rack = ip.substring(0, ip.lastIndexOf(".") + 1) + "0"
          host2Rack.put(hostname, rack)
          return rack
        }
      } catch {
        case e: Exception =>
      }
      "/default-rack"
    }
  }
}
