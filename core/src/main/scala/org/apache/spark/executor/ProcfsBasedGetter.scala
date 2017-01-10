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
package org.apache.spark.executor

import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree

import org.apache.spark.internal.Logging

private[spark] class ProcfsBasedGetter(pid: Int) extends Logging {
  private val processTree = new ProcfsBasedProcessTree(pid.toString)

  def getCpuUsagePercent(): Float = {
    processTree.updateProcessTree()
    val ret = processTree.getCpuUsagePercent()
    logDebug(s"Executor CpuUsagePercent:$ret")
    ret
  }

  def getProcessRssSize(): Long = {
    processTree.updateProcessTree()
    val ret = processTree.getRssMemorySize()
    logDebug(s"Executor RssMemorySize: $ret")
    ret
  }
}