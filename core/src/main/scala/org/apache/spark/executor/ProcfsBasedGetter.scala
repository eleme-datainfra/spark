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

import java.io.{BufferedReader, File, FileNotFoundException, FileReader, IOException}
import java.util.regex.Pattern

import org.apache.spark.Logging

private[spark] class ProcfsBasedGetter {
}


private[spark] object ProcfsBasedGetter extends Logging {
  val PROCFS = "/proc";
  val STAT_FILE = "stat"
  
  val PAGE_SIZE = 4096
    
  val INFO_REGEX = Pattern.compile(
    "^([0-9-]+)\\s([^\\s]+)\\s[^\\s]\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+)\\s" +
    "([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)\\s([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)" +
    "(\\s[0-9-]+){15}"
    )
    
  def getProcessRss(pid: Int): Long = {
    var ret: Long = -1L
    var in: BufferedReader = null
    var fReader: FileReader = null
    try {
      val pidDir = new File(PROCFS, pid.toString)
      fReader = new FileReader(new File(pidDir, STAT_FILE))
      in = new BufferedReader(fReader)
    }
    catch {
      case e: FileNotFoundException =>
        return ret
    }
    
    try {
      val info = in.readLine()
      val m = INFO_REGEX.matcher(info)
      if (m.find()) {
        // Set (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
        ret = (m.group(11).toLong * PAGE_SIZE)
      } else {
        logWarning(s"Unexpected: procfs stat file is not in the expected format for pid: $pid, info: $info")
      }
    }
    catch {
      case e: IOException =>
        logWarning("Error reading the stream " + in)
    } finally {
      try {
        fReader.close()
        try {
          in.close()
        } catch { case e: IOException => logWarning("Error closing the stream " + in) }
      } catch { case e: IOException => logWarning("Error closing the stream " + fReader) }
    }
    
    ret
  }
    
}
