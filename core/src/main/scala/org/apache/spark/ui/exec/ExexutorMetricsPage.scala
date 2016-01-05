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

package org.apache.spark.ui.exec

import java.net.URLDecoder
import javax.servlet.http.HttpServletRequest
import org.apache.spark.ui.{UIUtils, WebUIPage}
import scala.xml.Node

private[ui] class ExexutorMetricsPage(parent: ExecutorsTab) extends WebUIPage("executorMetrics") {
  private val sc = parent.sc

  def render(request: HttpServletRequest): Seq[Node] = {
    val executorId = Option(request.getParameter("executorId")).map {
      executorId =>
        // Due to YARN-2844, "<driver>" in the url will be encoded to "%25253Cdriver%25253E" when
        // running in yarn-cluster mode. `request.getParameter("executorId")` will return
        // "%253Cdriver%253E". Therefore we need to decode it until we get the real id.
        var id = executorId
        var decodedId = URLDecoder.decode(id, "UTF-8")
        while (id != decodedId) {
          id = decodedId
          decodedId = URLDecoder.decode(id, "UTF-8")
        }
        id
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing executorId parameter")
    }

    val metrics = sc.get.getExecutorMetrics(executorId)
    val fileSystemInformationTable = UIUtils.listingTable(
      propertyHeader, propertyRow, metrics.get("filesystem"), fixedWidth = true)
    val memoryInformationTable = UIUtils.listingTable(
      propertyHeader, propertyRow, metrics.get("memory"), fixedWidth = true)

    val content =
      <span>
        <h4>File System</h4> {fileSystemInformationTable}
        <h4>Memory</h4> {memoryInformationTable}
      </span>

    UIUtils.headerSparkPage(s"Executor metrics for executor $executorId", content, parent)
  }

  private def propertyHeader = Seq("Name", "Value")
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
}
