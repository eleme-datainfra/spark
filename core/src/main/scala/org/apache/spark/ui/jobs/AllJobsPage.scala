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

package org.apache.spark.ui.jobs

import java.util.Date
import javax.servlet.http.HttpServletRequest

import org.apache.spark.scheduler.{SparkListenerExecutorRemoved, SparkListenerExecutorAdded, SparkListenerEvent}

import scala.collection.mutable.ListBuffer
import scala.xml._

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.JobExecutionStatus
import org.apache.spark.ui.jobs.UIData.JobUIData
import org.apache.spark.ui.{ToolTips, UIUtils, WebUIPage}

/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPage(parent: JobsTab) extends WebUIPage("") {
  private val JOBS_LEGEND =
    <div class="legend-area"><svg width="150px" height="85px">
      <rect class="succeeded-job-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Succeeded</text>
      <rect class="failed-job-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Failed</text>
      <rect class="running-job-legend"
        x="5px" y="55px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="67px">Running</text>
    </svg></div>.toString.filter(_ != '\n')

  private val EXECUTORS_LEGEND =
    <div class="legend-area"><svg width="150px" height="55px">
      <rect class="executor-added-legend"
        x="5px" y="5px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="17px">Added</text>
      <rect class="executor-removed-legend"
        x="5px" y="30px" width="20px" height="15px" rx="2px" ry="2px"></rect>
      <text x="35px" y="42px">Removed</text>
    </svg></div>.toString.filter(_ != '\n')

  private def getLastStageNameAndDescription(job: JobUIData): (String, String) = {
    val lastStageInfo = Option(job.stageIds)
      .filter(_.nonEmpty)
      .flatMap { ids => parent.jobProgresslistener.stageIdToInfo.get(ids.max)}
    val lastStageData = lastStageInfo.flatMap { s =>
      parent.jobProgresslistener.stageIdToData.get((s.stageId, s.attemptId))
    }
    val name = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
    val description = lastStageData.flatMap(_.description).getOrElse("")
    (name, description)
  }

  private def makeJobEvent(jobUIDatas: Seq[JobUIData]): Seq[String] = {
    jobUIDatas.filter { jobUIData =>
      jobUIData.status != JobExecutionStatus.UNKNOWN && jobUIData.submissionTime.isDefined
    }.map { jobUIData =>
      val jobId = jobUIData.jobId
      val status = jobUIData.status
      val (jobName, jobDescription) = getLastStageNameAndDescription(jobUIData)
      val displayJobDescription = if (jobDescription.isEmpty) jobName else jobDescription
      val submissionTime = jobUIData.submissionTime.get
      val completionTimeOpt = jobUIData.completionTime
      val completionTime = completionTimeOpt.getOrElse(System.currentTimeMillis())
      val classNameByStatus = status match {
        case JobExecutionStatus.SUCCEEDED => "succeeded"
        case JobExecutionStatus.FAILED => "failed"
        case JobExecutionStatus.RUNNING => "running"
        case JobExecutionStatus.UNKNOWN => "unknown"
      }

      // The timeline library treats contents as HTML, so we have to escape them. We need to add
      // extra layers of escaping in order to embed this in a Javascript string literal.
      val escapedDesc = Utility.escape(displayJobDescription)
      val jsEscapedDesc = StringEscapeUtils.escapeEcmaScript(escapedDesc)
      val jobEventJsonAsStr =
        s"""
           |{
           |  'className': 'job application-timeline-object ${classNameByStatus}',
           |  'group': 'jobs',
           |  'start': new Date(${submissionTime}),
           |  'end': new Date(${completionTime}),
           |  'content': '<div class="application-timeline-content"' +
           |     'data-html="true" data-placement="top" data-toggle="tooltip"' +
           |     'data-title="${jsEscapedDesc} (Job ${jobId})<br>' +
           |     'Status: ${status}<br>' +
           |     'Submitted: ${UIUtils.formatDate(new Date(submissionTime))}' +
           |     '${
                     if (status != JobExecutionStatus.RUNNING) {
                       s"""<br>Completed: ${UIUtils.formatDate(new Date(completionTime))}"""
                     } else {
                       ""
                     }
                  }">' +
           |    '${jsEscapedDesc} (Job ${jobId})</div>'
           |}
         """.stripMargin
      jobEventJsonAsStr
    }
  }

  private def makeExecutorEvent(executorUIDatas: Seq[SparkListenerEvent]):
      Seq[String] = {
    val events = ListBuffer[String]()
    executorUIDatas.foreach {
      case a: SparkListenerExecutorAdded =>
        val addedEvent =
          s"""
             |{
             |  'className': 'executor added',
             |  'group': 'executors',
             |  'start': new Date(${a.time}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${a.executorId}<br>' +
             |    'Added at ${UIUtils.formatDate(new Date(a.time))}"' +
             |    'data-html="true">Executor ${a.executorId} added</div>'
             |}
           """.stripMargin
        events += addedEvent
      case e: SparkListenerExecutorRemoved =>
        val removedEvent =
          s"""
             |{
             |  'className': 'executor removed',
             |  'group': 'executors',
             |  'start': new Date(${e.time}),
             |  'content': '<div class="executor-event-content"' +
             |    'data-toggle="tooltip" data-placement="bottom"' +
             |    'data-title="Executor ${e.executorId}<br>' +
             |    'Removed at ${UIUtils.formatDate(new Date(e.time))}' +
             |    '${
                      if (e.reason != null) {
                        s"""<br>Reason: ${e.reason.replace("\n", " ")}"""
                      } else {
                        ""
                      }
                   }"' +
             |    'data-html="true">Executor ${e.executorId} removed</div>'
             |}
           """.stripMargin
        events += removedEvent

    }
    events.toSeq
  }

  private def makeTimeline(
      jobs: Seq[JobUIData],
      executors: Seq[SparkListenerEvent],
      startTime: Long): Seq[Node] = {

    val jobEventJsonAsStrSeq = makeJobEvent(jobs)
    val executorEventJsonAsStrSeq = makeExecutorEvent(executors)

    val groupJsonArrayAsStr =
      s"""
          |[
          |  {
          |    'id': 'executors',
          |    'content': '<div>Executors</div>${EXECUTORS_LEGEND}',
          |  },
          |  {
          |    'id': 'jobs',
          |    'content': '<div>Jobs</div>${JOBS_LEGEND}',
          |  }
          |]
        """.stripMargin

    val eventArrayAsStr =
      (jobEventJsonAsStrSeq ++ executorEventJsonAsStrSeq).mkString("[", ",", "]")

    <span class="expand-application-timeline">
      <span class="expand-application-timeline-arrow arrow-closed"></span>
      <a data-toggle="tooltip" title={ToolTips.JOB_TIMELINE} data-placement="right">
        Event Timeline
      </a>
    </span> ++
    <div id="application-timeline" class="collapsed">
      <div class="control-panel">
        <div id="application-timeline-zoom-lock">
          <input type="checkbox"></input>
          <span>Enable zooming</span>
        </div>
      </div>
    </div> ++
    <script type="text/javascript">
      {Unparsed(s"drawApplicationTimeline(${groupJsonArrayAsStr}," +
      s"${eventArrayAsStr}, ${startTime});")}
    </script>
  }

  private def jobsTable(jobs: Seq[JobUIData]): Seq[Node] = {
    val someJobHasJobGroup = jobs.exists(_.jobGroup.isDefined)

    val columns: Seq[Node] = {
      <th>{if (someJobHasJobGroup) "Job Id (Job Group)" else "Job Id"}</th>
      <th>Description</th>
      <th>Submitted</th>
      <th>Duration</th>
      <th class="sorttable_nosort">Stages: Succeeded/Total</th>
      <th class="sorttable_nosort">Tasks (for all stages): Succeeded/Total</th>
    }

    def makeRow(job: JobUIData): Seq[Node] = {
      val (lastStageName, lastStageDescription) = getLastStageNameAndDescription(job)
      val duration: Option[Long] = {
        job.submissionTime.map { start =>
          val end = job.completionTime.getOrElse(System.currentTimeMillis())
          end - start
        }
      }
      val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
      val formattedSubmissionTime = job.submissionTime.map(UIUtils.formatDate).getOrElse("Unknown")
      val basePathUri = UIUtils.prependBaseUri(parent.basePath)
      val jobDescription = UIUtils.makeDescription(lastStageDescription, basePathUri)

      val detailUrl = "%s/jobs/job?id=%s".format(basePathUri, job.jobId)
      <tr id={"job-" + job.jobId}>
        <td sorttable_customkey={job.jobId.toString}>
          {job.jobId} {job.jobGroup.map(id => s"($id)").getOrElse("")}
        </td>
        <td>
          {jobDescription}
          <a href={detailUrl} class="name-link">{lastStageName}</a>
        </td>
        <td sorttable_customkey={job.submissionTime.getOrElse(-1).toString}>
          {formattedSubmissionTime}
        </td>
        <td sorttable_customkey={duration.getOrElse(-1).toString}>{formattedDuration}</td>
        <td class="stage-progress-cell">
          {job.completedStageIndices.size}/{job.stageIds.size - job.numSkippedStages}
          {if (job.numFailedStages > 0) s"(${job.numFailedStages} failed)"}
          {if (job.numSkippedStages > 0) s"(${job.numSkippedStages} skipped)"}
        </td>
        <td class="progress-cell">
          {UIUtils.makeProgressBar(started = job.numActiveTasks, completed = job.numCompletedTasks,
           failed = job.numFailedTasks, skipped = job.numSkippedTasks,
           total = job.numTasks - job.numSkippedTasks)}
        </td>
      </tr>
    }

    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>{columns}</thead>
      <tbody>
        {jobs.map(makeRow)}
      </tbody>
    </table>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val listener = parent.jobProgresslistener
    listener.synchronized {
      val startTime = listener.startTime
      val endTime = listener.endTime
      val activeJobs = listener.activeJobs.values.toSeq
      val completedJobs = listener.completedJobs.reverse.toSeq
      val failedJobs = listener.failedJobs.reverse.toSeq

      val activeJobsTable =
        jobsTable(activeJobs.sortBy(_.submissionTime.getOrElse(-1L)).reverse)
      val completedJobsTable =
        jobsTable(completedJobs.sortBy(_.completionTime.getOrElse(-1L)).reverse)
      val failedJobsTable =
        jobsTable(failedJobs.sortBy(_.completionTime.getOrElse(-1L)).reverse)

      val shouldShowActiveJobs = activeJobs.nonEmpty
      val shouldShowCompletedJobs = completedJobs.nonEmpty
      val shouldShowFailedJobs = failedJobs.nonEmpty

      val completedJobNumStr = if (completedJobs.size == listener.numCompletedJobs) {
        s"${completedJobs.size}"
      } else {
        s"${listener.numCompletedJobs}, only showing ${completedJobs.size}"
      }

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            <li>
              <strong>Total Uptime:</strong>
              {
                if (endTime < 0 && parent.sc.isDefined) {
                  UIUtils.formatDuration(System.currentTimeMillis() - startTime)
                } else if (endTime > 0) {
                  UIUtils.formatDuration(endTime - startTime)
                }
              }
            </li>
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
            </li>
            {
              if (shouldShowActiveJobs) {
                <li>
                  <a href="#active"><strong>Active Jobs:</strong></a>
                  {activeJobs.size}
                </li>
              }
            }
            {
              if (shouldShowCompletedJobs) {
                <li id="completed-summary">
                  <a href="#completed"><strong>Completed Jobs:</strong></a>
                  {completedJobNumStr}
                </li>
              }
            }
            {
              if (shouldShowFailedJobs) {
                <li>
                  <a href="#failed"><strong>Failed Jobs:</strong></a>
                  {listener.numFailedJobs}
                </li>
              }
            }
          </ul>
        </div>

      var content = summary
      val executorListener = parent.executorListener
      content ++= makeTimeline(activeJobs ++ completedJobs ++ failedJobs,
          executorListener.executorEvents, startTime)

      if (shouldShowActiveJobs) {
        content ++= <h4 id="active">Active Jobs ({activeJobs.size})</h4> ++
          activeJobsTable
      }
      if (shouldShowCompletedJobs) {
        content ++= <h4 id="completed">Completed Jobs ({completedJobNumStr})</h4> ++
          completedJobsTable
      }
      if (shouldShowFailedJobs) {
        content ++= <h4 id ="failed">Failed Jobs ({failedJobs.size})</h4> ++
          failedJobsTable
      }

      val helpText = """A job is triggered by an action, like count() or saveAsTextFile().""" +
        " Click on a job to see information about the stages of tasks inside it."

      UIUtils.headerSparkPage("Spark Jobs", content, parent, helpText = Some(helpText))
    }
  }
}
