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

import scala.collection.mutable.HashSet
import scala.xml.{Elem, Node, Unparsed}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{AccumulableInfo, TaskInfo}
import org.apache.spark.ui.{ToolTips, WebUIPage, UIUtils}
import org.apache.spark.ui.jobs.UIData._
import org.apache.spark.ui.scope.RDDOperationGraph
import org.apache.spark.util.{Utils, Distribution}

/** Page showing statistics and task list for a given stage */
private[ui] class StagePage(parent: StagesTab) extends WebUIPage("stage") {
  private val progressListener = parent.progressListener
  private val operationGraphListener = parent.operationGraphListener

  private val TIMELINE_LEGEND = {
    <div class="legend-area">
      <svg>
        {
          val legendPairs = List(("scheduler-delay-proportion", "Scheduler Delay"),
            ("deserialization-time-proportion", "Task Deserialization Time"),
            ("shuffle-read-time-proportion", "Shuffle Read Time"),
            ("executor-runtime-proportion", "Executor Computing Time"),
            ("shuffle-write-time-proportion", "Shuffle Write Time"),
            ("serialization-time-proportion", "Result Serialization TIme"),
            ("getting-result-time-proportion", "Getting Result Time"))

          legendPairs.zipWithIndex.map {
            case ((classAttr, name), index) =>
              <rect x={5 + (index / 3) * 210 + "px"} y={10 + (index % 3) * 15 + "px"}
                width="10px" height="10px" class={classAttr}></rect>
                <text x={25 + (index / 3) * 210 + "px"}
                  y={20 + (index % 3) * 15 + "px"}>{name}</text>
          }
        }
      </svg>
    </div>
  }

  // TODO: We should consider increasing the number of this parameter over time
  // if we find that it's okay.
  private val MAX_TIMELINE_TASKS = parent.conf.getInt("spark.ui.timeline.tasks.maximum", 1000)


  def render(request: HttpServletRequest): Seq[Node] = {
    progressListener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

      val parameterAttempt = request.getParameter("attempt")
      require(parameterAttempt != null && parameterAttempt.nonEmpty, "Missing attempt parameter")

      // If this is set, expand the dag visualization by default
      val expandDagVizParam = request.getParameter("expandDagViz")
      val expandDagViz = expandDagVizParam != null && expandDagVizParam.toBoolean

      val stageId = parameterId.toInt
      val stageAttemptId = parameterAttempt.toInt
      val stageDataOption = progressListener.stageIdToData.get((stageId, stageAttemptId))

      val stageHeader = s"Details for Stage $stageId (Attempt $stageAttemptId)"
      if (stageDataOption.isEmpty) {
        val content =
          <div id="no-info">
            <p>No information to display for Stage {stageId} (Attempt {stageAttemptId})</p>
          </div>
        return UIUtils.headerSparkPage(stageHeader, content, parent)

      }
      if (stageDataOption.get.taskData.isEmpty) {
        val content =
          <div>
            <h4>Summary Metrics</h4> No tasks have started yet
            <h4>Tasks</h4> No tasks have started yet
          </div>
        return UIUtils.headerSparkPage(stageHeader, content, parent)
      }

      val stageData = stageDataOption.get
      val tasks = stageData.taskData.values.toSeq.sortBy(_.taskInfo.launchTime)

      val numCompleted = tasks.count(_.taskInfo.finished)
      val accumulables = progressListener.stageIdToData((stageId, stageAttemptId)).accumulables
      val hasAccumulators = accumulables.size > 0

      val summary =
        <div>
          <ul class="unstyled">
            <li>
              <strong>Total Time Across All Tasks: </strong>
              {UIUtils.formatDuration(stageData.executorRunTime)}
            </li>
            {if (stageData.hasInput) {
              <li>
                <strong>Input Size / Records: </strong>
                {s"${Utils.bytesToString(stageData.inputBytes)} / ${stageData.inputRecords}"}
              </li>
            }}
            {if (stageData.hasOutput) {
              <li>
                <strong>Output: </strong>
                {s"${Utils.bytesToString(stageData.outputBytes)} / ${stageData.outputRecords}"}
              </li>
            }}
            {if (stageData.hasShuffleRead) {
              <li>
                <strong>Shuffle Read: </strong>
                {s"${Utils.bytesToString(stageData.shuffleReadTotalBytes)} / " +
                 s"${stageData.shuffleReadRecords}"}
              </li>
            }}
            {if (stageData.hasShuffleWrite) {
              <li>
                <strong>Shuffle Write: </strong>
                 {s"${Utils.bytesToString(stageData.shuffleWriteBytes)} / " +
                 s"${stageData.shuffleWriteRecords}"}
              </li>
            }}
            {if (stageData.hasBytesSpilled) {
              <li>
                <strong>Shuffle Spill (Memory): </strong>
                {Utils.bytesToString(stageData.memoryBytesSpilled)}
              </li>
              <li>
                <strong>Shuffle Spill (Disk): </strong>
                {Utils.bytesToString(stageData.diskBytesSpilled)}
              </li>
            }}
          </ul>
        </div>

      val showAdditionalMetrics =
        <div>
          <span class="expand-additional-metrics">
            <span class="expand-additional-metrics-arrow arrow-closed"></span>
            <a>Show Additional Metrics</a>
          </span>
          <div class="additional-metrics collapsed">
            <ul>
              <li>
                  <input type="checkbox" id="select-all-metrics"/>
                  <span class="additional-metric-title"><em>(De)select All</em></span>
              </li>
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.SCHEDULER_DELAY} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.SCHEDULER_DELAY}/>
                  <span class="additional-metric-title">Scheduler Delay</span>
                </span>
              </li>
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.TASK_DESERIALIZATION_TIME} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}/>
                  <span class="additional-metric-title">Task Deserialization Time</span>
                </span>
              </li>
              {if (stageData.hasShuffleRead) {
                <li>
                  <span data-toggle="tooltip"
                        title={ToolTips.SHUFFLE_READ_BLOCKED_TIME} data-placement="right">
                    <input type="checkbox" name={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}/>
                    <span class="additional-metric-title">Shuffle Read Blocked Time</span>
                  </span>
                </li>
                <li>
                  <span data-toggle="tooltip"
                        title={ToolTips.SHUFFLE_READ_REMOTE_SIZE} data-placement="right">
                    <input type="checkbox" name={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}/>
                    <span class="additional-metric-title">Shuffle Remote Reads</span>
                  </span>
                </li>
              }}
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.RESULT_SERIALIZATION_TIME} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}/>
                  <span class="additional-metric-title">Result Serialization Time</span>
                </span>
              </li>
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.GETTING_RESULT_TIME} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.GETTING_RESULT_TIME}/>
                  <span class="additional-metric-title">Getting Result Time</span>
                </span>
              </li>
            </ul>
          </div>
        </div>

      val dagViz = UIUtils.showDagVizForStage(
        stageId, operationGraphListener.getOperationGraphForStage(stageId))

      val maybeExpandDagViz: Seq[Node] =
        if (expandDagViz) {
          UIUtils.expandDagVizOnLoad(forJob = false)
        } else {
          Seq.empty
        }

      val accumulableHeaders: Seq[String] = Seq("Accumulable", "Value")
      def accumulableRow(acc: AccumulableInfo): Elem =
        <tr><td>{acc.name}</td><td>{acc.value}</td></tr>
      val accumulableTable = UIUtils.listingTable(
        accumulableHeaders,
        accumulableRow,
        accumulables.values.toSeq)

      val taskHeadersAndCssClasses: Seq[(String, String)] =
        Seq(
          ("Index", ""), ("ID", ""), ("Attempt", ""), ("Status", ""), ("Locality Level", ""),
          ("Executor ID / Host", ""), ("Launch Time", ""), ("Duration", ""),
          ("Scheduler Delay", TaskDetailsClassNames.SCHEDULER_DELAY),
          ("Task Deserialization Time", TaskDetailsClassNames.TASK_DESERIALIZATION_TIME),
          ("GC Time", ""),
          ("Result Serialization Time", TaskDetailsClassNames.RESULT_SERIALIZATION_TIME),
          ("Getting Result Time", TaskDetailsClassNames.GETTING_RESULT_TIME)) ++
        {if (hasAccumulators) Seq(("Accumulators", "")) else Nil} ++
        {if (stageData.hasInput) Seq(("Input Size / Records", "")) else Nil} ++
        {if (stageData.hasOutput) Seq(("Output Size / Records", "")) else Nil} ++
        {if (stageData.hasShuffleRead) {
          Seq(("Shuffle Read Blocked Time", TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME),
            ("Shuffle Read Size / Records", ""),
            ("Shuffle Remote Reads", TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE))
        } else {
          Nil
        }} ++
        {if (stageData.hasShuffleWrite) {
          Seq(("Write Time", ""), ("Shuffle Write Size / Records", ""))
        } else {
          Nil
        }} ++
        {if (stageData.hasBytesSpilled) {
          Seq(("Shuffle Spill (Memory)", ""), ("Shuffle Spill (Disk)", ""))
        } else {
          Nil
        }} ++
        Seq(("Errors", ""))

      val unzipped = taskHeadersAndCssClasses.unzip

      val currentTime = System.currentTimeMillis()
      val taskTable = UIUtils.listingTable(
        unzipped._1,
        taskRow(
          hasAccumulators,
          stageData.hasInput,
          stageData.hasOutput,
          stageData.hasShuffleRead,
          stageData.hasShuffleWrite,
          stageData.hasBytesSpilled,
          currentTime),
        tasks,
        headerClasses = unzipped._2)
      // Excludes tasks which failed and have incomplete metrics
      val validTasks = tasks.filter(t => t.taskInfo.status == "SUCCESS" && t.taskMetrics.isDefined)

      val summaryTable: Option[Seq[Node]] =
        if (validTasks.size == 0) {
          None
        }
        else {
          def getDistributionQuantiles(data: Seq[Double]): IndexedSeq[Double] =
            Distribution(data).get.getQuantiles()

          def getFormattedTimeQuantiles(times: Seq[Double]): Seq[Node] = {
            getDistributionQuantiles(times).map { millis =>
              <td>{UIUtils.formatDuration(millis.toLong)}</td>
            }
          }

          val deserializationTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.executorDeserializeTime.toDouble
          }
          val deserializationQuantiles =
            <td>
              <span data-toggle="tooltip" title={ToolTips.TASK_DESERIALIZATION_TIME}
                    data-placement="right">
                Task Deserialization Time
              </span>
            </td> +: getFormattedTimeQuantiles(deserializationTimes)

          val serviceTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.executorRunTime.toDouble
          }
          val serviceQuantiles = <td>Duration</td> +: getFormattedTimeQuantiles(serviceTimes)

          val gcTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.jvmGCTime.toDouble
          }
          val gcQuantiles =
            <td>
              <span data-toggle="tooltip"
                  title={ToolTips.GC_TIME} data-placement="right">GC Time
              </span>
            </td> +: getFormattedTimeQuantiles(gcTimes)

          val serializationTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.resultSerializationTime.toDouble
          }
          val serializationQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.RESULT_SERIALIZATION_TIME} data-placement="right">
                Result Serialization Time
              </span>
            </td> +: getFormattedTimeQuantiles(serializationTimes)

          val gettingResultTimes = validTasks.map { case TaskUIData(info, _, _) =>
            getGettingResultTime(info).toDouble
          }
          val gettingResultQuantiles =
            <td>
              <span data-toggle="tooltip"
                  title={ToolTips.GETTING_RESULT_TIME} data-placement="right">
                Getting Result Time
              </span>
            </td> +:
            getFormattedTimeQuantiles(gettingResultTimes)
          // The scheduler delay includes the network delay to send the task to the worker
          // machine and to send back the result (but not the time to fetch the task result,
          // if it needed to be fetched from the block manager on the worker).
          val schedulerDelays = validTasks.map { case TaskUIData(info, metrics, _) =>
            getSchedulerDelay(info, metrics.get).toDouble
          }
          val schedulerDelayTitle = <td><span data-toggle="tooltip"
            title={ToolTips.SCHEDULER_DELAY} data-placement="right">Scheduler Delay</span></td>
          val schedulerDelayQuantiles = schedulerDelayTitle +:
            getFormattedTimeQuantiles(schedulerDelays)

          def getFormattedSizeQuantiles(data: Seq[Double]): Seq[Elem] =
            getDistributionQuantiles(data).map(d => <td>{Utils.bytesToString(d.toLong)}</td>)

          def getFormattedSizeQuantilesWithRecords(data: Seq[Double], records: Seq[Double])
            : Seq[Elem] = {
            val recordDist = getDistributionQuantiles(records).iterator
            getDistributionQuantiles(data).map(d =>
              <td>{s"${Utils.bytesToString(d.toLong)} / ${recordDist.next().toLong}"}</td>
            )
          }

          val inputSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.inputMetrics.map(_.bytesRead).getOrElse(0L).toDouble
          }

          val inputRecords = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.inputMetrics.map(_.recordsRead).getOrElse(0L).toDouble
          }

          val inputQuantiles = <td>Input Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(inputSizes, inputRecords)

          val outputSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.outputMetrics.map(_.bytesWritten).getOrElse(0L).toDouble
          }

          val outputRecords = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.outputMetrics.map(_.recordsWritten).getOrElse(0L).toDouble
          }

          val outputQuantiles = <td>Output Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(outputSizes, outputRecords)

          val shuffleReadBlockedTimes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleReadMetrics.map(_.fetchWaitTime).getOrElse(0L).toDouble
          }
          val shuffleReadBlockedQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ_BLOCKED_TIME} data-placement="right">
                Shuffle Read Blocked Time
              </span>
            </td> +:
            getFormattedTimeQuantiles(shuffleReadBlockedTimes)

          val shuffleReadTotalSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleReadMetrics.map(_.totalBytesRead).getOrElse(0L).toDouble
          }
          val shuffleReadTotalRecords = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleReadMetrics.map(_.recordsRead).getOrElse(0L).toDouble
          }
          val shuffleReadTotalQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ} data-placement="right">
                Shuffle Read Size / Records
              </span>
            </td> +:
            getFormattedSizeQuantilesWithRecords(shuffleReadTotalSizes, shuffleReadTotalRecords)

          val shuffleReadRemoteSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
          }
          val shuffleReadRemoteQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ_REMOTE_SIZE} data-placement="right">
                Shuffle Remote Reads
              </span>
            </td> +:
            getFormattedSizeQuantiles(shuffleReadRemoteSizes)

          val shuffleWriteSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
          }

          val shuffleWriteRecords = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.shuffleWriteMetrics.map(_.shuffleRecordsWritten).getOrElse(0L).toDouble
          }

          val shuffleWriteQuantiles = <td>Shuffle Write Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(shuffleWriteSizes, shuffleWriteRecords)

          val memoryBytesSpilledSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.memoryBytesSpilled.toDouble
          }
          val memoryBytesSpilledQuantiles = <td>Shuffle spill (memory)</td> +:
            getFormattedSizeQuantiles(memoryBytesSpilledSizes)

          val diskBytesSpilledSizes = validTasks.map { case TaskUIData(_, metrics, _) =>
            metrics.get.diskBytesSpilled.toDouble
          }
          val diskBytesSpilledQuantiles = <td>Shuffle spill (disk)</td> +:
            getFormattedSizeQuantiles(diskBytesSpilledSizes)

          val listings: Seq[Seq[Node]] = Seq(
            <tr>{serviceQuantiles}</tr>,
            <tr class={TaskDetailsClassNames.SCHEDULER_DELAY}>{schedulerDelayQuantiles}</tr>,
            <tr class={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}>
              {deserializationQuantiles}
            </tr>
            <tr>{gcQuantiles}</tr>,
            <tr class={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}>
              {serializationQuantiles}
            </tr>,
            <tr class={TaskDetailsClassNames.GETTING_RESULT_TIME}>{gettingResultQuantiles}</tr>,
            if (stageData.hasInput) <tr>{inputQuantiles}</tr> else Nil,
            if (stageData.hasOutput) <tr>{outputQuantiles}</tr> else Nil,
            if (stageData.hasShuffleRead) {
              <tr class={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}>
                {shuffleReadBlockedQuantiles}
              </tr>
              <tr>{shuffleReadTotalQuantiles}</tr>
              <tr class={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}>
                {shuffleReadRemoteQuantiles}
              </tr>
            } else {
              Nil
            },
            if (stageData.hasShuffleWrite) <tr>{shuffleWriteQuantiles}</tr> else Nil,
            if (stageData.hasBytesSpilled) <tr>{memoryBytesSpilledQuantiles}</tr> else Nil,
            if (stageData.hasBytesSpilled) <tr>{diskBytesSpilledQuantiles}</tr> else Nil)

          val quantileHeaders = Seq("Metric", "Min", "25th percentile",
            "Median", "75th percentile", "Max")
          // The summary table does not use CSS to stripe rows, which doesn't work with hidden
          // rows (instead, JavaScript in table.js is used to stripe the non-hidden rows).
          Some(UIUtils.listingTable(
            quantileHeaders,
            identity[Seq[Node]],
            listings,
            fixedWidth = true,
            id = Some("task-summary-table"),
            stripeRowsWithCss = false))
        }

      val executorTable = new ExecutorTable(stageId, stageAttemptId, parent)

      val maybeAccumulableTable: Seq[Node] =
        if (accumulables.size > 0) { <h4>Accumulators</h4> ++ accumulableTable } else Seq()

      val content =
        summary ++
        dagViz ++
        maybeExpandDagViz ++
        showAdditionalMetrics ++
        makeTimeline(stageData.taskData.values.toSeq, currentTime) ++
        <h4>Summary Metrics for {numCompleted} Completed Tasks</h4> ++
        <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
        <h4>Aggregated Metrics by Executor</h4> ++ executorTable.toNodeSeq ++
        maybeAccumulableTable ++
        <h4>Tasks</h4> ++ taskTable
      UIUtils.headerSparkPage(stageHeader, content, parent, showVisualization = true)
    }
  }

  def makeTimeline(tasks: Seq[TaskUIData], currentTime: Long): Seq[Node] = {
    val executorsSet = new HashSet[(String, String)]
    var minLaunchTime = Long.MaxValue
    var maxFinishTime = Long.MinValue

    val executorsArrayStr =
      tasks.sortBy(-_.taskInfo.launchTime).take(MAX_TIMELINE_TASKS).map { taskUIData =>
        val taskInfo = taskUIData.taskInfo
        val executorId = taskInfo.executorId
        val host = taskInfo.host
        executorsSet += ((executorId, host))

        val launchTime = taskInfo.launchTime
        val finishTime = if (!taskInfo.running) taskInfo.finishTime else currentTime
        val totalExecutionTime = finishTime - launchTime
        minLaunchTime = launchTime.min(minLaunchTime)
        maxFinishTime = finishTime.max(maxFinishTime)

        def toProportion(time: Long) = time.toDouble / totalExecutionTime * 100

        val metricsOpt = taskUIData.taskMetrics
        val shuffleReadTime =
          metricsOpt.flatMap(_.shuffleReadMetrics.map(_.fetchWaitTime)).getOrElse(0L)
        val shuffleReadTimeProportion = toProportion(shuffleReadTime)
        val shuffleWriteTime =
          (metricsOpt.flatMap(_.shuffleWriteMetrics
            .map(_.shuffleWriteTime)).getOrElse(0L) / 1e6).toLong
        val shuffleWriteTimeProportion = toProportion(shuffleWriteTime)
        val executorComputingTime = metricsOpt.map(_.executorRunTime).getOrElse(0L) -
          shuffleReadTime - shuffleWriteTime
        val executorComputingTimeProportion = toProportion(executorComputingTime)
        val serializationTime = metricsOpt.map(_.resultSerializationTime).getOrElse(0L)
        val serializationTimeProportion = toProportion(serializationTime)
        val deserializationTime = metricsOpt.map(_.executorDeserializeTime).getOrElse(0L)
        val deserializationTimeProportion = toProportion(deserializationTime)
        val gettingResultTime = getGettingResultTime(taskUIData.taskInfo)
        val gettingResultTimeProportion = toProportion(gettingResultTime)
        val schedulerDelay = totalExecutionTime -
          (executorComputingTime + shuffleReadTime + shuffleWriteTime +
            serializationTime + deserializationTime + gettingResultTime)
        val schedulerDelayProportion =
          (100 - executorComputingTimeProportion - shuffleReadTimeProportion -
            shuffleWriteTimeProportion - serializationTimeProportion -
            deserializationTimeProportion - gettingResultTimeProportion)

        val schedulerDelayProportionPos = 0
        val deserializationTimeProportionPos =
          schedulerDelayProportionPos + schedulerDelayProportion
        val shuffleReadTimeProportionPos =
          deserializationTimeProportionPos + deserializationTimeProportion
        val executorRuntimeProportionPos =
          shuffleReadTimeProportionPos + shuffleReadTimeProportion
        val shuffleWriteTimeProportionPos =
          executorRuntimeProportionPos + executorComputingTimeProportion
        val serializationTimeProportionPos =
          shuffleWriteTimeProportionPos + shuffleWriteTimeProportion
        val gettingResultTimeProportionPos =
          serializationTimeProportionPos + serializationTimeProportion

        val index = taskInfo.index
        val attempt = taskInfo.attempt
        val timelineObject =
          s"""
             {
               'className': 'task task-assignment-timeline-object',
               'group': '$executorId',
               'content': '<div class="task-assignment-timeline-content"' +
                 'data-toggle="tooltip" data-placement="top"' +
                 'data-html="true" data-container="body"' +
                 'data-title="${s"Task " + index + " (attempt " + attempt + ")"}<br>' +
                 'Status: ${taskInfo.status}<br>' +
                 'Launch Time: ${UIUtils.formatDate(new Date(launchTime))}' +
                 '${
                     if (!taskInfo.running) {
                       s"""<br>Finish Time: ${UIUtils.formatDate(new Date(finishTime))}"""
                     } else {
                        ""
                      }
                   }' +
                 '<br>Scheduler Delay: $schedulerDelay ms' +
                 '<br>Task Deserialization Time: ${UIUtils.formatDuration(deserializationTime)}' +
                 '<br>Shuffle Read Time: ${UIUtils.formatDuration(shuffleReadTime)}' +
                 '<br>Executor Computing Time: ${UIUtils.formatDuration(executorComputingTime)}' +
                 '<br>Shuffle Write Time: ${UIUtils.formatDuration(shuffleWriteTime)}' +
                 '<br>Result Serialization Time: ${UIUtils.formatDuration(serializationTime)}' +
                 '<br>Getting Result Time: ${UIUtils.formatDuration(gettingResultTime)}">' +
                 '<svg class="task-assignment-timeline-duration-bar">' +
                 '<rect class="scheduler-delay-proportion" ' +
                   'x="$schedulerDelayProportionPos%" y="0px" height="26px"' +
                   'width="$schedulerDelayProportion%""></rect>' +
                 '<rect class="deserialization-time-proportion" '+
                   'x="$deserializationTimeProportionPos%" y="0px" height="26px"' +
                   'width="$deserializationTimeProportion%"></rect>' +
                 '<rect class="shuffle-read-time-proportion" ' +
                   'x="$shuffleReadTimeProportionPos%" y="0px" height="26px"' +
                   'width="$shuffleReadTimeProportion%"></rect>' +
                 '<rect class="executor-runtime-proportion" ' +
                   'x="$executorRuntimeProportionPos%" y="0px" height="26px"' +
                   'width="$executorComputingTimeProportion%"></rect>' +
                 '<rect class="shuffle-write-time-proportion" ' +
                   'x="$shuffleWriteTimeProportionPos%" y="0px" height="26px"' +
                   'width="$shuffleWriteTimeProportion%"></rect>' +
                 '<rect class="serialization-time-proportion" ' +
                   'x="$serializationTimeProportionPos%" y="0px" height="26px"' +
                   'width="$serializationTimeProportion%"></rect>' +
                 '<rect class="getting-result-time-proportion" ' +
                   'x="$gettingResultTimeProportionPos%" y="0px" height="26px"' +
                   'width="$gettingResultTimeProportion%"></rect></svg>',
               'start': new Date($launchTime),
               'end': new Date($finishTime)
             }
           """
        timelineObject
      }.mkString("[", ",", "]")

    val groupArrayStr = executorsSet.map {
      case (executorId, host) =>
        s"""
            {
              'id': '$executorId',
              'content': '$executorId / $host',
            }
          """
    }.mkString("[", ",", "]")

    <span class="expand-task-assignment-timeline">
      <span class="expand-task-assignment-timeline-arrow arrow-closed"></span>
      <a>Event Timeline</a>
    </span> ++
    <div id="task-assignment-timeline" class="collapsed">
      {
        if (MAX_TIMELINE_TASKS < tasks.size) {
          <strong>
            This stage has more than the maximum number of tasks that can be shown in the
            visualization! Only the most recent {MAX_TIMELINE_TASKS} tasks
            (of {tasks.size} total) are shown.
          </strong>
        } else {
          Seq.empty
        }
      }
      <div class="control-panel">
        <div id="task-assignment-timeline-zoom-lock">
          <input type="checkbox"></input>
          <span>Enable zooming</span>
        </div>
      </div>
      {TIMELINE_LEGEND}
    </div> ++
    <script type="text/javascript">
      {Unparsed(s"drawTaskAssignmentTimeline(" +
      s"$groupArrayStr, $executorsArrayStr, $minLaunchTime, $maxFinishTime)")}
    </script>
  }

  def taskRow(
      hasAccumulators: Boolean,
      hasInput: Boolean,
      hasOutput: Boolean,
      hasShuffleRead: Boolean,
      hasShuffleWrite: Boolean,
      hasBytesSpilled: Boolean,
      currentTime: Long)(taskData: TaskUIData): Seq[Node] = {
    taskData match { case TaskUIData(info, metrics, errorMessage) =>
      val duration = if (info.status == "RUNNING") info.timeRunning(currentTime)
        else metrics.map(_.executorRunTime).getOrElse(1L)
      val formatDuration = if (info.status == "RUNNING") UIUtils.formatDuration(duration)
        else metrics.map(m => UIUtils.formatDuration(m.executorRunTime)).getOrElse("")
      val schedulerDelay = metrics.map(getSchedulerDelay(info, _)).getOrElse(0L)
      val gcTime = metrics.map(_.jvmGCTime).getOrElse(0L)
      val taskDeserializationTime = metrics.map(_.executorDeserializeTime).getOrElse(0L)
      val serializationTime = metrics.map(_.resultSerializationTime).getOrElse(0L)
      val gettingResultTime = getGettingResultTime(info)

      val maybeAccumulators = info.accumulables
      val accumulatorsReadable = maybeAccumulators.map{acc => s"${acc.name}: ${acc.update.get}"}

      val maybeInput = metrics.flatMap(_.inputMetrics)
      val inputSortable = maybeInput.map(_.bytesRead.toString).getOrElse("")
      val inputReadable = maybeInput
        .map(m => s"${Utils.bytesToString(m.bytesRead)} (${m.readMethod.toString.toLowerCase()})")
        .getOrElse("")
      val inputRecords = maybeInput.map(_.recordsRead.toString).getOrElse("")

      val maybeOutput = metrics.flatMap(_.outputMetrics)
      val outputSortable = maybeOutput.map(_.bytesWritten.toString).getOrElse("")
      val outputReadable = maybeOutput
        .map(m => s"${Utils.bytesToString(m.bytesWritten)}")
        .getOrElse("")
      val outputRecords = maybeOutput.map(_.recordsWritten.toString).getOrElse("")

      val maybeShuffleRead = metrics.flatMap(_.shuffleReadMetrics)
      val shuffleReadBlockedTimeSortable = maybeShuffleRead
        .map(_.fetchWaitTime.toString).getOrElse("")
      val shuffleReadBlockedTimeReadable =
        maybeShuffleRead.map(ms => UIUtils.formatDuration(ms.fetchWaitTime)).getOrElse("")

      val totalShuffleBytes = maybeShuffleRead.map(_.totalBytesRead)
      val shuffleReadSortable = totalShuffleBytes.map(_.toString).getOrElse("")
      val shuffleReadReadable = totalShuffleBytes.map(Utils.bytesToString).getOrElse("")
      val shuffleReadRecords = maybeShuffleRead.map(_.recordsRead.toString).getOrElse("")

      val remoteShuffleBytes = maybeShuffleRead.map(_.remoteBytesRead)
      val shuffleReadRemoteSortable = remoteShuffleBytes.map(_.toString).getOrElse("")
      val shuffleReadRemoteReadable = remoteShuffleBytes.map(Utils.bytesToString).getOrElse("")

      val maybeShuffleWrite = metrics.flatMap(_.shuffleWriteMetrics)
      val shuffleWriteSortable = maybeShuffleWrite.map(_.shuffleBytesWritten.toString).getOrElse("")
      val shuffleWriteReadable = maybeShuffleWrite
        .map(m => s"${Utils.bytesToString(m.shuffleBytesWritten)}").getOrElse("")
      val shuffleWriteRecords = maybeShuffleWrite
        .map(_.shuffleRecordsWritten.toString).getOrElse("")

      val maybeWriteTime = metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleWriteTime)
      val writeTimeSortable = maybeWriteTime.map(_.toString).getOrElse("")
      val writeTimeReadable = maybeWriteTime.map(t => t / (1000 * 1000)).map { ms =>
        if (ms == 0) "" else UIUtils.formatDuration(ms)
      }.getOrElse("")

      val maybeMemoryBytesSpilled = metrics.map(_.memoryBytesSpilled)
      val memoryBytesSpilledSortable = maybeMemoryBytesSpilled.map(_.toString).getOrElse("")
      val memoryBytesSpilledReadable =
        maybeMemoryBytesSpilled.map(Utils.bytesToString).getOrElse("")

      val maybeDiskBytesSpilled = metrics.map(_.diskBytesSpilled)
      val diskBytesSpilledSortable = maybeDiskBytesSpilled.map(_.toString).getOrElse("")
      val diskBytesSpilledReadable = maybeDiskBytesSpilled.map(Utils.bytesToString).getOrElse("")

      <tr>
        <td>{info.index}</td>
        <td>{info.taskId}</td>
        <td sorttable_customkey={info.attempt.toString}>{
          if (info.speculative) s"${info.attempt} (speculative)" else info.attempt.toString
        }</td>
        <td>{info.status}</td>
        <td>{info.taskLocality}</td>
        <td>{info.executorId} / {info.host}</td>
        <td>{UIUtils.formatDate(new Date(info.launchTime))}</td>
        <td sorttable_customkey={duration.toString}>
          {formatDuration}
        </td>
        <td sorttable_customkey={schedulerDelay.toString}
            class={TaskDetailsClassNames.SCHEDULER_DELAY}>
          {UIUtils.formatDuration(schedulerDelay.toLong)}
        </td>
        <td sorttable_customkey={taskDeserializationTime.toString}
            class={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}>
          {UIUtils.formatDuration(taskDeserializationTime.toLong)}
        </td>
        <td sorttable_customkey={gcTime.toString}>
          {if (gcTime > 0) UIUtils.formatDuration(gcTime) else ""}
        </td>
        <td sorttable_customkey={serializationTime.toString}
            class={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}>
          {UIUtils.formatDuration(serializationTime)}
        </td>
        <td sorttable_customkey={gettingResultTime.toString}
            class={TaskDetailsClassNames.GETTING_RESULT_TIME}>
          {UIUtils.formatDuration(gettingResultTime)}
        </td>
        {if (hasAccumulators) {
          <td>
            {Unparsed(accumulatorsReadable.mkString("<br/>"))}
          </td>
        }}
        {if (hasInput) {
          <td sorttable_customkey={inputSortable}>
            {s"$inputReadable / $inputRecords"}
          </td>
        }}
        {if (hasOutput) {
          <td sorttable_customkey={outputSortable}>
            {s"$outputReadable / $outputRecords"}
          </td>
        }}
        {if (hasShuffleRead) {
           <td sorttable_customkey={shuffleReadBlockedTimeSortable}
             class={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}>
             {shuffleReadBlockedTimeReadable}
           </td>
           <td sorttable_customkey={shuffleReadSortable}>
             {s"$shuffleReadReadable / $shuffleReadRecords"}
           </td>
           <td sorttable_customkey={shuffleReadRemoteSortable}
               class={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}>
             {shuffleReadRemoteReadable}
           </td>
        }}
        {if (hasShuffleWrite) {
           <td sorttable_customkey={writeTimeSortable}>
             {writeTimeReadable}
           </td>
           <td sorttable_customkey={shuffleWriteSortable}>
             {s"$shuffleWriteReadable / $shuffleWriteRecords"}
           </td>
        }}
        {if (hasBytesSpilled) {
          <td sorttable_customkey={memoryBytesSpilledSortable}>
            {memoryBytesSpilledReadable}
          </td>
          <td sorttable_customkey={diskBytesSpilledSortable}>
            {diskBytesSpilledReadable}
          </td>
        }}
        {errorMessageCell(errorMessage)}
      </tr>
    }
  }

  private def errorMessageCell(errorMessage: Option[String]): Seq[Node] = {
    val error = errorMessage.getOrElse("")
    val isMultiline = error.indexOf('\n') >= 0
    // Display the first line by default
    val errorSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        error.substring(0, error.indexOf('\n'))
      } else {
        error
      })
    val details = if (isMultiline) {
      // scalastyle:off
      <span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stacktrace-details collapsed">
          <pre>{error}</pre>
        </div>
      // scalastyle:on
    } else {
      ""
    }
    <td>{errorSummary}{details}</td>
  }

  private def getGettingResultTime(info: TaskInfo): Long = {
    if (info.gettingResultTime > 0) {
      if (info.finishTime > 0) {
        info.finishTime - info.gettingResultTime
      } else {
        // The task is still fetching the result.
        System.currentTimeMillis - info.gettingResultTime
      }
    } else {
      0L
    }
  }

  private def getSchedulerDelay(info: TaskInfo, metrics: TaskMetrics): Long = {
    val totalExecutionTime =
      if (info.gettingResult) {
        info.gettingResultTime - info.launchTime
      } else if (info.finished) {
        info.finishTime - info.launchTime
      } else {
        0
      }
    val executorOverhead = (metrics.executorDeserializeTime +
      metrics.resultSerializationTime)
    math.max(
      0,
      totalExecutionTime - metrics.executorRunTime - executorOverhead - getGettingResultTime(info))
  }
}
