/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.instrumentation

import org.apache.spark.scheduler.{ StageInfo, SparkListenerStageCompleted, SparkListenerTaskEnd, SparkListener }
import scala.concurrent.duration._

/**
 * Spark listener that accumulates metrics in the passed-in [[ADAMMetrics]] object
 * at stage completion time.
 * @note This class relies on being run in the same process as the driver. However,
 * this is the way that Spark seems to work.
 */
class ADAMMetricsListener(val adamMetrics: ADAMMetrics) extends SparkListener {

  private val adamSparkMetrics = adamMetrics.adamSparkMetrics

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    adamSparkMetrics.mapStageIdToName(stageInfo.stageId, stageInfo.name)
    getStageDuration(stageInfo).foreach(adamSparkMetrics.recordStageDuration(stageInfo.stageId, Option(stageInfo.name), _))
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    val taskMetrics = Option(taskEnd.taskMetrics)
    val taskInfo = Option(taskEnd.taskInfo)

    implicit val taskContext = TaskContext(
      if (taskMetrics.isDefined && taskMetrics.get.hostname != null) taskMetrics.get.hostname else "unknown",
      taskEnd.stageId)

    taskMetrics.foreach(e => {
      adamSparkMetrics.executorRunTime += e.executorRunTime
      adamSparkMetrics.executorDeserializeTime += e.executorDeserializeTime
      adamSparkMetrics.resultSerializationTime += e.resultSerializationTime
    })

    taskInfo.foreach(e => {
      adamSparkMetrics.duration += e.duration
    })

  }

  private def getStageDuration(info: StageInfo): Option[Duration] = {
    // We calculate this in the same way as the Spark code calculates the task duration when it
    // logs it at completion time, so hopefully it is a good representation of the duration
    if (info.submissionTime.isDefined && info.completionTime.isDefined) {
      Some(Duration(info.completionTime.get - info.submissionTime.get, MILLISECONDS))
    } else {
      None
    }
  }

}
