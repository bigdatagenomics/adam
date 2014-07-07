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

import scala.collection.mutable
import com.netflix.servo.monitor.MonitorConfig
import java.io.PrintStream
import scala.collection.mutable.ArrayBuffer
import com.netflix.servo.tag.Tags.newTag
import org.bdgenomics.adam.instrumentation.ServoTimer._
import org.bdgenomics.adam.instrumentation.ValueExtractor._
import com.netflix.servo.tag.Tag
import org.bdgenomics.adam.instrumentation.SparkMetrics._

/**
 * Allows metrics for Spark to be captured and rendered in tabular form.
 */
abstract class SparkMetrics {

  private val taskTimers = new mutable.ArrayBuffer[TaskTimer]()
  private val stageIdToName = new mutable.HashMap[Int, String]()

  def print(out: PrintStream) = {
    val overallMonitors = taskTimers.map(_.getOverallTimings).sortBy(-_.getTotalTime)
    val ordering = getOrdering(overallMonitors)
    val monitorsByHost = taskTimers.flatMap(_.getHostTimings).sorted(ordering)
    val monitorsByStageName = taskTimers.flatMap(_.getStageTimings).map(addStageName).sorted(ordering)
    renderTable(out, "Task Timings", overallMonitors, createBaseHeader())
    out.println()
    renderTable(out, "Task Timings By Host", monitorsByHost,
      createHeaderWith(TableHeader(name = "Host", valueExtractor = forTagValueWithKey(HostTagKey), alignment = Alignment.Left), 1))
    out.println()
    renderTable(out, "Task Timings By Stage", monitorsByStageName,
      createHeaderWith(TableHeader(name = "Stage ID & Name", valueExtractor = forTagValueWithKey(StageNameTagKey), alignment = Alignment.Left), 1))
  }

  def mapStageIdToName(stageId: Int, stageName: String) {
    stageIdToName.put(stageId, stageName)
  }

  /**
   * Subclasses should call this method to create a new [[TaskTimer]] and to register it
   */
  protected def taskTimer(name: String) = {
    val timer = new TaskTimer(name)
    taskTimers += timer
    timer
  }

  /**
   * Uses the sort order from the names of the passed-in timers to create an [[Ordering]]
   */
  private def getOrdering(timers: Seq[ServoTimer]): Ordering[ServoTimer] = {
    val sortOrderMap = getSortOrder(timers)
    object TimerOrdering extends Ordering[ServoTimer] {
      def compare(a: ServoTimer, b: ServoTimer): Int = {
        val sortOrderA = sortOrderMap.get(a.getName)
        val sortOrderB = sortOrderMap.get(b.getName)
        if (sortOrderA.isEmpty || sortOrderB.isEmpty || sortOrderA == sortOrderB) {
          -(a.getTotalTime compare b.getTotalTime)
        } else {
          sortOrderA.get - sortOrderB.get
        }
      }
    }
    TimerOrdering
  }

  /**
   * Gets a map of the timer name to the order in the passed-in list
   */
  private def getSortOrder(timers: Seq[ServoTimer]): Map[String, Int] = {
    var sortOrder: Int = 0
    timers.map(timer => {
      sortOrder = sortOrder + 1
      (timer.getName, sortOrder)
    }).toMap
  }

  private def addStageName(stageIdAndTimer: (Int, ServoTimer)): ServoTimer = {
    val stageName = stageIdAndTimer._1 + ": " + stageIdToName.get(stageIdAndTimer._1).getOrElse("unknown")
    stageIdAndTimer._2.addTag(newTag(StageNameTagKey, stageName))
    stageIdAndTimer._2
  }

  private def renderTable(out: PrintStream, name: String, timers: Seq[ServoTimer], header: ArrayBuffer[TableHeader]) = {
    val monitorTable = new MonitorTable(header.toArray, timers.toArray)
    out.println(name)
    monitorTable.print(out)
  }

  private def createHeaderWith(header: TableHeader, position: Int): ArrayBuffer[TableHeader] = {
    val baseHeader = createBaseHeader()
    baseHeader.insert(position, header)
    baseHeader
  }

  private def createBaseHeader(): ArrayBuffer[TableHeader] = {
    ArrayBuffer(
      TableHeader(name = "Metric", valueExtractor = forTagValueWithKey(NameTagKey), alignment = Alignment.Left),
      TableHeader(name = "Total Time", valueExtractor = forMonitorMatchingTag(TotalTimeTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Count", valueExtractor = forMonitorMatchingTag(CountTag)),
      TableHeader(name = "Mean", valueExtractor = forMonitorMatchingTag(MeanTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Min", valueExtractor = forMonitorMatchingTag(MinTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Max", valueExtractor = forMonitorMatchingTag(MaxTag), formatFunction = Some(formatNanos)))
  }

  private def formatNanos(number: Any): String = {
    // We need to do some dynamic type checking here, as monitors return an Object
    number match {
      case number: Number => DurationFormatting.formatNanosecondDuration(number)
      case _              => throw new IllegalArgumentException("Cannot format non-numeric value [" + number + "]")
    }
  }

}

protected object SparkMetrics {
  final val HostTagKey = "host"
  final val StageNameTagKey = "stageName"
  final val StageIdTagKey = "stageId"
}

class TaskTimer(name: String) {
  val overallTimings = buildTimer(name)
  val timingsByHost = new mutable.HashMap[String, ServoTimer]
  val timingsByStageId = new mutable.HashMap[Int, ServoTimer]
  def +=(millisecondTiming: Long)(implicit taskContext: TaskContext) = {
    recordMillis(overallTimings, millisecondTiming)
    recordMillis(timingsByHost.getOrElseUpdate(taskContext.hostname,
      buildTimer(name, newTag(HostTagKey, taskContext.hostname))), millisecondTiming)
    recordMillis(timingsByStageId.getOrElseUpdate(taskContext.stageId,
      buildTimer(name, newTag(StageIdTagKey, taskContext.stageId.toString))), millisecondTiming)
  }
  def getOverallTimings: ServoTimer = {
    overallTimings
  }
  def getHostTimings: Iterable[ServoTimer] = {
    timingsByHost.values
  }
  def getStageTimings: Seq[(Int, ServoTimer)] = {
    timingsByStageId.toSeq
  }
  private def recordMillis(timer: ServoTimer, milliSecondTiming: Long) = {
    timer.recordMillis(milliSecondTiming)
  }
  private def buildTimer(name: String): ServoTimer = {
    new ServoTimer(MonitorConfig.builder(name).build())
  }
  private def buildTimer(name: String, tag: Tag): ServoTimer = {
    new ServoTimer(MonitorConfig.builder(name).withTag(tag).build())
  }
}

case class TaskContext(hostname: String, stageId: Int)
