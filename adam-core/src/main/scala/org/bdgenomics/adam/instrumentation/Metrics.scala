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

import com.netflix.servo.monitor.{ BasicCompositeMonitor, LongGauge, Monitor, MonitorConfig }
import com.netflix.servo.tag.{ Tag, Tags }
import java.io.PrintStream
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.rdd.Timer
import org.apache.spark.{ Accumulable, SparkContext }
import org.bdgenomics.adam.instrumentation.InstrumentationFunctions._
import org.bdgenomics.adam.instrumentation.ServoTimer._
import org.bdgenomics.adam.instrumentation.ValueExtractor._
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.DynamicVariable

/**
 * Allows metrics to be created for an application. Currently only timers are supported, but other types of metrics
 * may be supported in the future.
 *
 * Classes should extend this class to provide collections of metrics for an application. For example, typical usage
 * might be:
 *
 * {{{
 * object Timers extends Metrics {
 *   val Operation1 = timer("Operation 1")
 *   val Operation2 = timer("Operation 2")
 * }
 * }}}
 *
 * This creates two timers: one for Operation 1, and one for Operation 2.
 *
 * To switch-on recording of metrics for a specific thread, the companion [[Metrics]] object must be used.
 */
abstract class Metrics(val clock: Clock = new Clock()) extends Serializable {
  /**
   * Creates a timer with the specified name.
   */
  def timer(name: String): Timer = {
    val timer = new Timer(name, clock)
    timer
  }
}

/**
 * Manages recording of metrics in an application. Metrics collection is turned on for a particular thread by
 * calling the `initialize` method. After `initialize` is called the [[Metrics.Recorder]] field is set, and the
 * application can use that to record metrics. To print metrics, call the `print` method, and  to stop recording metrics
 * for a particular thread, call the `stopRecording` method.
 *
 * If an application does not want to record metrics, it can simply avoid calling the `initialize` method (or can
 * call the `stopRecording` method if there is a chance that `initialize` has been called on that thread previously).
 * Attempting to record metrics when the initialize method has not been called will not produce an error, and incurs very
 * little overhead. However, attempting to call the print method to print the metrics will produce an error.
 */
object Metrics {

  private implicit val accumulableParam = new ServoTimersAccumulableParam()

  private final val TreePathTagKey = "TreePath"
  private final val BlockingTagKey = "IsBlocking"
  private final val StageIdTagKey = "StageId"
  private final val DriverTimeTag = Tags.newTag("statistic", "DriverTime")
  private final val WorkerTimeTag = Tags.newTag("statistic", "WorkerTime")
  private final val StageDurationTag = Tags.newTag("statistic", "StageDuration")

  private val sequenceIdGenerator = new AtomicInteger()

  /**
   * Dynamic variable (thread-local) which allows metrics to be recorded for the current thread. Note that
   * this will be set to [[None]] if metrics have not been initialized for the current thread, so callers should
   * deal with this appropriately (normally by avoiding recording any metrics or propagating the recorder
   * to other threads).
   */
  final val Recorder = new DynamicVariable[Option[MetricsRecorder]](None)

  /**
   * Starts recording metrics for this thread. Any previously-recorded metrics for this thread will be cleared.
   * This method must always be called before recording metrics.
   */
  def initialize(sparkContext: SparkContext) = synchronized {
    val accumulable = sparkContext.accumulable[ServoTimers, RecordedTiming](new ServoTimers())
    val metricsRecorder = new MetricsRecorder(accumulable)
    Metrics.Recorder.value = Some(metricsRecorder)
  }

  /**
   * Stops recording metrics for the current thread. This avoids the (slight) overhead of recording metrics
   * if they are not going to be used. Calling the `print` method after calling this one will result in an
   * [[IllegalStateException]].
   */
  def stopRecording() {
    Metrics.Recorder.value = None
  }

  /**
   * Prints the metrics recorded by this instance to the specified [[PrintStream]], using the specified
   * [[SparkMetrics]] to print details of any Spark operations that have occurred.
   */
  def print(out: PrintStream, sparkStageTimings: Option[Seq[StageTiming]]) {
    if (!Metrics.Recorder.value.isDefined) {
      throw new IllegalStateException("Trying to print metrics for an uninitialized Metrics class! " +
        "Call the initialize method to initialize it.")
    }
    val accumulable = Recorder.value.get.accumulable
    val treeRoots = buildTree(accumulable).toSeq.sortWith((a, b) => { a.timingPath.sequenceId < b.timingPath.sequenceId })
    val treeNodeRows = new mutable.ArrayBuffer[Monitor[_]]()
    treeRoots.foreach(treeNode => { treeNode.addToTable(treeNodeRows) })
    renderTable(out, "Timings", treeNodeRows, createTreeViewHeader())
    out.println()
    sparkStageTimings.foreach(printRddOperations(out, _, accumulable))
  }

  /**
   * Generates a new sequence ID for operations that we wish to appear in sequence
   */
  def generateNewSequenceId(): Int = {
    val newValue = sequenceIdGenerator.incrementAndGet()
    if (newValue < 0) {
      // This really shouldn't happen, but just in case...
      throw new IllegalStateException("Out of sequence IDs!")
    }
    newValue
  }

  private def printRddOperations(out: PrintStream, sparkStageTimings: Seq[StageTiming],
                                 accumulable: Accumulable[ServoTimers, RecordedTiming]) {

    // First, extract a list of the RDD operations, sorted by sequence ID (the order in which they occurred)
    val sortedRddOperations = accumulable.value.timerMap.filter(_._1.isRDDOperation).toList.sortBy(_._1.sequenceId)

    // Now, create a map from the Spark stages so that we can look them up
    val stageMap = sparkStageTimings.map(t => t.stageName -> t).toMap

    val rddMonitors = sortedRddOperations.map(rddOperation => {
      val name = rddOperation._2.getName
      val stageOption = stageMap.get(Some(name))
      val durationMonitors = new mutable.ArrayBuffer[Monitor[_]]()
      val monitorConfig = MonitorConfig.builder(name).withTag(NameTagKey, name)
        .withTag(BlockingTagKey, stageOption.isDefined.toString)
      stageOption.foreach(stage => {
        val durationGauge = new LongGauge(MonitorConfig.builder(name).withTag(StageDurationTag).build())
        durationGauge.set(stage.duration.toNanos)
        durationMonitors += durationGauge
        monitorConfig.withTag(StageIdTagKey, stage.stageId.toString)
      })
      new BasicCompositeMonitor(monitorConfig.build(), durationMonitors)
    })

    renderTable(out, "Spark Operations", rddMonitors, createRDDOperationsHeader())

  }

  private def createRDDOperationsHeader(): ArrayBuffer[TableHeader] = {
    ArrayBuffer(
      TableHeader(name = "Operation", valueExtractor = forTagValueWithKey(NameTagKey), alignment = Alignment.Left),
      TableHeader(name = "Is Blocking?", valueExtractor = forTagValueWithKey(BlockingTagKey), alignment = Alignment.Left),
      TableHeader(name = "Duration", valueExtractor = forMonitorMatchingTag(StageDurationTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Stage ID", valueExtractor = forTagValueWithKey(StageIdTagKey), alignment = Alignment.Left))
  }

  private def createTreeViewHeader(): ArrayBuffer[TableHeader] = {
    ArrayBuffer(
      TableHeader(name = "Metric", valueExtractor = forTagValueWithKey(TreePathTagKey), alignment = Alignment.Left),
      TableHeader(name = "Worker Time", valueExtractor = forMonitorMatchingTag(WorkerTimeTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Driver Time", valueExtractor = forMonitorMatchingTag(DriverTimeTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Count", valueExtractor = forMonitorMatchingTag(CountTag)),
      TableHeader(name = "Mean", valueExtractor = forMonitorMatchingTag(MeanTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Min", valueExtractor = forMonitorMatchingTag(MinTag), formatFunction = Some(formatNanos)),
      TableHeader(name = "Max", valueExtractor = forMonitorMatchingTag(MaxTag), formatFunction = Some(formatNanos)))
  }

  private def buildTree(accumulable: Accumulable[ServoTimers, RecordedTiming]): Iterable[TreeNode] = {
    val timerPaths: Seq[(TimingPath, ServoTimer)] = accumulable.value.timerMap.toSeq
    val rootNodes = new mutable.LinkedHashMap[TimingPath, TreeNode]
    buildTree(timerPaths, 0, rootNodes)
    rootNodes.values
  }

  @tailrec
  private def buildTree(timerPaths: Seq[(TimingPath, ServoTimer)], depth: Int,
                        parentNodes: mutable.Map[TimingPath, TreeNode]) {
    val currentLevelNodes = new mutable.HashMap[TimingPath, TreeNode]()
    timerPaths.filter(_._1.depth == depth).foreach(timerPath => {
      addToMaps(timerPath, parentNodes, currentLevelNodes)
    })
    if (!currentLevelNodes.isEmpty) {
      buildTree(timerPaths, depth + 1, currentLevelNodes)
    }
  }

  private def addToMaps(timerPath: (TimingPath, ServoTimer), parents: mutable.Map[TimingPath, TreeNode],
                        currentLevelNodes: mutable.Map[TimingPath, TreeNode]) = {
    // If this is a non-root node, add it to the parent node. Otherwise, just put it in the maps.
    val parentPath = timerPath._1.parentPath
    if (parentPath.isDefined) {
      parents.get(parentPath.get).foreach(parentNode => {
        val node = new TreeNode(timerPath, Some(parentNode))
        parentNode.addChild(node)
        currentLevelNodes.put(timerPath._1, node)
      })
    } else {
      val node = new TreeNode(timerPath, None)
      parents.put(node.timingPath, node)
      currentLevelNodes.put(timerPath._1, node)
    }
  }

  private class TreeNode(nodeData: (TimingPath, ServoTimer), val parent: Option[TreeNode]) {

    val timingPath = nodeData._1
    val timer = nodeData._2

    val children = new mutable.ArrayBuffer[TreeNode]()

    // We subtract the time taken for RDD operations from all of its ancestors, since the time is misleading
    adjustTimingsForRddOperations()

    def addChild(node: TreeNode) = {
      children += node
    }

    def addToTable(rows: mutable.Buffer[Monitor[_]]) {
      addToTable(rows, "", isInSparkWorker = false, isTail = true)
    }

    def addToTable(rows: mutable.Buffer[Monitor[_]], prefix: String, isInSparkWorker: Boolean, isTail: Boolean) {
      // We always sort the children every time (by sequence ID then largest total time) before printing,
      // but we assume that printing isn't a very common operation
      val sortedChildren = children.sortWith((a, b) => { childLessThan(a, b) })
      val name = timer.name
      addToRows(rows, prefix + (if (isTail) "└─ " else "├─ ") + name, timer, isInSparkWorker = isInSparkWorker)
      for (i <- 0 until sortedChildren.size) {
        if (i < sortedChildren.size - 1) {
          sortedChildren.get(i).addToTable(rows, prefix + (if (isTail) "    " else "│   "),
            isInSparkWorker = isInSparkWorker || timingPath.isRDDOperation, isTail = false)
        } else {
          sortedChildren.get(sortedChildren.size - 1).addToTable(rows, prefix + (if (isTail) "    " else "│   "),
            isInSparkWorker = isInSparkWorker || timingPath.isRDDOperation, isTail = true)
        }
      }
    }

    private def adjustTimingsForRddOperations() = {
      if (timingPath.isRDDOperation) {
        parent.foreach(_.subtractTimingFromAncestors(timer.getTotalTime))
      }
    }

    private def subtractTimingFromAncestors(totalTime: Long) {
      timer.adjustTotalTime(-totalTime)
      parent.foreach(_.subtractTimingFromAncestors(totalTime))
    }

    private def childLessThan(a: TreeNode, b: TreeNode): Boolean = {
      if (a.timingPath.sequenceId == b.timingPath.sequenceId) {
        a.timer.getTotalTime > b.timer.getTotalTime
      } else {
        a.timingPath.sequenceId < b.timingPath.sequenceId
      }
    }

    private def addToRows(rows: mutable.Buffer[Monitor[_]], treePath: String,
                          servoTimer: ServoTimer, isInSparkWorker: Boolean) = {
      // For RDD Operations the time taken is misleading since Spark executes operations lazily
      // (most take no time at all and the last one typically takes all of the time). So for RDD operations we
      // create a new monitor that just contains the count.
      if (timingPath.isRDDOperation) {
        val newConfig = servoTimer.getConfig.withAdditionalTag(Tags.newTag(TreePathTagKey, treePath))
        val count = new LongGauge(newConfig.withAdditionalTag(ServoTimer.CountTag))
        count.set(servoTimer.getCount)
        rows += new BasicCompositeMonitor(newConfig, List(count))
      } else {
        servoTimer.addTag(Tags.newTag(TreePathTagKey, treePath))
        val tag = if (isInSparkWorker) WorkerTimeTag else DriverTimeTag
        val gauge = new LongGauge(MonitorConfig.builder(tag.getKey).withTag(tag).build())
        gauge.set(servoTimer.getTotalTime)
        servoTimer.addSubMonitor(gauge)
        rows += servoTimer
      }
    }

  }

  private class StringMonitor(name: String, value: String, tags: Tag*) extends Monitor[String] {
    private val config = MonitorConfig.builder(name).withTags(tags).build()
    override def getConfig: MonitorConfig = {
      config
    }
    override def getValue: String = {
      value
    }
  }

}

class Clock extends Serializable {
  def nanoTime() = System.nanoTime()
}