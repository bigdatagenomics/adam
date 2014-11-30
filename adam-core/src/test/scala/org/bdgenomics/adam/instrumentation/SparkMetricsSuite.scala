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

import java.io._
import java.util.concurrent.TimeUnit
import org.apache.spark.Logging
import org.bdgenomics.adam.instrumentation.InstrumentationTestingUtil._
import org.scalatest.FunSuite
import scala.concurrent.duration._

class SparkMetricsSuite extends FunSuite with Logging {

  test("Task metrics are captured correctly") {

    val myMetrics = new MyMetrics()

    addValue(myMetrics.metric1, 100, "host1", 1)
    addValue(myMetrics.metric1, 110, "host1", 2)
    addValue(myMetrics.metric1, 111, "host2", 2)
    addValue(myMetrics.metric1, 112, "host2", 1)
    addValue(myMetrics.metric1, 113, "host3", 3)

    assert(fromNanos(myMetrics.metric1.overallTimings.getTotalTime) === 546)

    val stageTimings = myMetrics.metric1.timingsByStageId
    assert(fromNanos(stageTimings.get(1).get.getTotalTime) === 212)
    assert(fromNanos(stageTimings.get(2).get.getTotalTime) === 221)
    assert(fromNanos(stageTimings.get(3).get.getTotalTime) === 113)

    val hostTimings = myMetrics.metric1.timingsByHost
    assert(fromNanos(hostTimings.get("host1").get.getTotalTime) === 210)
    assert(fromNanos(hostTimings.get("host2").get.getTotalTime) === 223)
    assert(fromNanos(hostTimings.get("host3").get.getTotalTime) === 113)

  }

  test("Stage metrics are captured correctly") {

    val myMetrics = new MyMetrics()

    myMetrics.recordStageDuration(1, Some("stage1"), Duration(100, MILLISECONDS))
    myMetrics.recordStageDuration(2, None, Duration(200, MILLISECONDS))

    val stage1 = myMetrics.stageTimes.filter(_.stageId == 1).iterator.next()
    assert(stage1.stageName.get === "stage1")
    assert(fromNanos(stage1.duration.toNanos) === 100)
    val stage2 = myMetrics.stageTimes.filter(_.stageId == 2).iterator.next()
    assert(stage2.stageName === None)
    assert(fromNanos(stage2.duration.toNanos) === 200)

  }

  test("Metrics are rendered correctly") {

    val myMetrics = new MyMetrics()

    addValue(myMetrics.metric1, 100, "host1", 1)
    addValue(myMetrics.metric1, 110, "host1", 2)
    addValue(myMetrics.metric1, 111, "host2", 2)
    addValue(myMetrics.metric1, 112, "host2", 1)
    addValue(myMetrics.metric2, 200, "host1", 1)
    addValue(myMetrics.metric2, 210, "host1", 2)
    addValue(myMetrics.metric2, 211, "host2", 2)
    addValue(myMetrics.metric2, 212, "host2", 1)

    myMetrics.recordStageDuration(1, None, Duration(100, MILLISECONDS))
    myMetrics.recordStageDuration(2, Some("stage2"), Duration(200, MILLISECONDS))

    // Don't map stage 1 so we test what happens in this case (we output "unknown")
    myMetrics.mapStageIdToName(2, "stage2")

    val renderedTable = getRenderedTable(myMetrics)
    val reader = new BufferedReader(new StringReader(renderedTable))

    val expectedOverallValues = getExpectedOverallValues
    checkTable("Task Timings", expectedOverallValues, reader)

    val expectedValuesByHost = getExpectedValuesByHost
    checkTable("Task Timings By Host", expectedValuesByHost, reader)

    val expectedValuesByStage = getExpectedValuesByStage
    checkTable("Task Timings By Stage", expectedValuesByStage, reader)

  }

  private def getExpectedStageDurations: Array[Array[String]] = {
    Array(
      Array("Stage ID & Name", "Duration"),
      Array("2: stage2", "200 ms"),
      Array("1: unknown", "100 ms"),
      Array("TOTAL", "300 ms"))
  }

  private def getExpectedOverallValues: Array[Array[String]] = {
    Array(
      Array("Metric", "Total Time", "Count", "Mean", "Min", "Max"),
      Array("Metric 2", "833 ms", "4", "208.25 ms", "200 ms", "212 ms"),
      Array("Metric 1", "433 ms", "4", "108.25 ms", "100 ms", "112 ms"))
  }

  private def getExpectedValuesByHost: Array[Array[String]] = {
    Array(
      Array("Metric", "Host", "Total Time", "Count", "Mean", "Min", "Max"),
      Array("Metric 2", "host2", "423 ms", "2", "211.5 ms", "211 ms", "212 ms"),
      Array("Metric 2", "host1", "410 ms", "2", "205 ms", "200 ms", "210 ms"),
      Array("Metric 1", "host2", "223 ms", "2", "111.5 ms", "111 ms", "112 ms"),
      Array("Metric 1", "host1", "210 ms", "2", "105 ms", "100 ms", "110 ms"))
  }

  private def getExpectedValuesByStage: Array[Array[String]] = {
    Array(
      Array("Metric", "Stage ID & Name", "Total Time", "Count", "Mean", "Min", "Max"),
      Array("Metric 2", "2: stage2", "421 ms", "2", "210.5 ms", "210 ms", "211 ms"),
      Array("Metric 2", "1: unknown", "412 ms", "2", "206 ms", "200 ms", "212 ms"),
      Array("Metric 1", "2: stage2", "221 ms", "2", "110.5 ms", "110 ms", "111 ms"),
      Array("Metric 1", "1: unknown", "212 ms", "2", "106 ms", "100 ms", "112 ms"))
  }

  private def addValue(timer: TaskTimer, value: Long, host: String, stage: Int) {
    implicit val taskContext = TaskContext(host, stage)
    timer += value
  }

  private def getRenderedTable(myMetrics: MyMetrics): String = {
    val bytes = new ByteArrayOutputStream()
    val out = new PrintStream(bytes)
    myMetrics.print(out)
    bytes.toString("UTF8")
  }

  private def fromNanos(value: Long): Long = {
    TimeUnit.MILLISECONDS.convert(value, TimeUnit.NANOSECONDS)
  }

  private class MyMetrics extends SparkMetrics {
    val metric1 = taskTimer("Metric 1")
    val metric2 = taskTimer("Metric 2")
  }

}
