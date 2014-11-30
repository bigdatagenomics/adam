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

import java.io.{ BufferedReader, ByteArrayOutputStream, PrintStream, StringReader }
import org.apache.spark.Logging
import org.apache.spark.rdd.Timer
import org.bdgenomics.adam.instrumentation.InstrumentationTestingUtil._
import org.bdgenomics.adam.util.SparkFunSuite
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration.Duration

class MetricsSuite extends SparkFunSuite with Logging with BeforeAndAfterAll {

  sparkTest("Timer metrics are computed correctly") {

    val testingClock = new TestingClock()

    val testTimers = new TestTimers(testingClock)
    Metrics.initialize(sc)

    testTimers.timer1.time {
      testingClock.currentTime += 30000000
      testTimers.timer2.time {
        testingClock.currentTime += 200000
      }
      testTimers.timer2.time {
        testingClock.currentTime += 200000
        testTimers.timer3.time {
          testingClock.currentTime += 100000
        }
        testTimers.timer4.time {
          testingClock.currentTime += 200000
        }
      }
      testTimers.timer1.time {
        testingClock.currentTime += 600000
      }
      testTimers.timer3.time {
        testingClock.currentTime += 200000
      }
    }
    testTimers.timer1.time {
      testingClock.currentTime += 40000000
    }
    testTimers.timer2.time {
      testingClock.currentTime += 20000000
    }
    testTimers.timer1.time {
      testingClock.currentTime += 10000000
    }

    val renderedTable = getRenderedTable(None)
    val reader = new BufferedReader(new StringReader(renderedTable))

    // We are using a prefix string here so that we compare the whitespace at the start of the metrics column
    // as well (if we didn't use the prefix it would be trimmed). In other words, we want to compare the actual
    // tree structure and not just the individual metrics.
    checkTable("Timings", getExpectedTimerValues, reader, prefixString = Some("#"))

  }

  sparkTest("Timer metrics are computed correctly for RDD operations") {

    val testingClock = new TestingClock()

    val testTimers = new TestTimers(testingClock)
    Metrics.initialize(sc)

    timeRddOperations(testTimers, testingClock)

    val stageTimings = Seq(
      StageTiming(1, Some("RDD Group Operation 2"), Duration.fromNanos(1000000)),
      StageTiming(2, Some("RDD Map Operation 1"), Duration.fromNanos(2000000)))

    val renderedTable = renderTableFromMetricsObject(Some(stageTimings))
    // We are using a prefix string here so that we compare the whitespace at the start of the metrics column
    // as well (if we didn't use the prefix it would be trimmed). In other words, we want to compare the actual
    // tree structure and not just the individual metrics.
    checkTable("Timings", getExpectedTimerValuesWithRDDOperations,
      new BufferedReader(new StringReader(renderedTable)), prefixString = Some("#"))

    checkTable("Spark Operations", getExpectedSparkOperations, new BufferedReader(new StringReader(renderedTable)))

  }

  def timeRddOperations(testTimers: MetricsSuite.this.type#TestTimers, testingClock: TestingClock) {
    testTimers.timer4.time {
      testingClock.currentTime += 30000000
      testTimers.timer1.time {
        testingClock.currentTime += 30000000
        val rddGroup = new Timer("RDD Group Operation 1", clock = testingClock, sequenceId = Some(2), isRDDOperation = true)
        rddGroup.time {
          testingClock.currentTime += 1000000
          testTimers.timer3.time {
            testingClock.currentTime += 2000000
          }
          testTimers.timer3.time {
            testingClock.currentTime += 2000000
          }
        }
        val rddMap = new Timer("RDD Map Operation 1", clock = testingClock, sequenceId = Some(1), isRDDOperation = true)
        rddMap.time {
          testingClock.currentTime += 1000000
          testTimers.timer2.time {
            testingClock.currentTime += 1000000
          }
        }
      }
      testTimers.timer1.time {
        testingClock.currentTime += 30000000
        val rddGroup = new Timer("RDD Group Operation 1", clock = testingClock, sequenceId = Some(3), isRDDOperation = true)
        rddGroup.time {
          testingClock.currentTime += 2000000
        }
      }
      val rddGroup = new Timer("RDD Group Operation 2", clock = testingClock, sequenceId = Some(4), isRDDOperation = true)
      rddGroup.time {
        testingClock.currentTime += 2000000
      }
    }
  }

  private def getExpectedSparkOperations: Array[Array[String]] = {
    Array(
      Array("Operation", "Is Blocking?", "Duration", "Stage ID"),
      // Operations occur in order of sequence ID. Two of them are matched to Spark stages --
      // these are the ones that are marked as blocking, and have timings and stage IDs.
      Array("RDD Map Operation 1", "true", "2 ms", "2"),
      Array("RDD Group Operation 1", "false", "-", "-"),
      Array("RDD Group Operation 1", "false", "-", "-"),
      Array("RDD Group Operation 2", "true", "1 ms", "1"))
  }

  private def getExpectedTimerValuesWithRDDOperations: Array[Array[String]] = {
    Array(
      Array("Metric", "Worker Time", "Driver Time", "Count", "Mean", "Min", "Max"),
      // Timer 4 and 1 have occurred in the driver (before the RDD operation). The time taken in the RDD
      // operations has been subtracted from their time, which is recorded as "driver time".
      Array("# └─ Timer 4", "-", "90 ms", "1", "90 ms", "-", "-"),
      Array("#     ├─ Timer 1", "-", "60 ms", "2", "30 ms", "-", "-"),
      // The three RDD operations are arranged in order of sequence ID. The RDD operations do not have a time
      // recorded (only a count). The timings within the RDD operations are recorded as "worker time".
      Array("#     │   ├─ RDD Map Operation 1", "-", "-", "1", "-", "-", "-"),
      Array("#     │   │   └─ Timer 2", "1 ms", "-", "1", "1 ms", "1 ms", "1 ms"),
      Array("#     │   ├─ RDD Group Operation 1", "-", "-", "1", "-", "-", "-"),
      Array("#     │   │   └─ Timer 3", "4 ms", "-", "2", "2 ms", "2 ms", "2 ms"),
      Array("#     │   └─ RDD Group Operation 1", "-", "-", "1", "-", "-", "-"),
      // Another RDD operation has occurred, as a direct child of the root (Timer 4)
      Array("#     └─ RDD Group Operation 2", "-", "-", "1", "-", "-", "-"))
  }

  private def getExpectedTimerValues: Array[Array[String]] = {
    Array(
      Array("Metric", "Worker Time", "Driver Time", "Count", "Mean", "Min", "Max"),
      // The two consecutive top-level instances of Timer 1 should have been combined
      Array("# └─ Timer 1", "-", "71.5 ms", "2", "35.75 ms", "31.5 ms", "40 ms"),
      // Timer 2 has occurred twice as a child of Timer 1, once taking 200 µs, and once taking 500 µs (including children)
      Array("#     ├─ Timer 2", "-", "700 µs", "2", "350 µs", "200 µs", "500 µs"),
      // Timer 4 has occurred once as a child of Timer 2 -- it comes before Timer 3, as its total time is greater
      Array("#     │   ├─ Timer 4", "-", "200 µs", "1", "200 µs", "200 µs", "200 µs"),
      // Timer 3 has occurred once as a child of Timer 2 -- it comes after Timer 4, as its total time is smaller
      Array("#     │   └─ Timer 3", "-", "100 µs", "1", "100 µs", "100 µs", "100 µs"),
      // Timer 1 has occurred once as a child of Timer 1 -- it comes before Timer 3, as its total timer is greater
      Array("#     ├─ Timer 1", "-", "600 µs", "1", "600 µs", "600 µs", "600 µs"),
      // Timer 3 has occurred once as a child of Timer 1 -- it comes after Timer 1, as its total timer is smaller
      Array("#     └─ Timer 3", "-", "200 µs", "1", "200 µs", "200 µs", "200 µs"),
      // Timer 2 has occurred once at the top level, after the two instances of Timer 1
      Array("# └─ Timer 2", "-", "20 ms", "1", "20 ms", "20 ms", "20 ms"),
      // Timer 2 has occurred again at the top level but is not consecutive with the first instance
      Array("# └─ Timer 1", "-", "10 ms", "1", "10 ms", "10 ms", "10 ms"))
  }

  private def getRenderedTable(sparkStageTimings: Option[Seq[StageTiming]]): String = {
    val bytes = new ByteArrayOutputStream()
    val out = new PrintStream(bytes)
    Metrics.print(out, sparkStageTimings)
    val renderedTable = bytes.toString("UTF8")
    renderedTable
  }

  class TestTimers(testingClock: TestingClock) extends Metrics(testingClock) {
    val timer1 = timer("Timer 1")
    val timer2 = timer("Timer 2")
    val timer3 = timer("Timer 3")
    val timer4 = timer("Timer 4")
  }

}
