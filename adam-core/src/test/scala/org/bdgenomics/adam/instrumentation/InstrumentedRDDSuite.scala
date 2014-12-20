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

import org.apache.spark.rdd.{ Timer, InstrumentedRDD }
import org.apache.spark.rdd.InstrumentedRDD._
import org.bdgenomics.adam.instrumentation.InstrumentationTestingUtil._
import org.bdgenomics.adam.util.SparkFunSuite
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConversions._

class InstrumentedRDDSuite extends SparkFunSuite with BeforeAndAfter {

  sparkBefore("Before") {
    Metrics.initialize(sc)
  }

  sparkAfter("After") {
    Metrics.stopRecording()
  }

  sparkTest("Operation is recorded correctly") {
    val testingClock = new TestingClock()
    InstrumentedRDD.clock = testingClock
    recordOperation({ testingClock.currentTime += 300000 })
    val timers = Metrics.Recorder.value.get.accumulable.value.timerMap
    assert(timers.size() === 1)
    assert(timers.values().iterator().next().getTotalTime === 300000)
    assert(timers.keys().nextElement().isRDDOperation === true)
    assert(timers.values().iterator().next().getName contains "recordOperation at InstrumentedRDDSuite.scala")
  }

  sparkTest("Function call is recorded correctly") {
    val testingClock = new TestingClock()
    InstrumentedRDD.clock = testingClock
    InstrumentedRDD.functionTimer = new Timer("function call", clock = testingClock)
    implicit val funcRecorder = functionRecorder()
    val recorder = funcRecorder.recorder
    assert(recorder ne Metrics.Recorder.value) // This should be a copy
    // Use a new thread to check that the recorder is propagated properly (as opposed to just using this thread's
    // thread-local value.
    val thread = new Thread() {
      override def run() {
        // DynamicVariables use inheritable thread locals, so set this to the default value to simulate
        // not having created the thread
        Metrics.Recorder.value = None
        recordFunction(myFunction(testingClock))
      }
    }
    thread.start()
    thread.join()
    val timers = recorder.get.accumulable.value.timerMap
    assert(timers.values().iterator().next().getTotalTime === 200000)
    assert(timers.keys().nextElement().isRDDOperation === false)
    assert(timers.values().iterator().next().getName contains "function call")
  }

  sparkTest("Instrumented Spark operation works correctly") {
    val rdd = new InstrumentedRDD(sc.parallelize(List.range(1, 11), 2))
    rdd.map(e => {
      OtherTimers.Timer1.time {
        OtherTimers.Timer2.time {
          e + 1
        }
      }
    }).count()
    val table = renderTableFromMetricsObject()
    val dataRows = rowsOfTable(table).filter(_.startsWith("|"))
    // We can't assert much on the timings themselves, but we can check that all timings were recorded and that
    // the names are correct
    assertOnNameAndCountInTimingsTable(dataRows.get(1), "map at InstrumentedRDDSuite.scala", 1)
    assertOnNameAndCountInTimingsTable(dataRows.get(2), "function call", 10)
    assertOnNameAndCountInTimingsTable(dataRows.get(3), "timer 1", 10)
    assertOnNameAndCountInTimingsTable(dataRows.get(4), "timer 2", 10)
  }

  def myFunction(testingClock: TestingClock): Boolean = {
    testingClock.currentTime += 200000
    true
  }

}

object OtherTimers extends Metrics {
  val Timer1 = timer("timer 1")
  val Timer2 = timer("timer 2")
}
