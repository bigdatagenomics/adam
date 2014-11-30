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

import org.apache.spark.rdd.Timer
import org.mockito.Mockito.verify
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar._

class TimerSuite extends FunSuite {

  test("Time method does not error when no recorder is defined") {
    val timer = new Timer("Timer 1")
    timer.time {}
  }

  test("Function times are recorded correctly with explicit recorder") {
    val testingClock = new TestingClock()
    val recorder = mock[MetricsRecorder]
    val recorderOption = Some(recorder)
    doTest(testingClock, recorder, recorderOption)
  }

  test("Function times are recorded correctly with thread-local recorder") {
    val testingClock = new TestingClock()
    val recorder = mock[MetricsRecorder]
    Metrics.Recorder.withValue(Some(recorder)) {
      doTest(testingClock, recorder, None)
    }
  }

  def doTest(testingClock: TestingClock, recorder: MetricsRecorder, explicitRecorder: Option[MetricsRecorder]) {
    val timer = new Timer("Timer 1", clock = testingClock, recorder = explicitRecorder)
    val text = timer.time {
      testingClock.currentTime += 30000
      "Finished!"
    }
    verify(recorder).startPhase("Timer 1", None, isRDDOperation = false)
    verify(recorder).finishPhase("Timer 1", 30000)
    assert(text === "Finished!") // Just make sure the result of the function is returned ok
  }

}
