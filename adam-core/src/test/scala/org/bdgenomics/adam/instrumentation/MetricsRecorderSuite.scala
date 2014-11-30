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

import org.apache.spark.Accumulable
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{ times, verify }
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar._

class MetricsRecorderSuite extends FunSuite {

  test("Timings are recorded correctly") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase("Timer 1", sequenceId = Some(1))
    recorder.finishPhase("Timer 1", 100000)
    val timingPath = new TimingPath("Timer 1", None, sequenceId = 1)
    verify(accumulable).+=(new RecordedTiming(100000, timingPath))
  }

  test("Nested timings are recorded correctly") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase("Timer 1", sequenceId = Some(1))
    recorder.startPhase("Timer 2", sequenceId = Some(1))
    recorder.finishPhase("Timer 2", 200000)
    recorder.startPhase("Timer 3", sequenceId = Some(1))
    recorder.finishPhase("Timer 3", 300000)
    recorder.finishPhase("Timer 1", 100000)
    val timingPath1 = new TimingPath("Timer 1", None, sequenceId = 1)
    val timingPath2 = new TimingPath("Timer 2", Some(timingPath1), sequenceId = 1)
    val timingPath3 = new TimingPath("Timer 3", Some(timingPath1), sequenceId = 1)
    verify(accumulable).+=(new RecordedTiming(200000, timingPath2))
    verify(accumulable).+=(new RecordedTiming(300000, timingPath3))
    verify(accumulable).+=(new RecordedTiming(100000, timingPath1))
  }

  test("New top-level operations get new sequence IDs") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase("Timer 1")
    recorder.finishPhase("Timer 1", 100000)
    recorder.startPhase("Timer 2")
    recorder.finishPhase("Timer 2", 100000)
    val recordedTimings = ArgumentCaptor.forClass(classOf[RecordedTiming])
    verify(accumulable, times(2)).+=(recordedTimings.capture())
    val allTimings = recordedTimings.getAllValues
    assert(allTimings.size() === 2)
    assert(allTimings.get(1).pathToRoot.sequenceId > allTimings.get(0).pathToRoot.sequenceId)
  }

  test("Repeated top-level operations get new sequence IDs") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    recorder.startPhase("Timer 1")
    recorder.finishPhase("Timer 1", 100000)
    recorder.startPhase("Timer 1")
    recorder.finishPhase("Timer 1", 100000)
    val recordedTimings = ArgumentCaptor.forClass(classOf[RecordedTiming])
    verify(accumulable, times(2)).+=(recordedTimings.capture())
    val allTimings = recordedTimings.getAllValues
    assert(allTimings.size() === 2)
    assert(allTimings.get(1).pathToRoot.sequenceId === allTimings.get(0).pathToRoot.sequenceId)
  }

  test("Non-matching timer name causes assertion error") {
    val accumulable = mock[Accumulable[ServoTimers, RecordedTiming]]
    val recorder = new MetricsRecorder(accumulable)
    intercept[AssertionError] {
      recorder.startPhase("Timer 2", sequenceId = Some(1))
      recorder.finishPhase("Timer 3", 200000)
    }
  }

}
