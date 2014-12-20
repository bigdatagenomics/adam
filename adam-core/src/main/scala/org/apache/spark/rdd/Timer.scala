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
package org.apache.spark.rdd

import org.bdgenomics.adam.instrumentation.{ Clock, Metrics, MetricsRecorder }

/**
 * Represents a timer, for timing a function. Call the `time` function, passing the function to time.
 *
 * For recording metrics the [[Timer]] either uses the passed-in [[MetricsRecorder]] if it is defined, or it looks in
 * the [[Metrics.Recorder]] field for a recorder. If neither of these are defined then no metrics are recorded
 * (the function is executed without recording metrics).
 *
 * The overhead of recording metrics has been measured at around 100 nanoseconds on an Intel i7-3720QM. The overhead
 * of calling the `time` method when no metrics are being recorded (a recorder is not defined) is negligible.
 *
 * @note This class needs to be in the org.apache.spark.rdd package, otherwise Spark records somewhere in the
 *       `time` method as the call site (which in turn becomes the stage name).
 *       This can be fixed when Spark 1.1.1 is released (needs SPARK-1853).
 */
class Timer(name: String, clock: Clock = new Clock(), recorder: Option[MetricsRecorder] = None,
            sequenceId: Option[Int] = None, isRDDOperation: Boolean = false) extends Serializable {
  // Ensure all timer names are interned, since there should not be many distinct values and this will enable
  // us to compare timer names much more efficiently (they can be compared by reference).
  val timerName = name.intern()
  /**
   * Runs f, recording its duration, and returns its result.
   */
  def time[A](f: => A): A = {
    val recorderOption = if (recorder.isDefined) recorder else Metrics.Recorder.value
    // If we were not initialized this will not be set, and nothing will be recorded (which is what we want)
    if (recorderOption.isDefined) {
      val recorder = recorderOption.get
      val startTime = clock.nanoTime()
      recorder.startPhase(timerName, sequenceId, isRDDOperation)
      try {
        f
      } finally {
        recorder.finishPhase(timerName, clock.nanoTime() - startTime)
      }
    } else {
      f
    }
  }
}