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
import scala.collection.mutable

/**
 * Allows metrics to be recorded. Currently only timings are supported, but other metrics
 * may be supported in the future. Use the `startPhase` method to start recording a timing
 * and the `finishPhase` method to finish recording it. If timings are nested the
 * hierarchy is preserved.
 *
 * Note: this class is intended to be used in a thread-local context, and therefore it is not
 * thread-safe. Do not attempt to call it concurrently from multiple threads!
 */
class MetricsRecorder(val accumulable: Accumulable[ServoTimers, RecordedTiming],
                      existingTimings: Option[Seq[TimingPath]] = None) extends Serializable {

  // We don't attempt to make these variables thread-safe, as this class is explicitly
  // not thread-safe (it's generally intended to be used in a thread-local).

  private val timingsStack = new mutable.Stack[TimingPath]()
  existingTimings.foreach(_.foreach(timingsStack.push))
  private var previousTopLevelTimerName: String = null
  private var previousTopLevelSequenceId: Int = -1

  @transient private var roots = new mutable.HashMap[TimingPathKey, TimingPath]()

  def startPhase(timerName: String, sequenceId: Option[Int] = None, isRDDOperation: Boolean = false) {
    val newSequenceId = generateSequenceId(sequenceId, timerName)
    val key = new TimingPathKey(timerName, newSequenceId, isRDDOperation)
    val newPath = if (timingsStack.isEmpty) root(key) else timingsStack.top.child(key)
    timingsStack.push(newPath)
  }

  def finishPhase(timerName: String, timingNanos: Long) {
    val top = timingsStack.pop()
    assert(top.timerName == timerName, "Timer name from on top of stack [" + top +
      "] did not match passed-in timer name [" + timerName + "]")
    accumulable += new RecordedTiming(timingNanos, top)
  }

  def deleteCurrentPhase() {
    timingsStack.pop()
  }

  def copy(): MetricsRecorder = {
    // Calling toList on a stack returns elements in LIFO order so we need to reverse
    // this to get them in FIFO order, which is what the constructor expects
    new MetricsRecorder(accumulable, Some(this.timingsStack.toList.reverse))
  }

  private def root(key: TimingPathKey) = {
    // We use object caching here to try to get the same instance of TimingPath each time. The objective is
    // not to avoid object creation, but to make TimingPath comparisons more efficient when they have
    // many ancestors (TimingPaths can be compared by reference, rather than having to compare all fields).
    // This makes a significant different for TimingPaths with many ancestors. For example, it improves
    // performance by about 1/3 for TimingPaths with 10 ancestors.
    roots.getOrElseUpdate(key, { new TimingPath(key.timerName, None, key.sequenceId, key.isRDDOperation) })
  }

  private def generateSequenceId(sequenceId: Option[Int], timerName: String): Int = {
    // If a sequence ID has been specified explicitly, always use that.
    // Always generate a new sequence ID for top-level operations, as we want to display them in sequence.
    // The exception to this is consecutive operations for the same timer, as these are most likely a loop.
    // For non top-level operations, just return a constant sequence ID.
    if (sequenceId.isDefined) {
      sequenceId.get
    } else {
      val topLevel = timingsStack.isEmpty
      if (topLevel) {
        val newSequenceId = if (timerName != previousTopLevelTimerName) Metrics.generateNewSequenceId() else previousTopLevelSequenceId
        previousTopLevelTimerName = timerName
        previousTopLevelSequenceId = newSequenceId
        newSequenceId
      } else {
        0
      }
    }
  }

  @throws(classOf[java.io.IOException])
  private def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    roots = new mutable.HashMap[TimingPathKey, TimingPath]()
  }

}