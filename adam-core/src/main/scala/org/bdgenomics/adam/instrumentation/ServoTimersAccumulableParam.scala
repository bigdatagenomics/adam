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

import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.AccumulableParam
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Implementation of [[AccumulableParam]] that records timings and returns a [[ServoTimers]] with the accumulated timings.
 */
class ServoTimersAccumulableParam extends AccumulableParam[ServoTimers, RecordedTiming] {
  override def addAccumulator(timers: ServoTimers, newTiming: RecordedTiming): ServoTimers = {
    timers.recordTiming(newTiming)
    timers
  }
  override def zero(initialValue: ServoTimers): ServoTimers = {
    new ServoTimers()
  }
  override def addInPlace(timers1: ServoTimers, timers2: ServoTimers): ServoTimers = {
    timers1.merge(timers2)
    timers1
  }
}

/**
 * Holds a collection of [[ServoTimer]]s. Each instance of a timer is stored against a [[TimingPath]], which
 * specifies all of its ancestors. Timings can be recorded using the `recordTiming` method, which will
 * either update an existing timer if the specified [[TimingPath]] exists already, or will create a new timer.
 */
class ServoTimers extends Serializable {

  val timerMap = new ConcurrentHashMap[TimingPath, ServoTimer]()

  def recordTiming(timing: RecordedTiming) = {
    val servoTimer = timerMap.getOrElseUpdate(timing.pathToRoot, createServoTimer(timing.pathToRoot.timerName))
    servoTimer.recordNanos(timing.timingNanos)
  }

  def merge(servoTimers: ServoTimers) {
    servoTimers.timerMap.foreach(entry => {
      val existing = this.timerMap.get(entry._1)
      if (existing != null) {
        existing.merge(entry._2)
      } else {
        this.timerMap.put(entry._1, entry._2)
      }
    })
  }

  private def createServoTimer(timerName: String): ServoTimer = {
    new ServoTimer(timerName)
  }

}

/**
 * Specifies a timing that is to recorded
 */
case class RecordedTiming(timingNanos: Long, pathToRoot: TimingPath) extends Serializable

/**
 * Specifies a timer name, along with all of its ancestors.
 */
class TimingPath(val timerName: String, val parentPath: Option[TimingPath], val sequenceId: Int = 0,
                 val isRDDOperation: Boolean = false) extends Serializable {

  val depth = computeDepth()

  @transient private var children = new mutable.HashMap[TimingPathKey, TimingPath]()

  // We pre-calculate the hash code here since we know we will need it (since the main purpose of TimingPaths
  // is to be used as a key in a map). Since the hash code of a TimingPath is calculated recursively using
  // its ancestors, this should save some re-computation for paths with many ancestors.
  private val cachedHashCode = computeHashCode()

  override def equals(other: Any): Boolean = other match {
    case that: TimingPath =>
      // It's quite likely that this will succeed, since we try to cache TimingPath objects and reuse them
      if (this eq that) {
        true
      }
      // This is ordered with timerName first, as that is likely to be a much cheaper comparison
      // and is likely to identify a TimingPath uniquely most of the time (String.equals checks
      // for reference equality, and since timer names are likely to be interned this should be cheap).
      timerName == that.timerName && otherFieldsEqual(that) &&
        (if (parentPath.isDefined) that.parentPath.isDefined && parentPath.get.equals(that.parentPath.get)
        else !that.parentPath.isDefined)
    case _ => false
  }

  override def hashCode(): Int = {
    cachedHashCode
  }

  override def toString: String = {
    (if (parentPath.isDefined) parentPath.get.toString() else "") + "/" + timerName +
      "(" + sequenceId + "," + isRDDOperation + ")"
  }

  /**
   * Gets a [[TimingPath]] for the specified [[TimingPathKey]]. This is preferable to creating [[TimingPath]]s
   * directly, as it re-uses objects, thus making equality comparisons faster (objects can be compared by reference)
   */
  def child(key: TimingPathKey): TimingPath = {
    children.getOrElseUpdate(key, { new TimingPath(key.timerName, Some(this), key.sequenceId, key.isRDDOperation) })
  }

  private def otherFieldsEqual(that: TimingPath): Boolean = {
    sequenceId == that.sequenceId && isRDDOperation == that.isRDDOperation
  }

  private def computeDepth(): Int = {
    if (parentPath.isDefined) parentPath.get.depth + 1 else 0
  }

  private def computeHashCode(): Int = {
    var result = 23
    result = 37 * result + timerName.hashCode()
    result = 37 * result + sequenceId
    result = 37 * result + (if (isRDDOperation) 1 else 0)
    result = 37 * result + (if (parentPath.isDefined) parentPath.hashCode() else 0)
    result
  }

  @throws(classOf[java.io.IOException])
  private def readObject(in: java.io.ObjectInputStream): Unit = {
    in.defaultReadObject()
    children = new mutable.HashMap[TimingPathKey, TimingPath]()
  }

}

class TimingPathKey(val timerName: String, val sequenceId: Int, val isRDDOperation: Boolean) {
  private val cachedHashCode = computeHashCode()
  override def equals(other: Any): Boolean = other match {
    case that: TimingPathKey =>
      timerName == that.timerName && sequenceId == that.sequenceId && isRDDOperation == that.isRDDOperation
    case _ => false
  }
  private def computeHashCode(): Int = {
    var result = 23
    result = 37 * result + timerName.hashCode()
    result = 37 * result + sequenceId
    result = 37 * result + (if (isRDDOperation) 1 else 0)
    result
  }
  override def hashCode(): Int = {
    cachedHashCode
  }
}