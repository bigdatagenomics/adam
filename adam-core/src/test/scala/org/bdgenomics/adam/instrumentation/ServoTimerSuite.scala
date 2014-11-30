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

import com.netflix.servo.tag.{ Tag, Tags }
import java.util.concurrent.TimeUnit
import org.bdgenomics.adam.instrumentation.ServoTimer._
import org.scalatest.FunSuite
import scala.collection.JavaConversions._

class ServoTimerSuite extends FunSuite {

  test("Total time computed correctly") {
    val timer = createTimer()
    timer.recordMillis(100)
    timer.recordMillis(200)
    assert(fromNanos(timer.getTotalTime) === 300)
    assert(fromNanos(timer.getValue.asInstanceOf[Number].longValue()) === 300)
    assert(fromNanos(getTaggedValue(timer, TotalTimeTag).longValue()) === 300)
  }

  test("Count computed correctly") {
    val timer = createTimer()
    timer.recordMillis(100)
    timer.recordMillis(200)
    assert(timer.getCount === 2)
    assert(getTaggedValue(timer, CountTag).longValue() === 2)
  }

  test("Mean computed correctly") {
    val timer = createTimer()
    timer.recordMillis(100)
    timer.recordMillis(200)
    assert(fromNanos(timer.getMean) === 150)
    assert(fromNanos(getTaggedValue(timer, MeanTag).longValue()) === 150)
  }

  test("Max computed correctly") {
    val timer = createTimer()
    timer.recordMillis(100)
    timer.recordMillis(201)
    timer.recordMillis(200)
    assert(fromNanos(timer.getMax) === 201)
    assert(fromNanos(getTaggedValue(timer, MaxTag).longValue()) === 201)
  }

  test("Min computed correctly") {
    val timer = createTimer()
    timer.recordMillis(100)
    timer.recordMillis(201)
    timer.recordMillis(99)
    timer.recordMillis(200)
    assert(fromNanos(timer.getMin) === 99)
    assert(fromNanos(getTaggedValue(timer, MinTag).longValue()) === 99)
  }

  test("Timer and sub-monitors tagged correctly") {
    val timer = createTimer()
    assert(timer.getConfig.getTags.getValue("myTag") === null)
    assert(timer.getMonitors.get(0).getConfig.getTags.getValue("myTag") === null)
    timer.addTag(Tags.newTag("myTag", "tagValue"))
    assert(timer.getConfig.getTags.getValue("myTag") === "tagValue")
  }

  test("Nanosecond timings recorded correctly") {
    val timer = createTimer()
    timer.recordNanos(10000)
    timer.recordNanos(20000)
    assert(timer.getTotalTime === 30000)
    assert(timer.getValue.asInstanceOf[Number].longValue() === 30000)
    assert(getTaggedValue(timer, TotalTimeTag).longValue() === 30000)
  }

  def fromNanos(value: Long): Long = {
    TimeUnit.MILLISECONDS.convert(value, TimeUnit.NANOSECONDS)
  }

  def getTaggedValue(timer: ServoTimer, tag: Tag): Number = {
    timer.getMonitors.foreach(monitor => {
      val tagValue = monitor.getConfig.getTags.getValue(tag.getKey)
      if (tagValue != null && tagValue.equals(tag.getValue)) {
        monitor.getValue match {
          case value: Number => return value
        }
      }
    })
    fail("Could not find monitor with tag [" + tag + "] and numeric value")
  }

  def createTimer(): ServoTimer = {
    new ServoTimer("testTimer")
  }

}
