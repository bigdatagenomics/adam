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
import org.bdgenomics.adam.util.SparkFunSuite
import org.scalatest.BeforeAndAfterAll

class ServoTimersAccumulableParamSuite extends SparkFunSuite with BeforeAndAfterAll {

  sparkTest("Values accumulated correctly") {

    implicit val accumulableParam = new ServoTimersAccumulableParam()

    var accumulable: Accumulable[ServoTimers, RecordedTiming] = sc.accumulable(new ServoTimers())

    val root = new TimingPath("Timer 1", None)
    val child1 = new TimingPath("Timer 2", Some(root))
    val child2 = new TimingPath("Timer 3", Some(root))
    // This has the same ID as child2, but has (some) different ancestors
    val grandchild1 = new TimingPath("Timer 2", Some(child1))

    // First, record a single timing for each timer
    accumulable += new RecordedTiming(100000, root)
    accumulable += new RecordedTiming(200000, child1)
    accumulable += new RecordedTiming(300000, child2)
    accumulable += new RecordedTiming(400000, grandchild1)
    var timerMap = accumulable.value.timerMap
    assert(timerMap.size() === 4)
    assert(timerMap.get(root).getTotalTime === 100000)
    assert(timerMap.get(root).getName === "Timer 1")
    assert(timerMap.get(child1).getTotalTime === 200000)
    assert(timerMap.get(child1).getName === "Timer 2")
    assert(timerMap.get(child2).getTotalTime === 300000)
    assert(timerMap.get(child2).getName === "Timer 3")
    assert(timerMap.get(grandchild1).getTotalTime === 400000)
    assert(timerMap.get(grandchild1).getName === "Timer 2")

    // Record a new timing for two of the existing timers. No new timers should be created.
    accumulable += new RecordedTiming(50000, root)
    accumulable += new RecordedTiming(50000, child2)
    timerMap = accumulable.value.timerMap
    assert(timerMap.size() === 4)
    assert(timerMap.get(root).getTotalTime === 150000)
    assert(timerMap.get(root).getCount === 2)
    assert(timerMap.get(child2).getTotalTime === 350000)
    assert(timerMap.get(child2).getCount === 2)

  }

  sparkTest("Paths with different properties are treated as distinct timers") {

    implicit val accumulableParam = new ServoTimersAccumulableParam()

    var accumulable: Accumulable[ServoTimers, RecordedTiming] = sc.accumulable(new ServoTimers())

    val root = new TimingPath("Timer 1", None)
    // These children all have the same timer ID and parent but have different sequence IDs
    // and isRDDOperation flags
    val child1 = new TimingPath("Timer 2", Some(root), sequenceId = 101)
    val child2 = new TimingPath("Timer 3", Some(root), sequenceId = 102)
    val child3 = new TimingPath("Timer 4", Some(root), sequenceId = 101, isRDDOperation = true)

    // We should get four distinct timers here
    accumulable += new RecordedTiming(100000, root)
    accumulable += new RecordedTiming(200000, child1)
    accumulable += new RecordedTiming(300000, child2)
    accumulable += new RecordedTiming(400000, child3)
    val timerMap = accumulable.value.timerMap
    assert(timerMap.size() === 4)
    assert(timerMap.get(root).getTotalTime === 100000)
    assert(timerMap.get(root).getName === "Timer 1")
    assert(timerMap.get(child1).getTotalTime === 200000)
    assert(timerMap.get(child1).getName === "Timer 2")
    assert(timerMap.get(child2).getTotalTime === 300000)
    assert(timerMap.get(child2).getName === "Timer 3")
    assert(timerMap.get(child3).getTotalTime === 400000)
    assert(timerMap.get(child3).getName === "Timer 4")

  }

  sparkTest("Accumulable params can be merged together") {

    implicit val accumulableParam = new ServoTimersAccumulableParam()

    val root = new TimingPath("Timer 1", None)
    val child1 = new TimingPath("Timer 2", Some(root))
    val child2 = new TimingPath("Timer 3", Some(root))
    // This has the same ID as child2, but has (some) different ancestors
    val grandchild1 = new TimingPath("Timer 2", Some(child1))

    // Accumulable 1 contains 3 distinct timers
    var accumulable1: Accumulable[ServoTimers, RecordedTiming] = sc.accumulable(new ServoTimers())
    accumulable1 += new RecordedTiming(100000, root)
    accumulable1 += new RecordedTiming(200000, child1)
    accumulable1 += new RecordedTiming(300000, child2)

    // Accumulable 2 contains 1 new timer, and 2 additions to accumulable 1
    var accumulable2: Accumulable[ServoTimers, RecordedTiming] = sc.accumulable(new ServoTimers())
    accumulable2 += new RecordedTiming(400000, grandchild1)
    accumulable2 += new RecordedTiming(50000, root)
    accumulable2 += new RecordedTiming(50000, child2)

    accumulable1.merge(accumulable2.value)

    val timerMap = accumulable1.value.timerMap
    assert(timerMap.size() === 4)
    assert(timerMap.get(root).getTotalTime === 150000)
    assert(timerMap.get(root).getName === "Timer 1")
    assert(timerMap.get(child1).getTotalTime === 200000)
    assert(timerMap.get(child1).getName === "Timer 2")
    assert(timerMap.get(child2).getTotalTime === 350000)
    assert(timerMap.get(child2).getName === "Timer 3")
    assert(timerMap.get(grandchild1).getTotalTime === 400000)
    assert(timerMap.get(grandchild1).getName === "Timer 2")

  }

}
