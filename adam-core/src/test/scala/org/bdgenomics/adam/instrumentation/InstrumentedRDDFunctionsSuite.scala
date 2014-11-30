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
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd._
import org.bdgenomics.adam.instrumentation.InstrumentationTestingUtil._
import org.bdgenomics.adam.util.SparkFunSuite
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class InstrumentedRDDFunctionsSuite extends SparkFunSuite {

  implicit def rddToInstrumentedRDD[T](rdd: RDD[T]) = new MyInstrumentedRDDFunctions(rdd)

  sparkTest("Operation is recorded correctly") {
    Metrics.initialize(sc)
    val testingClock = new TestingClock()
    val instrumentedRddFunctions = new TestingInstrumentedRDDFunctions(testingClock)
    implicit val implicitSc = sc
    instrumentedRddFunctions.recordOperation({ testingClock.currentTime += 300000 })
    val timers = Metrics.Recorder.value.get.accumulable.value.timerMap
    assert(timers.size() === 1)
    assert(timers.values().iterator().next().getTotalTime === 300000)
    assert(timers.keys().nextElement().isRDDOperation === true)
    assert(timers.values().iterator().next().getName contains "recordOperation at InstrumentedRDDFunctionsSuite.scala")
  }

  sparkTest("Function call is recorded correctly") {
    Metrics.initialize(sc)
    val testingClock = new TestingClock()
    val instrumentedRddFunctions = new TestingInstrumentedRDDFunctions(testingClock)
    val functionTimer = new Timer("Function Timer", clock = testingClock)
    val recorder = instrumentedRddFunctions.metricsRecorder()
    assert(recorder ne Metrics.Recorder.value) // This should be a copy
    // Use a new thread to check that the recorder is propagated properly (as opposed to just using this thread's
    // thread-local value.
    val thread = new Thread() {
      override def run() {
        // DynamicVariables use inheritable thread locals, so set this to the default value to simulate
        // not having created the thread
        Metrics.Recorder.value = None
        instrumentedRddFunctions.recordFunction(myFunction(testingClock), recorder, functionTimer)
      }
    }
    thread.start()
    thread.join()
    val timers = recorder.get.accumulable.value.timerMap
    assert(timers.values().iterator().next().getTotalTime === 200000)
    assert(timers.keys().nextElement().isRDDOperation === false)
    assert(timers.values().iterator().next().getName contains "Function Timer")
  }

  sparkTest("Persisting to Hadoop file is instrumented correctly") {
    Metrics.initialize(sc)
    val testingClock = new TestingClock()
    val instrumentedRddFunctions = new TestingInstrumentedRDDFunctions(testingClock)
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 2).keyBy(e => e)
    val tempDir = new File(System.getProperty("java.io.tmpdir"), "hadoopfiletest")
    if (tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir)
    }
    // We need to call recordOperation here or the timing stat ends up as a top-level stat, which has a unique
    // sequence Id
    implicit val implicitSc = sc
    instrumentedRddFunctions.recordOperation {
      instrumentedRddFunctions.instrumentedSaveAsNewAPIHadoopFile(rdd, tempDir.getAbsolutePath, classOf[java.lang.Long],
        classOf[java.lang.Long], classOf[MyInstrumentedOutputFormat], new Configuration())
    }
    val timers = Metrics.Recorder.value.get.accumulable.value.timerMap.filter(!_._1.isRDDOperation).toSeq
    assert(timers.size === 1)
    assert(timers.iterator.next()._2.getName === "Write Record Timer")
    assert(timers.iterator.next()._2.getCount === 5)
    FileUtils.deleteDirectory(tempDir)
  }

  sparkTest("Instrumenting Spark operation works correctly") {
    Metrics.initialize(sc)
    val rdd = sc.parallelize(List.range(1, 11), 2)
    rdd.instrumentedMap(e => {
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
    assertOnNameAndCountInTimingsTable(dataRows.get(1), "recordOperation at InstrumentedRDDFunctionsSuite.scala", 1)
    assertOnNameAndCountInTimingsTable(dataRows.get(2), "map function", 10)
    assertOnNameAndCountInTimingsTable(dataRows.get(3), "timer 1", 10)
    assertOnNameAndCountInTimingsTable(dataRows.get(4), "timer 2", 10)
  }

  def myFunction(testingClock: TestingClock): Boolean = {
    testingClock.currentTime += 200000
    true
  }

  class TestingInstrumentedRDDFunctions[T](testingClock: Clock) extends InstrumentedRDDFunctions(testingClock) {}

}

class MyInstrumentedRDDFunctions[T](self: RDD[T]) extends InstrumentedRDDFunctions() {
  def instrumentedMap[U: ClassTag](f: T => U): RDD[U] = {
    recordOperation {
      val recorder = metricsRecorder()
      self.map((t: T) => { recordFunction(f(t), recorder, FunctionTimers.MapFunction) })
    }
  }
}

object FunctionTimers extends Metrics {
  val MapFunction = timer("map function")
}

object OtherTimers extends Metrics {
  val Timer1 = timer("timer 1")
  val Timer2 = timer("timer 2")
}

class MyInstrumentedOutputFormat extends InstrumentedOutputFormat[Long, Long] {
  override def timerName(): String = "Write Record Timer"
  override def outputFormatClass(): Class[_ <: OutputFormat[Long, Long]] = classOf[TextOutputFormat[Long, Long]]
}
