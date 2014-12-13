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

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.{ InstrumentedOutputFormat, InstrumentedRDD }
import org.bdgenomics.adam.util.SparkFunSuite
import org.apache.spark.rdd.InstrumentedRDD._
import org.apache.spark.rdd.MetricsContext._
import scala.collection.JavaConversions._

class InstrumentedPairRDDFunctionsSuite extends SparkFunSuite {

  sparkBefore("Before") {
    Metrics.initialize(sc)
  }

  sparkAfter("After") {
    Metrics.stopRecording()
  }

  sparkTest("Persisting to Hadoop file is instrumented correctly") {
    val testingClock = new TestingClock()
    InstrumentedRDD.clock = testingClock
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 2).keyBy(e => e).instrument()
    val tempDir = new File(System.getProperty("java.io.tmpdir"), "hadoopfiletest")
    if (tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir)
    }
    // We need to call recordOperation here or the timing stat ends up as a top-level stat, which has a unique
    // sequence Id
    implicit val implicitSc = sc
    recordOperation {
      rdd.saveAsNewAPIHadoopFile(tempDir.getAbsolutePath, classOf[java.lang.Long],
        classOf[java.lang.Long], classOf[MyInstrumentedOutputFormat], new Configuration())
    }
    val timers = Metrics.Recorder.value.get.accumulable.value.timerMap.filter(!_._1.isRDDOperation).toSeq
    assert(timers.size === 1)
    assert(timers.iterator.next()._2.getName === "Write Record Timer")
    assert(timers.iterator.next()._2.getCount === 5)
    FileUtils.deleteDirectory(tempDir)
  }

}

class MyInstrumentedOutputFormat extends InstrumentedOutputFormat[Long, Long] {
  override def timerName(): String = "Write Record Timer"
  override def outputFormatClass(): Class[_ <: OutputFormat[Long, Long]] = classOf[TextOutputFormat[Long, Long]]
}
