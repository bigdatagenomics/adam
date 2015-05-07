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
package org.bdgenomics.adam.cli

import java.io.{ StringWriter, PrintWriter }

import org.apache.spark.{ SparkConf, Logging, SparkContext }
import org.bdgenomics.utils.instrumentation._

trait ADAMCommandCompanion {
  val commandName: String
  val commandDescription: String

  def apply(cmdLine: Array[String]): ADAMCommand

  // Make running an ADAM command easier from an IDE
  def main(cmdLine: Array[String]) {
    apply(cmdLine).run()
  }
}

trait ADAMCommand extends Runnable {
  val companion: ADAMCommandCompanion
}

trait ADAMSparkCommand[A <: Args4jBase] extends ADAMCommand with Logging {
  protected val args: A

  def run(sc: SparkContext): Unit

  def run() {
    val start = System.nanoTime()
    val conf = new SparkConf().setAppName("adam: " + companion.commandName)
    if (conf.getOption("spark.master").isEmpty) {
      conf.setMaster("local[%d]".format(Runtime.getRuntime.availableProcessors()))
    }
    val sc = new SparkContext(conf)
    val metricsListener = initializeMetrics(sc)
    run(sc)
    val totalTime = System.nanoTime() - start
    printMetrics(totalTime, metricsListener)
  }

  def initializeMetrics(sc: SparkContext): Option[MetricsListener] = {
    if (args.printMetrics) {
      val metricsListener = new MetricsListener(new RecordedMetrics())
      sc.addSparkListener(metricsListener)
      Metrics.initialize(sc)
      Some(metricsListener)
    } else {
      // This avoids recording metrics if we have a recorder left over from previous use of this thread
      Metrics.stopRecording()
      None
    }
  }

  def printMetrics(totalTime: Long, metricsListener: Option[MetricsListener]) {
    logInfo("Overall Duration: " + DurationFormatting.formatNanosecondDuration(totalTime))
    if (args.printMetrics && metricsListener.isDefined) {
      // Set the output buffer size to 4KB by default
      val stringWriter = new StringWriter()
      val out = new PrintWriter(stringWriter)
      out.println("Metrics:")
      out.println()
      Metrics.print(out, Some(metricsListener.get.metrics.sparkMetrics.stageTimes))
      out.println()
      metricsListener.get.metrics.sparkMetrics.print(out)
      out.flush()
      logInfo(stringWriter.getBuffer.toString)
    }
  }

}
