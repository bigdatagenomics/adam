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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.adam.instrumentation.{ DurationFormatting, ADAMMetricsListener, ADAMMetrics }
import java.io.{ ByteArrayOutputStream, PrintStream }

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

trait ADAMSparkCommand[A <: Args4jBase with SparkArgs] extends ADAMCommand with SparkCommand with Logging {

  protected val args: A

  def run(sc: SparkContext, job: Job)

  def run() {

    val start = System.nanoTime()

    val metricsListener = if (args.printMetrics) Some(new ADAMMetricsListener(new ADAMMetrics())) else None

    val sc: SparkContext = createSparkContext(args, metricsListener)
    val job = HadoopUtil.newJob()

    run(sc, job)

    val totalTime = System.nanoTime() - start
    printMetrics(totalTime, metricsListener)

  }

  def printMetrics(totalTime: Long, metricsListener: Option[ADAMMetricsListener]) {
    metricsListener.foreach(listener => {
      // Set the output buffer size to 4KB by default
      val bytes = new ByteArrayOutputStream(1024 * 4)
      val out = new PrintStream(bytes)
      out.println()
      out.println()
      out.println("Overall Duration: " + DurationFormatting.formatNanosecondDuration(totalTime))
      out.println()
      listener.adamMetrics.sparkTaskMetrics.print(out)
      logInfo("Metrics:" + bytes.toString("UTF-8"))
    })
  }

}
