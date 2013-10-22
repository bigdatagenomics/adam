/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.commands

import spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.util.Args4jBase

trait AdamCommandCompanion {
  val commandName: String
  val commandDescription: String

  def apply(cmdLine: Array[String]): AdamCommand

  // Make running an ADAM command easier from an IDE
  def main(cmdLine: Array[String]) {
    apply(cmdLine).run()
  }
}

trait AdamCommand extends Runnable {
  val companion: AdamCommandCompanion
}

trait AdamSparkCommand[A <: Args4jBase with SparkArgs] extends AdamCommand with SparkCommand {

  protected val args: A

  def run(sc: SparkContext, job: Job)

  def run() {
    val sc: SparkContext = createSparkContext(args)
    val job = new Job()

    run(sc, job)
  }
}
