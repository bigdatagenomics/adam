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

import edu.berkeley.cs.amplab.adam.util.{ParquetFileTraversable, Args4j, Args4jBase}
import org.kohsuke.args4j.Argument
import edu.berkeley.cs.amplab.adam.avro.ADAMPileup

object Mpileup {
  def main(args: Array[String]) {
    new MpileupCommand().commandExec(args)
  }
}

class MpileupArgs extends Args4jBase with SparkArgs {
  @Argument(required = true, metaVar = "ADAMFILE", usage = "ADAM reference-oriented file", index = 0)
  var rodFile: String = _
  @Argument(required = true, metaVar = "PILEUPFILE", usage = "The location of the pileup file", index = 1)
  var pileupFile: String = _
}

class MpileupCommand extends AdamCommand with SparkCommand {
  val commandName: String = "mpileup"
  val commandDescription: String = "Output the samtool mpileup text from ADAM reference-oriented data"

  def commandExec(cmdLine: Array[String]) {
    var args = Args4j[MpileupArgs](cmdLine)
    var sc = createSparkContext(args)

    val parquetFile = new ParquetFileTraversable[ADAMPileup](sc, args.rodFile)
    for (pileup <- parquetFile) {
      println(pileup)
    }

  }
}
