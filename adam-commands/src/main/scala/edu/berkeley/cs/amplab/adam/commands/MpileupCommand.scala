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

import edu.berkeley.cs.amplab.adam.util._
import org.kohsuke.args4j.Argument
import edu.berkeley.cs.amplab.adam.avro.ADAMPileup
import scala.Some

object Mpileup {
  def main(args: Array[String]) {
    new MpileupCommand().commandExec(args)
  }
}

class MpileupArgs extends Args4jBase with SparkArgs {
  @Argument(required = true, metaVar = "ADAMFILE", usage = "ADAM file", index = 0)
  var file: String = _
}

class MpileupCommand extends AdamCommand with SparkCommand {
  val commandName: String = "mpileup"
  val commandDescription: String = "Output the samtool mpileup text from ADAM reference-oriented data"

  def commandExec(cmdLine: Array[String]) {
    val args = Args4j[MpileupArgs](cmdLine)
    val sc = createSparkContext(args)

    val pileups = new PileupTraversable(sc, args.file)
    for (pileup <- pileups) {
      // Reference name and position
      print("%s %s ".format(pileup.referenceName, pileup.referencePosition))

      // The reference base
      pileup.referenceBase match {
        case Some(base) => print(base)
        case None => print("?")
      }

      // The number of reads
      print(" " + pileup.numReads + " ")

      // Matches
      for (matchEvent <- pileup.matches)
        if (matchEvent.isReverseStrand) print(",") else print(".")

      // Mismatches
      for (mismatchEvent <- pileup.mismatches) {
        val mismatchBase = mismatchEvent.mismatchedBase.toString
        if (mismatchEvent.isReverseStrand) print(mismatchBase.toLowerCase) else print(mismatchBase)
      }

      // Deletes
      for (deletedEvent <- pileup.deletes)
        print("-1" + pileup.referenceBase.get)

      // Inserts
      for (insertEvent <- pileup.insertions)
        print("+" + insertEvent.insertedSequence.length + insertEvent.insertedSequence)

      println()
    }

  }
}
