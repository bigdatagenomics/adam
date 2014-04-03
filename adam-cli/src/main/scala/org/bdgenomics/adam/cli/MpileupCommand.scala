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

package org.bdgenomics.adam.cli

import org.bdgenomics.adam.util._
import org.kohsuke.args4j.Argument
import scala.Some
import org.apache.spark.{ SparkContext, Logging }
import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.predicates.LocusPredicate

object MpileupCommand extends ADAMCommandCompanion {
  val commandName: String = "mpileup"
  val commandDescription: String = "Output the samtool mpileup text from ADAM reference-oriented data"

  def apply(cmdLine: Array[String]) = {
    new MpileupCommand(Args4j[MpileupArgs](cmdLine))
  }
}

class MpileupArgs extends Args4jBase with SparkArgs {
  @Argument(metaVar = "ADAMREADS", required = true, usage = "ADAM read-oriented data", index = 0)
  var readInput: String = _

  spark_master = "local"
}

class MpileupCommand(protected val args: MpileupArgs) extends ADAMSparkCommand[MpileupArgs] with Logging {
  val companion = MpileupCommand

  def run(sc: SparkContext, job: Job) {

    val reads: RDD[ADAMRecord] = sc.adamLoad(args.readInput, Some(classOf[LocusPredicate]))

    val pileups = new PileupTraversable(reads)
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
