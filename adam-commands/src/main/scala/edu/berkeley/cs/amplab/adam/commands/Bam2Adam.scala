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

import org.apache.hadoop.mapreduce.Job
import spark.{RDD, SparkContext, Logging}
import edu.berkeley.cs.amplab.adam.util.{Args4jBase, Args4j}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

object Bam2Adam extends AdamCommandCompanion {

  val commandName = "bam2adam"
  val commandDescription = "Convert a SAM/BAM file to ADAM read-oriented format"

  def apply(cmdLine: Array[String]) = {
    new Bam2Adam(Args4j[Bam2AdamArgs](cmdLine))
  }

}

class Bam2AdamArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "BAM", usage = "The SAM or BAM file to convert", index = 0)
  var bamFile: String = null
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM data", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-sort", usage = "Sort the reads by referenceId and read position")
  val sortReads: Boolean = false
  @Args4jOption(required = false, name = "-mark_duplicates", usage = "Mark the duplicate reads")
  val markDuplicates: Boolean = false
  @Args4jOption(required = false, name = "-single_partition", usage = "Write a single partition")
  val singlePartition: Boolean = false
}

class Bam2Adam(protected val args: Bam2AdamArgs) extends AdamSparkCommand[Bam2AdamArgs] with Logging {
  val companion = Bam2Adam

  initLogging()

  def run(sc: SparkContext, job: Job) {
    var adamRecords: RDD[ADAMRecord] = sc.adamLoad(args.bamFile)
    if (args.sortReads) {
      log.info("Sorting reads")
      adamRecords = adamRecords.adamSortReadsByReferencePosition()
    }
    if (args.markDuplicates) {
      log.info("Marking duplicates")
      adamRecords = adamRecords.adamMarkDuplicates()
    }
    if (args.singlePartition) {
      log.info("Writing output to a single partition")
      adamRecords = adamRecords.coalesce(1, shuffle = true)
    }
    adamRecords.adamSave(args.outputPath, args)
  }

}


