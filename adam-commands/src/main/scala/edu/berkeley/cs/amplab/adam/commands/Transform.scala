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

object Transform extends AdamCommandCompanion {
  val commandName = "transform"
  val commandDescription = "Apply one of more transforms to an ADAM file and save the results to another ADAM file"

  def apply(cmdLine: Array[String]) = {
    new Transform(Args4j[TransformArgs](cmdLine))
  }
}

class TransformArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM, BAM or SAM file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the transformed data in ADAM/Parquet format", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-sort_reads", usage = "Sort the reads by referenceId and read position")
  val sortReads: Boolean = false
  @Args4jOption(required = false, name = "-mark_duplicate_reads", usage = "Mark duplicate reads")
  val markDuplicates: Boolean = false
}

class Transform(protected val args: TransformArgs) extends AdamSparkCommand[TransformArgs] with Logging {
  val companion = Transform

  initLogging()

  def run(sc: SparkContext, job: Job) {
    var adamRecords: RDD[ADAMRecord] = sc.adamLoad(args.inputPath)
    if (args.markDuplicates) {
      log.info("Marking duplicates")
      adamRecords = adamRecords.adamMarkDuplicates()
    }
    // NOTE: For now, sorting needs to be the last transform
    if (args.sortReads) {
      log.info("Sorting reads")
      adamRecords = adamRecords.adamSortReadsByReferencePosition()
    }
    adamRecords.adamSave(args.outputPath, args)
  }

}


