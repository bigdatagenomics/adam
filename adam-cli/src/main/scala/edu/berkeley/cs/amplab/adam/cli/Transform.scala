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
package edu.berkeley.cs.amplab.adam.cli

import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.util.ParquetLogger
import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import edu.berkeley.cs.amplab.adam.models.SnpTable
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD
import java.io.File
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.rich.RichADAMVariant

object Transform extends AdamCommandCompanion {
  val commandName = "transform"
  val commandDescription = "Convert SAM/BAM to ADAM format and optionally perform read pre-processing transformations"

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
  var sortReads: Boolean = false
  @Args4jOption(required = false, name = "-mark_duplicate_reads", usage = "Mark duplicate reads")
  var markDuplicates: Boolean = false
  @Args4jOption(required = false, name = "-recalibrate_base_qualities", usage = "Recalibrate the base quality scores (ILLUMINA only)")
  var recalibrateBaseQualities: Boolean = false
  @Args4jOption(required = false, name = "-known_snps", usage = "Sites-only VCF giving location of known SNPs")
  var knownSnpsFile: String = null
  @Args4jOption(required = false, name = "-coalesce", usage = "Set the number of partitions written to the ADAM output directory")
  var coalesce: Int = -1
  @Args4jOption(required = false, name = "-realignIndels", usage = "Locally realign indels present in reads.")
  var locallyRealign: Boolean = false
}

class Transform(protected val args: TransformArgs) extends AdamSparkCommand[TransformArgs] with Logging {
  val companion = Transform

  def run(sc: SparkContext, job: Job) {

    // Quiet Parquet...
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    var adamRecords: RDD[ADAMRecord] = sc.adamLoad(args.inputPath)
    if (args.coalesce != -1) {
      log.info("Coalescing the number of partitions to '%d'".format(args.coalesce))
      adamRecords = adamRecords.coalesce(args.coalesce, shuffle = true)
    }

    if (args.markDuplicates) {
      log.info("Marking duplicates")
      adamRecords = adamRecords.adamMarkDuplicates()
    }

    if (args.recalibrateBaseQualities) {
      log.info("Recalibrating base qualities")
      val variants : RDD[RichADAMVariant] = sc.adamVCFLoad(args.knownSnpsFile).map(_.variant)
      val knownSnps = SnpTable(variants)
      adamRecords = adamRecords.adamBQSR(knownSnps)
    }

    if (args.locallyRealign) {
      log.info("Locally realigning indels.")
      adamRecords = adamRecords.adamRealignIndels()
    }

    // NOTE: For now, sorting needs to be the last transform
    if (args.sortReads) {
      log.info("Sorting reads")
      adamRecords = adamRecords.adamSortReadsByReferencePosition()
    }

    adamRecords.adamSave(args.outputPath, blockSize = args.blockSize, pageSize = args.pageSize,
      compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
  }

}
