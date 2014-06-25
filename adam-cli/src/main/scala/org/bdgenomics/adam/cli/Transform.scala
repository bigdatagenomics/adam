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
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.bdgenomics.adam.models.SnpTable
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.RichADAMVariant

object Transform extends ADAMCommandCompanion {
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
  @Args4jOption(required = false, name = "-realignIndels", usage = "Locally realign indels present in reads.")
  var locallyRealign: Boolean = false
  @Args4jOption(required = false, name = "-trimReads", usage = "Apply a fixed trim to the prefix and suffix of all reads/reads in a specific read group.")
  var trimReads: Boolean = false
  @Args4jOption(required = false, name = "-trimFromStart", usage = "Trim to be applied to start of read.")
  var trimStart: Int = 0
  @Args4jOption(required = false, name = "-trimFromEnd", usage = "Trim to be applied to end of read.")
  var trimEnd: Int = 0
  @Args4jOption(required = false, name = "-trimReadGroup", usage = "Read group to be trimmed. If omitted, all reads are trimmed.")
  var trimReadGroup: Int = -1
  @Args4jOption(required = false, name = "-qualityBasedTrim", usage = "Trims reads based on quality scores of prefix/suffixes across read group.")
  var qualityBasedTrim: Boolean = false
  @Args4jOption(required = false, name = "-qualityThreshold", usage = "Phred scaled quality threshold used for trimming. If omitted, Phred 20 is used.")
  var qualityThreshold: Int = 20
  @Args4jOption(required = false, name = "-trimBeforeBQSR", usage = "Performs quality based trim before running BQSR. Default is to run quality based trim after BQSR.")
  var trimBeforeBQSR: Boolean = false
}

class Transform(protected val args: TransformArgs) extends ADAMSparkCommand[TransformArgs] with Logging {
  val companion = Transform

  def run(sc: SparkContext, job: Job) {

    var adamRecords: RDD[ADAMRecord] = sc.adamLoad(args.inputPath)

    if (args.repartition != -1) {
      log.info("Repartitioning reads to to '%d' partitions".format(args.repartition))
      adamRecords = adamRecords.repartition(args.repartition)
    }

    if (args.trimReads) {
      log.info("Trimming reads.")
      adamRecords = adamRecords.adamTrimReads(args.trimStart, args.trimEnd, args.trimReadGroup)
    }

    if (args.qualityBasedTrim && args.trimBeforeBQSR) {
      log.info("Applying quality based trim.")
      adamRecords = adamRecords.adamTrimLowQualityReadGroups(args.qualityThreshold)
    }

    if (args.markDuplicates) {
      log.info("Marking duplicates")
      adamRecords = adamRecords.adamMarkDuplicates()
    }

    if (args.recalibrateBaseQualities) {
      log.info("Recalibrating base qualities")
      val variants: RDD[RichADAMVariant] = sc.adamVCFLoad(args.knownSnpsFile).map(_.variant)
      val knownSnps = SnpTable(variants)
      adamRecords = adamRecords.adamBQSR(sc.broadcast(knownSnps))
    }

    if (args.qualityBasedTrim && !args.trimBeforeBQSR) {
      log.info("Applying quality based trim.")
      adamRecords = adamRecords.adamTrimLowQualityReadGroups(args.qualityThreshold)
    }

    if (args.locallyRealign) {
      log.info("Locally realigning indels.")
      adamRecords = adamRecords.adamRealignIndels()
    }

    if (args.coalesce != -1) {
      log.info("Coalescing the number of partitions to '%d'".format(args.coalesce))
      adamRecords = adamRecords.coalesce(args.coalesce, shuffle = true)
    }

    // NOTE: For now, sorting needs to be the last transform
    if (args.sortReads) {
      log.info("Sorting reads")
      adamRecords = adamRecords.adamSortReadsByReferencePosition()
    }

    if (args.outputPath.endsWith(".sam")) {
      log.info("Saving data in SAM format")
      adamRecords.adamSAMSave(args.outputPath)
    } else if (args.outputPath.endsWith(".bam")) {
      log.info("Saving data in BAM format")
      adamRecords.adamSAMSave(args.outputPath, false)
    } else {
      log.info("Saving data in ADAM format")
      adamRecords.adamSave(args.outputPath, blockSize = args.blockSize, pageSize = args.pageSize,
        compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
    }
  }

}
