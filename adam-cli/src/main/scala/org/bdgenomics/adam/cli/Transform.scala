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

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.fs.{ FileStatus, Path, FileSystem }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.algorithms.consensus._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.SnpTable
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Transform extends BDGCommandCompanion {
  val commandName = "transform"
  val commandDescription = "Convert SAM/BAM to ADAM format and optionally perform read pre-processing transformations"

  def apply(cmdLine: Array[String]) = {
    new Transform(Args4j[TransformArgs](cmdLine))
  }
}

class TransformArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
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
  @Args4jOption(required = false, name = "-strict_bqsr", usage = "Run BQSR with strict validation.")
  var strictBQSR: Boolean = false
  @Args4jOption(required = false, name = "-dump_observations", usage = "Local path to dump BQSR observations to. Outputs CSV format.")
  var observationsPath: String = null
  @Args4jOption(required = false, name = "-known_snps", usage = "Sites-only VCF giving location of known SNPs")
  var knownSnpsFile: String = null
  @Args4jOption(required = false, name = "-realign_indels", usage = "Locally realign indels present in reads.")
  var locallyRealign: Boolean = false
  @Args4jOption(required = false, name = "-known_indels", usage = "VCF file including locations of known INDELs. If none is provided, default consensus model will be used.")
  var knownIndelsFile: String = null
  @Args4jOption(required = false, name = "-max_indel_size", usage = "The maximum length of an INDEL to realign to. Default value is 500.")
  var maxIndelSize = 500
  @Args4jOption(required = false, name = "-max_consensus_number", usage = "The maximum number of consensus to try realigning a target region to. Default value is 30.")
  var maxConsensusNumber = 30
  @Args4jOption(required = false, name = "-log_odds_threshold", usage = "The log-odds threshold for accepting a realignment. Default value is 5.0.")
  var lodThreshold = 5.0
  @Args4jOption(required = false, name = "-max_target_size", usage = "The maximum length of a target region to attempt realigning. Default length is 3000.")
  var maxTargetSize = 3000
  @Args4jOption(required = false, name = "-repartition", usage = "Set the number of partitions to map data to")
  var repartition: Int = -1
  @Args4jOption(required = false, name = "-coalesce", usage = "Set the number of partitions written to the ADAM output directory")
  var coalesce: Int = -1
  @Args4jOption(required = false, name = "-sort_fastq_output", usage = "Sets whether to sort the FASTQ output, if saving as FASTQ. False by default. Ignored if not saving as FASTQ.")
  var sortFastqOutput: Boolean = false
  @Args4jOption(required = false, name = "-force_load_bam", usage = "Forces Transform to load from BAM/SAM.")
  var forceLoadBam: Boolean = false
  @Args4jOption(required = false, name = "-force_load_fastq", usage = "Forces Transform to load from unpaired FASTQ.")
  var forceLoadFastq: Boolean = false
  @Args4jOption(required = false, name = "-force_load_ifastq", usage = "Forces Transform to load from interleaved FASTQ.")
  var forceLoadIFastq: Boolean = false
  @Args4jOption(required = false, name = "-force_load_parquet", usage = "Forces Transform to load from Parquet.")
  var forceLoadParquet: Boolean = false
  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false
  @Args4jOption(required = false, name = "-recursive", usage = "Recursively search <INPUT> directory for files with extension matching the argument to -input_extension, and convert them to extension <OUTPUT>")
  var recursive: Boolean = false
  @Args4jOption(required = false, name = "-input_extension", usage = "When -recursive is enabled, look in the <INPUT> directory for files with this extension. Can include leading '.' or not.")
  var inputExtension: String = null
}

class Transform(protected val args: TransformArgs) extends BDGSparkCommand[TransformArgs] with Logging {
  val companion = Transform

  def apply(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {

    var adamRecords = rdd
    val sc = rdd.context

    if (args.repartition != -1) {
      log.info("Repartitioning reads to to '%d' partitions".format(args.repartition))
      adamRecords = adamRecords.repartition(args.repartition)
    }

    if (args.markDuplicates) {
      log.info("Marking duplicates")
      adamRecords = adamRecords.adamMarkDuplicates()
    }

    if (args.locallyRealign) {
      log.info("Locally realigning indels.")
      val consensusGenerator = Option(args.knownIndelsFile)
        .fold(new ConsensusGeneratorFromReads().asInstanceOf[ConsensusGenerator])(
          new ConsensusGeneratorFromKnowns(_, sc).asInstanceOf[ConsensusGenerator])

      adamRecords = adamRecords.adamRealignIndels(
        consensusGenerator,
        isSorted = false,
        args.maxIndelSize,
        args.maxConsensusNumber,
        args.lodThreshold,
        args.maxTargetSize)
    }

    if (args.recalibrateBaseQualities) {
      log.info("Recalibrating base qualities")
      val knownSnps: SnpTable = createKnownSnpsTable(sc)
      val stringency = if (args.strictBQSR) {
        ValidationStringency.STRICT
      } else {
        ValidationStringency.LENIENT
      }
      adamRecords = adamRecords.adamBQSR(sc.broadcast(knownSnps),
        Option(args.observationsPath),
        stringency)
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

    adamRecords
  }

  def getInputs(sc: SparkContext, dirname: String, extension: String): Seq[String] = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val path = new Path(dirname)

    def fileStatuses(fileStatus: FileStatus): Array[FileStatus] = {
      if (fileStatus.isDir) {
        fs.listStatus(fileStatus.getPath).flatMap(fileStatuses)
      } else {
        if (fileStatus.getPath.getName.endsWith(extension))
          Array(fileStatus)
        else
          Array()
      }
    }

    fileStatuses(fs.getFileStatus(path)).map(_.getPath.toUri.toString)
  }

  def swapExtensions(filenames: Seq[String], newExtension: String): Seq[String] = {
    for {
      filename <- filenames
      lastDotIdx = filename.lastIndexOf(".")
      extension = filename.substring(lastDotIdx + 1)
    } yield {
      filename.substring(0, lastDotIdx) + newExtension
    }
  }

  def run(sc: SparkContext) {

    def run(): Unit = {
      logInfo(s"Converting ${args.inputPath} to ${args.outputPath}")
      this.apply({
        if (args.forceLoadBam) {
          sc.loadBam(args.inputPath)
        } else if (args.forceLoadFastq) {
          sc.loadUnpairedFastq(args.inputPath)
        } else if (args.forceLoadIFastq) {
          sc.loadInterleavedFastq(args.inputPath)
        } else if (args.forceLoadParquet) {
          sc.loadParquetAlignments(args.inputPath)
        } else {
          sc.loadAlignments(args.inputPath)
        }
      }).adamSave(args, args.sortReads)
    }

    if (args.recursive) {
      args.recursive = false
      if (args.inputExtension == null) {
        throw new IllegalArgumentException("Must provide -input_extension argument with -recursive")
      }
      if (!args.inputExtension.startsWith(".")) {
        args.inputExtension = "." + args.inputExtension
      }
      val inputs = getInputs(sc, args.inputPath, args.inputExtension)
      if (!args.outputPath.startsWith(".")) {
        args.outputPath = "." + args.outputPath
      }
      val outputs = swapExtensions(inputs, args.outputPath)
      logInfo(s"Converting ${inputs.size} files:${inputs.zip(outputs).map(p => s"${p._1} -> ${p._2}").mkString("\n\t", "\n\t", "")}")
      for {
        (input, output) <- inputs.zip(outputs)
      } {
        args.inputPath = input
        args.outputPath = output
        run()
      }
    } else {
      run()
    }
  }

  private def createKnownSnpsTable(sc: SparkContext): SnpTable = CreateKnownSnpsTable.time {
    Option(args.knownSnpsFile).fold(SnpTable())(f => SnpTable(sc.loadVariants(f).map(new RichVariant(_))))
  }
}
