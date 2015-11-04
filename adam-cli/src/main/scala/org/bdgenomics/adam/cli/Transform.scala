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
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.algorithms.consensus._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.SnpTable
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.MDTagging
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
  @Args4jOption(required = false, name = "-limit_projection", usage = "Only project necessary fields. Only works for Parquet files.")
  var limitProjection: Boolean = false
  @Args4jOption(required = false, name = "-aligned_read_predicate", usage = "Only load aligned reads. Only works for Parquet files.")
  var useAlignedReadPredicate: Boolean = false
  @Args4jOption(required = false, name = "-sort_reads", usage = "Sort the reads by referenceId and read position")
  var sortReads: Boolean = false
  @Args4jOption(required = false, name = "-mark_duplicate_reads", usage = "Mark duplicate reads")
  var markDuplicates: Boolean = false
  @Args4jOption(required = false, name = "-recalibrate_base_qualities", usage = "Recalibrate the base quality scores (ILLUMINA only)")
  var recalibrateBaseQualities: Boolean = false
  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to LENIENT")
  var stringency: String = "LENIENT"
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
  @Args4jOption(required = false, name = "-force_shuffle_coalesce", usage = "Even if the repartitioned RDD has fewer partitions, force a shuffle.")
  var forceShuffle: Boolean = false
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
  @Args4jOption(required = false, name = "-paired_fastq", usage = "When converting two (paired) FASTQ files to ADAM, pass the path to the second file here.")
  var pairedFastqFile: String = null
  @Args4jOption(required = false, name = "-record_group", usage = "Set converted FASTQs' record-group names to this value; if empty-string is passed, use the basename of the input file, minus the extension.")
  var fastqRecordGroup: String = null
  @Args4jOption(required = false, name = "-concat", usage = "Concatenate this file with <INPUT> and write the result to <OUTPUT>")
  var concatFilename: String = null
  @Args4jOption(required = false, name = "-add_md_tags", usage = "Add MD Tags to reads based on the FASTA (or equivalent) file passed to this option.")
  var mdTagsReferenceFile: String = null
  @Args4jOption(required = false, name = "-md_tag_fragment_size", usage = "When adding MD tags to reads, load the reference in fragments of this size.")
  var mdTagsFragmentSize: Long = 1000000L
  @Args4jOption(required = false, name = "-md_tag_overwrite", usage = "When adding MD tags to reads, overwrite existing incorrect tags.")
  var mdTagsOverwrite: Boolean = false
}

class Transform(protected val args: TransformArgs) extends BDGSparkCommand[TransformArgs] with Logging {
  val companion = Transform

  val stringency = ValidationStringency.valueOf(args.stringency)

  def apply(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {

    var adamRecords = rdd
    val sc = rdd.context

    val stringencyOpt = Option(args.stringency).map(ValidationStringency.valueOf(_))

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
        args.maxTargetSize
      )
    }

    if (args.recalibrateBaseQualities) {
      log.info("Recalibrating base qualities")
      val knownSnps: SnpTable = createKnownSnpsTable(sc)
      adamRecords = adamRecords.adamBQSR(
        sc.broadcast(knownSnps),
        Option(args.observationsPath),
        stringency
      )
    }

    if (args.coalesce != -1) {
      log.info("Coalescing the number of partitions to '%d'".format(args.coalesce))
      if (args.coalesce > adamRecords.partitions.size || args.forceShuffle) {
        adamRecords = adamRecords.coalesce(args.coalesce, shuffle = true)
      } else {
        adamRecords = adamRecords.coalesce(args.coalesce, shuffle = false)
      }
    }

    // NOTE: For now, sorting needs to be the last transform
    if (args.sortReads) {
      log.info("Sorting reads")
      adamRecords = adamRecords.adamSortReadsByReferencePosition()
    }

    if (args.mdTagsReferenceFile != null) {
      log.info(s"Adding MDTags to reads based on reference file ${args.mdTagsReferenceFile}")
      adamRecords =
        MDTagging(
          adamRecords,
          args.mdTagsReferenceFile,
          fragmentLength = args.mdTagsFragmentSize,
          overwriteExistingTags = args.mdTagsOverwrite,
          validationStringency = stringencyOpt.getOrElse(ValidationStringency.STRICT)
        )
    }

    adamRecords
  }

  def run(sc: SparkContext) {
    // throw exception if aligned read predicate or projection flags are used improperly
    if ((args.useAlignedReadPredicate || args.limitProjection) &&
      (args.forceLoadBam || args.forceLoadFastq || args.forceLoadIFastq)) {
      throw new IllegalArgumentException(
        "-aligned_read_predicate and -limit_projection only apply to Parquet files, but a non-Parquet force load flag was passed.")
    }

    val rdd =
      if (args.forceLoadBam) {
        sc.loadBam(args.inputPath)
      } else if (args.forceLoadFastq) {
        sc.loadFastq(args.inputPath, Option(args.pairedFastqFile), Option(args.fastqRecordGroup), stringency)
      } else if (args.forceLoadIFastq) {
        sc.loadInterleavedFastq(args.inputPath)
      } else if (args.forceLoadParquet ||
        args.limitProjection ||
        args.useAlignedReadPredicate) {
        val pred = if (args.useAlignedReadPredicate) {
          Some((BooleanColumn("readMapped") === true))
        } else {
          None
        }
        val proj = if (args.limitProjection) {
          Some(Projection(AlignmentRecordField.contig,
            AlignmentRecordField.start,
            AlignmentRecordField.end,
            AlignmentRecordField.mapq,
            AlignmentRecordField.readName,
            AlignmentRecordField.sequence,
            AlignmentRecordField.cigar,
            AlignmentRecordField.qual,
            AlignmentRecordField.recordGroupId,
            AlignmentRecordField.recordGroupName,
            AlignmentRecordField.readPaired,
            AlignmentRecordField.readMapped,
            AlignmentRecordField.readNegativeStrand,
            AlignmentRecordField.firstOfPair,
            AlignmentRecordField.secondOfPair,
            AlignmentRecordField.primaryAlignment,
            AlignmentRecordField.duplicateRead,
            AlignmentRecordField.mismatchingPositions,
            AlignmentRecordField.secondaryAlignment,
            AlignmentRecordField.supplementaryAlignment))
        } else {
          None
        }
        sc.loadParquetAlignments(args.inputPath,
          predicate = pred,
          projection = proj)
      } else {
        sc.loadAlignments(
          args.inputPath,
          filePath2Opt = Option(args.pairedFastqFile),
          recordGroupOpt = Option(args.fastqRecordGroup),
          stringency = stringency
        )
      }

    // Optionally load a second RDD and concatenate it with the first.
    // Paired-FASTQ loading is avoided here because that wouldn't make sense
    // given that it's already happening above.
    val concatRddOpt =
      Option(args.concatFilename).map(concatFilename =>
        if (args.forceLoadBam) {
          sc.loadBam(concatFilename)
        } else if (args.forceLoadIFastq) {
          sc.loadInterleavedFastq(concatFilename)
        } else if (args.forceLoadParquet) {
          sc.loadParquetAlignments(concatFilename)
        } else {
          sc.loadAlignments(
            concatFilename,
            recordGroupOpt = Option(args.fastqRecordGroup)
          )
        }
      )

    this.apply(concatRddOpt match {
      case Some(concatRdd) => rdd ++ concatRdd
      case None            => rdd
    }).adamSave(args, args.sortReads)
  }

  private def createKnownSnpsTable(sc: SparkContext): SnpTable = CreateKnownSnpsTable.time {
    Option(args.knownSnpsFile).fold(SnpTable())(f => SnpTable(sc.loadVariants(f).map(new RichVariant(_))))
  }
}
