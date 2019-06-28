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

import java.time.Instant
import java.lang.{ Boolean => JBoolean }

import grizzled.slf4j.Logging
import htsjdk.samtools.ValidationStringency
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.Operators.BooleanColumn
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.algorithms.consensus._
import org.bdgenomics.adam.cli.FileSystemUtils._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.io.FastqRecordReader
import org.bdgenomics.adam.models.{ ReferenceRegion, SnpTable }
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Filter }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.{ AlignmentRecordDataset, QualityScoreBin }
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.{ AlignmentRecord, ProcessingStep }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object TransformAlignments extends BDGCommandCompanion {
  val commandName = "transformAlignments"
  val commandDescription = "Convert SAM/BAM to ADAM format and optionally perform read pre-processing transformations"

  def apply(cmdLine: Array[String]) = {
    val args = Args4j[TransformAlignmentsArgs](cmdLine)
    args.command = cmdLine.mkString(" ")
    new TransformAlignments(args)
  }
}

class TransformAlignmentsArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM, BAM or SAM file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the transformed data in ADAM/Parquet format", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-limit_projection", usage = "Only project necessary fields. Only works for Parquet files.")
  var limitProjection: Boolean = false
  @Args4jOption(required = false, name = "-aligned_read_predicate", usage = "Only load aligned reads. Only works for Parquet files. Exclusive of region predicate.")
  var useAlignedReadPredicate: Boolean = false
  @Args4jOption(required = false, name = "-region_predicate", usage = "Only load a specific range of regions. Mutually exclusive with aligned read predicate.")
  var regionPredicate: String = null
  @Args4jOption(required = false, name = "-sort_reads", usage = "Sort the reads by referenceId and read position")
  var sortReads: Boolean = false
  @Args4jOption(required = false, name = "-sort_lexicographically", usage = "Sort the reads lexicographically by contig name, instead of by index.")
  var sortLexicographically: Boolean = false
  @Args4jOption(required = false, name = "-mark_duplicate_reads", usage = "Mark duplicate reads")
  var markDuplicates: Boolean = false
  @Args4jOption(required = false, name = "-recalibrate_base_qualities", usage = "Recalibrate the base quality scores (ILLUMINA only)")
  var recalibrateBaseQualities: Boolean = false
  @Args4jOption(required = false, name = "-min_acceptable_quality", usage = "Minimum acceptable quality for recalibrating a base in a read. Default is 5.")
  var minAcceptableQuality: Int = 5
  @Args4jOption(required = false, name = "-sampling_fraction", usage = "Fraction of reads to sample during recalibration; omitted by default.")
  var samplingFraction: Double = 0.0
  @Args4jOption(required = false, name = "-sampling_seed", usage = "Seed to use when sampling during recalibration; omitted by default by setting to 0 (thus, 0 is an invalid seed).")
  var samplingSeed: Long = 0L
  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to LENIENT")
  var stringency: String = "LENIENT"
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
  @Args4jOption(required = false, name = "-unclip_reads", usage = "If true, unclips reads during realignment.")
  var unclipReads = false
  @Args4jOption(required = false, name = "-max_target_size", usage = "The maximum length of a target region to attempt realigning. Default length is 3000.")
  var maxTargetSize = 3000
  @Args4jOption(required = false, name = "-max_reads_per_target", usage = "The maximum number of reads attached to a target considered for realignment. Default is 20000.")
  var maxReadsPerTarget = 20000
  @Args4jOption(required = false, name = "-reference", usage = "Path to a reference file to use for indel realignment.")
  var reference: String = null
  @Args4jOption(required = false, name = "-repartition", usage = "Set the number of partitions to map data to")
  var repartition: Int = -1
  @Args4jOption(required = false, name = "-coalesce", usage = "Set the number of partitions written to the ADAM output directory")
  var coalesce: Int = -1
  @Args4jOption(required = false, name = "-force_shuffle_coalesce", usage = "Even if the repartitioned RDD has fewer partitions, force a shuffle.")
  var forceShuffle: Boolean = false
  @Args4jOption(required = false, name = "-sort_fastq_output", usage = "Sets whether to sort the FASTQ output, if saving as FASTQ. False by default. Ignored if not saving as FASTQ.")
  var sortFastqOutput: Boolean = false
  @Args4jOption(required = false, name = "-force_load_bam", usage = "Forces TransformAlignments to load from BAM/SAM.")
  var forceLoadBam: Boolean = false
  @Args4jOption(required = false, name = "-force_load_fastq", usage = "Forces TransformAlignments to load from unpaired FASTQ.")
  var forceLoadFastq: Boolean = false
  @Args4jOption(required = false, name = "-force_load_ifastq", usage = "Forces TransformAlignments to load from interleaved FASTQ.")
  var forceLoadIFastq: Boolean = false
  @Args4jOption(required = false, name = "-force_load_parquet", usage = "Forces TransformAlignments to load from Parquet.")
  var forceLoadParquet: Boolean = false
  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false
  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output")
  var deferMerging: Boolean = false
  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false
  @Args4jOption(required = false, name = "-paired_fastq", usage = "When converting two (paired) FASTQ files to ADAM, pass the path to the second file here.")
  var pairedFastqFile: String = null
  @Args4jOption(required = false, name = "-read_group", usage = "Set converted FASTQs' read-group identifiers to this value; if empty-string is passed, use the basename of the input file, minus the extension.")
  var fastqReadGroup: String = null
  @Args4jOption(required = false, name = "-concat", usage = "Concatenate this file with <INPUT> and write the result to <OUTPUT>")
  var concatFilename: String = null
  @Args4jOption(required = false, name = "-add_md_tags", usage = "Add MD Tags to reads based on the FASTA (or equivalent) file passed to this option.")
  var mdTagsReferenceFile: String = null
  @Args4jOption(required = false, name = "-md_tag_fragment_size", usage = "When adding MD tags to reads, load the reference in fragments of this size.")
  var mdTagsFragmentSize: Long = 1000000L
  @Args4jOption(required = false, name = "-md_tag_overwrite", usage = "When adding MD tags to reads, overwrite existing incorrect tags.")
  var mdTagsOverwrite: Boolean = false
  @Args4jOption(required = false, name = "-bin_quality_scores", usage = "Rewrites quality scores of reads into bins from a string of bin descriptions, e.g. 0,20,10;20,40,30.")
  var binQualityScores: String = null
  @Args4jOption(required = false, name = "-cache", usage = "Cache data to avoid recomputing between stages.")
  var cache: Boolean = false
  @Args4jOption(required = false, name = "-storage_level", usage = "Set the storage level to use for caching.")
  var storageLevel: String = "MEMORY_ONLY"
  @Args4jOption(required = false, name = "-disable_pg", usage = "Disable writing a new @PG line.")
  var disableProcessingStep = false
  @Args4jOption(required = false, name = "-partition_by_start_pos", usage = "Save the data partitioned by genomic range bins based on start pos using Hive-style partitioning.")
  var partitionByStartPos: Boolean = false
  @Args4jOption(required = false, name = "-partition_bin_size", usage = "Partition bin size used in Hive-style partitioning. Defaults to 1Mbp (1,000,000) base pairs).")
  var partitionedBinSize = 1000000
  @Args4jOption(required = false, name = "-max_read_length", usage = "Maximum FASTQ read length, defaults to 10,000 base pairs (bp).")
  var maxReadLength: Int = 0

  var command: String = null
}

class TransformAlignments(protected val args: TransformAlignmentsArgs) extends BDGSparkCommand[TransformAlignmentsArgs] with Logging {
  val companion = TransformAlignments

  val stringency = ValidationStringency.valueOf(args.stringency)

  /**
   * @param rdd An RDD of reads.
   * @return If the binQualityScores argument is set, rewrites the quality scores of the
   *   reads into bins. Else, returns the original RDD.
   */
  private def maybeBin(rdd: AlignmentRecordDataset): AlignmentRecordDataset = {
    Option(args.binQualityScores).fold(rdd)(binDescription => {
      val bins = QualityScoreBin(binDescription)
      rdd.binQualityScores(bins)
    })
  }

  /**
   * @param ds A genomic dataset of reads.
   * @return If the repartition argument is set, repartitions the input dataset
   *   to the number of partitions requested by the user. Forces a shuffle using
   *   a hash partitioner.
   */
  private def maybeRepartition(ds: AlignmentRecordDataset): AlignmentRecordDataset = {
    if (args.repartition != -1) {
      info("Repartitioning reads to to '%d' partitions".format(args.repartition))
      ds.transform((rdd: RDD[AlignmentRecord]) => rdd.repartition(args.repartition))
    } else {
      ds
    }
  }

  /**
   * @param ds The genomic dataset of reads that was the output of [[maybeRepartition]].
   * @return If the user has provided the `-mark_duplicates` flag, returns a RDD
   *   where reads have been marked as duplicates if they appear to be from
   *   duplicated fragments. Else, returns the input RDD.
   */
  private def maybeDedupe(ds: AlignmentRecordDataset): AlignmentRecordDataset = {
    if (args.markDuplicates) {
      info("Marking duplicates")
      ds.markDuplicates()
    } else {
      ds
    }
  }

  /**
   * @param ds The genomic dataset of reads that was output by [[maybeDedupe]].
   * @param sl The requested storage level for caching, if requested.
   *   Realignment is a two pass algorithm (generate targets, then realign), so
   *   we allow users to request caching with the -cache flag.
   * @return If the user has set the -realign_indels flag, we realign the input
   *   RDD against a set of targets. If the user has provided a known set of
   *   INDELs with the -known_indels flag, we realign against those indels.
   *   Else, we generate candidate INDELs from the reads and then realign. If
   *   -realign_indels is not set, we return the input RDD.
   */
  private def maybeRealign(sc: SparkContext,
                           ds: AlignmentRecordDataset,
                           sl: StorageLevel): AlignmentRecordDataset = {
    if (args.locallyRealign) {

      info("Locally realigning indels.")

      // has the user asked us to cache the rdd before multi-pass stages?
      if (args.cache) {
        ds.rdd.persist(sl)
      }

      // fold and get the consensus model for this realignment run
      val consensusGenerator = Option(args.knownIndelsFile)
        .fold(ConsensusGenerator.fromReads)(file => {
          ConsensusGenerator.fromKnownIndels(ds.rdd.context.loadVariants(file))
        })

      // optionally load a reference
      val optReferenceFile = Option(args.reference).map(f => {
        sc.loadReferenceFile(f,
          maximumLength = args.mdTagsFragmentSize)
      })

      // run realignment
      val realignmentDs = ds.realignIndels(
        consensusGenerator,
        isSorted = false,
        maxIndelSize = args.maxIndelSize,
        maxConsensusNumber = args.maxConsensusNumber,
        lodThreshold = args.lodThreshold,
        maxTargetSize = args.maxTargetSize,
        maxReadsPerTarget = args.maxReadsPerTarget,
        optReferenceFile = optReferenceFile,
        unclipReads = args.unclipReads
      )

      // unpersist our input, if persisting was requested
      // we don't reference said rdd again, so unpersisting is ok
      if (args.cache) {
        ds.rdd.unpersist()
      }

      realignmentDs
    } else {
      ds
    }
  }

  /**
   * @param ds The genomic dataset of reads that was output by [[maybeRealign]].
   * @param sl The requested storage level for caching, if requested.
   *   BQSR is a two pass algorithm (generate table, then recalibrate), so
   *   we allow users to request caching with the -cache flag.
   * @return If the user has set the -recalibrate_base_qualities flag, we
   *   estimate the empirical base qualities and recalibrate the reads. If
   *   the -known_snps flag is set, we use that file to mask off sites of
   *   known variation when recalibrating. If BQSR has not been requested,
   *   we return the input RDD.
   */
  private def maybeRecalibrate(ds: AlignmentRecordDataset,
                               sl: StorageLevel): AlignmentRecordDataset = {
    if (args.recalibrateBaseQualities) {

      info("Recalibrating base qualities")

      // bqsr is a two pass algorithm, so cache the rdd if requested
      val optSl = if (args.cache) {
        Some(sl)
      } else {
        None
      }

      // create the known sites file, if one is available
      val knownSnps: SnpTable = createKnownSnpsTable(ds.rdd.context)
      val broadcastedSnps = BroadcastingKnownSnps.time {
        ds.rdd.context.broadcast(knownSnps)
      }

      // run bqsr
      val bqsredDs = ds.recalibrateBaseQualities(
        broadcastedSnps,
        args.minAcceptableQuality,
        optSl,
        optSamplingFraction = Option(args.samplingFraction).filter(_ > 0.0),
        optSamplingSeed = Option(args.samplingSeed).filter(_ != 0L)
      )

      bqsredDs
    } else {
      ds
    }
  }

  /**
   * @param ds The genomic dataset of reads that was output by [[maybeRecalibrate]].
   * @return If -coalesce is set, coalesces the RDD to the number of paritions
   *   requested. Forces a shuffle if the number of partitions requested is
   *   smaller than the current number of partitions, or if the -force_shuffle
   *   flag is set. If -coalesce was not requested, returns the input RDD.
   */
  private def maybeCoalesce(ds: AlignmentRecordDataset): AlignmentRecordDataset = {
    if (args.coalesce != -1) {
      info("Coalescing the number of partitions to '%d'".format(args.coalesce))
      if (args.coalesce > ds.rdd.partitions.length || args.forceShuffle) {
        ds.transform((rdd: RDD[AlignmentRecord]) => rdd.coalesce(args.coalesce, shuffle = true))
      } else {
        ds.transform((rdd: RDD[AlignmentRecord]) => rdd.coalesce(args.coalesce, shuffle = false))
      }
    } else {
      ds
    }
  }

  /**
   * @param ds The genomic dataset of reads that was output by [[maybeCoalesce]].
   * @param sl The requested storage level for caching, if requested.
   * @return If -sortReads was set, returns the sorted reads. If
   *   -sort_lexicographically is additionally set, sorts lexicographically,
   *   instead of by contig index. If no sorting was requested, returns
   *   the input RDD.
   */
  private def maybeSort(ds: AlignmentRecordDataset,
                        sl: StorageLevel): AlignmentRecordDataset = {
    if (args.sortReads) {

      // cache the input if requested. sort is two stages:
      // 1. sample to create partitioner
      // 2. repartition and sort within partitions
      if (args.cache) {
        ds.rdd.persist(sl)
      }

      info("Sorting reads")

      // are we sorting lexicographically or using legacy SAM sort order?
      val sortedDs = if (args.sortLexicographically) {
        ds.sortByReferencePosition()
      } else {
        ds.sortByReferencePositionAndIndex()
      }

      // unpersist the cached rdd, if caching was requested
      if (args.cache) {
        ds.rdd.unpersist()
      }

      sortedDs
    } else {
      ds
    }
  }

  /**
   * @param ds The genomic dataset of reads that was output by [[maybeSort]].
   * @param stringencyOpt An optional validation stringency.
   * @return If -add_md_tags is set, recomputes the MD tags for the reads
   *   provided. If some reads have tags, -md_tag_overwrite controls whether
   *   these tags are recomputed or not. If MD tagging isn't requested, we
   *   return the input RDD.
   */
  private def maybeMdTag(sc: SparkContext,
                         ds: AlignmentRecordDataset,
                         stringencyOpt: Option[ValidationStringency]): AlignmentRecordDataset = {
    if (args.mdTagsReferenceFile != null) {
      info(s"Adding MDTags to reads based on reference file ${args.mdTagsReferenceFile}")
      val referenceFile = sc.loadReferenceFile(args.mdTagsReferenceFile,
        maximumLength = args.mdTagsFragmentSize)
      ds.computeMismatchingPositions(
        referenceFile,
        overwriteExistingTags = args.mdTagsOverwrite,
        validationStringency = stringencyOpt.getOrElse(ValidationStringency.STRICT))
    } else {
      ds
    }
  }

  def apply(ds: AlignmentRecordDataset): AlignmentRecordDataset = {

    val sc = ds.rdd.context
    val sl = StorageLevel.fromString(args.storageLevel)

    val stringencyOpt = Option(args.stringency).map(ValidationStringency.valueOf(_))

    if (args.maxReadLength > 0) {
      FastqRecordReader.setMaxReadLength(sc.hadoopConfiguration, args.maxReadLength)
    }

    // first repartition if needed
    val initialDs = maybeRepartition(ds)

    // then bin, if desired
    val binnedDs = maybeBin(initialDs)

    // then, mark duplicates, if desired
    val maybeDedupedDs = maybeDedupe(binnedDs)

    // once we've deduped our reads, maybe realign them
    val maybeRealignedDs = maybeRealign(sc, maybeDedupedDs, sl)

    // run BQSR
    val maybeRecalibratedDs = maybeRecalibrate(maybeRealignedDs, sl)

    // does the user want us to coalesce before saving?
    val finalPreprocessedDs = maybeCoalesce(maybeRecalibratedDs)

    // NOTE: For now, sorting needs to be the last transform
    val maybeSortedDs = maybeSort(finalPreprocessedDs, sl)

    // recompute md tags, if requested, and return
    maybeMdTag(sc, maybeSortedDs, stringencyOpt)
  }

  def forceNonParquet(): Boolean = {
    args.forceLoadBam || args.forceLoadFastq || args.forceLoadIFastq
  }

  def isNonParquet(inputPath: String): Boolean = {
    inputPath.endsWith(".sam") ||
      inputPath.endsWith(".bam") ||
      inputPath.endsWith(".ifq") ||
      inputPath.endsWith(".fq") ||
      inputPath.endsWith(".fastq") ||
      inputPath.endsWith(".fa") ||
      inputPath.endsWith(".fasta")
  }

  def run(sc: SparkContext) {
    checkWriteablePath(args.outputPath, sc.hadoopConfiguration)

    // throw exception if aligned read predicate or limit projection flags are used improperly
    if (args.useAlignedReadPredicate && forceNonParquet()) {
      throw new IllegalArgumentException(
        "-aligned_read_predicate only applies to Parquet files, but a non-Parquet force load flag was passed."
      )
    }
    if (args.limitProjection && forceNonParquet()) {
      throw new IllegalArgumentException(
        "-limit_projection only applies to Parquet files, but a non-Parquet force load flag was passed."
      )
    }
    if (args.useAlignedReadPredicate && isNonParquet(args.inputPath)) {
      throw new IllegalArgumentException(
        "-aligned_read_predicate only applies to Parquet files, but a non-Parquet input path was specified."
      )
    }
    if (args.limitProjection && isNonParquet(args.inputPath)) {
      throw new IllegalArgumentException(
        "-limit_projection only applies to Parquet files, but a non-Parquet input path was specified."
      )
    }
    if (args.useAlignedReadPredicate && args.regionPredicate != null) {
      throw new IllegalArgumentException(
        "-aligned_read_predicate and -region_predicate are mutually exclusive"
      )
    }

    val loadedDs: AlignmentRecordDataset =
      if (args.forceLoadBam) {
        if (args.regionPredicate != null) {
          val loci = ReferenceRegion.fromString(args.regionPredicate)
          sc.loadIndexedBam(args.inputPath, loci)
        } else {
          sc.loadBam(args.inputPath)
        }
      } else if (args.forceLoadFastq) {
        sc.loadFastq(args.inputPath, Option(args.pairedFastqFile), Option(args.fastqReadGroup), stringency)
      } else if (args.forceLoadIFastq) {
        sc.loadInterleavedFastq(args.inputPath)
      } else if (args.forceLoadParquet ||
        args.useAlignedReadPredicate ||
        args.limitProjection) {
        val pred = if (args.useAlignedReadPredicate) {
          Some(FilterApi.eq[JBoolean, BooleanColumn](
            FilterApi.booleanColumn("readMapped"), true))
        } else if (args.regionPredicate != null) {
          Some(ReferenceRegion.createPredicate(
            ReferenceRegion.fromString(args.regionPredicate).toSeq: _*
          ))
        } else {
          None
        }

        val proj = if (args.limitProjection) {
          Some(Filter(AlignmentRecordField.attributes,
            AlignmentRecordField.originalQuality))
        } else {
          None
        }

        sc.loadParquetAlignments(args.inputPath,
          optPredicate = pred,
          optProjection = proj)
      } else {
        val loadedReads = sc.loadAlignments(
          args.inputPath,
          optPathName2 = Option(args.pairedFastqFile),
          optReadGroup = Option(args.fastqReadGroup),
          stringency = stringency
        )

        if (args.regionPredicate != null) {
          val loci = ReferenceRegion.fromString(args.regionPredicate)
          loadedReads.filterByOverlappingRegions(loci)
        } else {
          loadedReads
        }
      }

    val aDs: AlignmentRecordDataset = if (args.disableProcessingStep) {
      loadedDs
    } else {
      // add program info
      val about = new About()
      val version = if (about.isSnapshot()) {
        "%s--%s".format(about.version(), about.commit())
      } else {
        about.version()
      }
      val epoch = Instant.now.getEpochSecond
      val processingStep = ProcessingStep.newBuilder
        .setId("ADAM_%d".format(epoch))
        .setProgramName("org.bdgenomics.adam.cli.TransformAlignments")
        .setVersion(version)
        .setCommandLine(args.command)
        .build
      loadedDs.addProcessingStep(processingStep)
    }

    val rdd = aDs.rdd
    val sd = aDs.sequences
    val rgd = aDs.readGroups
    val pgs = aDs.processingSteps

    // Optionally load a second RDD and concatenate it with the first.
    // Paired-FASTQ loading is avoided here because that wouldn't make sense
    // given that it's already happening above.
    val concatOpt =
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
            optReadGroup = Option(args.fastqReadGroup)
          )
        })

    // if we have a second rdd that we are merging in, process the merger here
    val (mergedRdd, mergedSd, mergedRgd, mergedPgs) = concatOpt.fold((rdd, sd, rgd, pgs))(t => {
      (rdd ++ t.rdd, sd ++ t.sequences, rgd ++ t.readGroups, pgs ++ t.processingSteps)
    })

    // make a new aligned read rdd, that merges the two RDDs together
    val newDs = AlignmentRecordDataset(mergedRdd, mergedSd, mergedRgd, mergedPgs)

    // run our transformation
    val outputDs = this.apply(newDs)

    // if we are sorting, we must strip the indices from the sequence dictionary
    // and sort the sequence dictionary
    //
    // we must do this because we do a lexicographic sort, not an index-based sort
    val sdFinal = if (args.sortReads) {
      if (args.sortLexicographically) {
        mergedSd.stripIndices
          .sorted
      } else {
        mergedSd
      }
    } else {
      mergedSd
    }

    if (args.partitionByStartPos) {
      if (outputDs.sequences.isEmpty) {
        warn("This dataset is not aligned and therefore will not benefit from being saved as a partitioned dataset")
      }
      outputDs.saveAsPartitionedParquet(args.outputPath, partitionSize = args.partitionedBinSize)
    } else {
      outputDs.save(args,
        isSorted = args.sortReads || args.sortLexicographically)
    }
  }

  private def createKnownSnpsTable(sc: SparkContext): SnpTable = {
    Option(args.knownSnpsFile).fold(SnpTable())(f => SnpTable(sc.loadVariants(f)))
  }
}
