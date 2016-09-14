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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.algorithms.consensus._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  SequenceDictionary,
  SnpTable
}
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Filter }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.{ AlignedReadRDD, AlignmentRecordRDD, MDTagging }
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
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
  @Args4jOption(required = false, name = "-sort_lexicographically", usage = "Sort the reads lexicographically by contig name, instead of by index.")
  var sortLexicographically: Boolean = false
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
  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output")
  var deferMerging: Boolean = false
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
  @Args4jOption(required = false, name = "-cache", usage = "Cache data to avoid recomputing between stages.")
  var cache: Boolean = false
  @Args4jOption(required = false, name = "-storage_level", usage = "Set the storage level to use for caching.")
  var storageLevel: String = "MEMORY_ONLY"
}

class Transform(protected val args: TransformArgs) extends BDGSparkCommand[TransformArgs] with Logging {
  val companion = Transform

  val stringency = ValidationStringency.valueOf(args.stringency)

  /**
   * @param rdd An RDD of reads.
   * @return If the repartition argument is set, repartitions the input dataset
   *   to the number of partitions requested by the user. Forces a shuffle using
   *   a hash partitioner.
   */
  private def maybeRepartition(rdd: AlignmentRecordRDD): AlignmentRecordRDD = {
    if (args.repartition != -1) {
      log.info("Repartitioning reads to to '%d' partitions".format(args.repartition))
      rdd.transform(_.repartition(args.repartition))
    } else {
      rdd
    }
  }

  /**
   * @param rdd The RDD of reads that was the output of [[maybeRepartition]].
   * @return If the user has provided the `-mark_duplicates` flag, returns a RDD
   *   where reads have been marked as duplicates if they appear to be from
   *   duplicated fragments. Else, returns the input RDD.
   */
  private def maybeDedupe(rdd: AlignmentRecordRDD): AlignmentRecordRDD = {
    if (args.markDuplicates) {
      log.info("Marking duplicates")
      rdd.markDuplicates()
    } else {
      rdd
    }
  }

  /**
   * @param rdd The RDD of reads that was output by [[maybeDedup]].
   * @param sl The requested storage level for caching, if requested.
   *   Realignment is a two pass algorithm (generate targets, then realign), so
   *   we allow users to request caching with the -cache flag.
   * @return If the user has set the -realign_indels flag, we realign the input
   *   RDD against a set of targets. If the user has provided a known set of
   *   INDELs with the -known_indels flag, we realign against those indels.
   *   Else, we generate candidate INDELs from the reads and then realign. If
   *   -realign_indels is not set, we return the input RDD.
   */
  private def maybeRealign(rdd: AlignmentRecordRDD,
                           sl: StorageLevel): AlignmentRecordRDD = {
    if (args.locallyRealign) {

      log.info("Locally realigning indels.")

      // has the user asked us to cache the rdd before multi-pass stages?
      if (args.cache) {
        rdd.rdd.persist(sl)
      }

      // fold and get the consensus model for this realignment run
      val consensusGenerator = Option(args.knownIndelsFile)
        .fold(new ConsensusGeneratorFromReads().asInstanceOf[ConsensusGenerator])(
          new ConsensusGeneratorFromKnowns(_, rdd.rdd.context).asInstanceOf[ConsensusGenerator]
        )

      // run realignment
      val realignmentRdd = rdd.realignIndels(
        consensusGenerator,
        isSorted = false,
        args.maxIndelSize,
        args.maxConsensusNumber,
        args.lodThreshold,
        args.maxTargetSize
      )

      // unpersist our input, if persisting was requested
      // we don't reference said rdd again, so unpersisting is ok
      if (args.cache) {
        rdd.rdd.unpersist()
      }

      realignmentRdd
    } else {
      rdd
    }
  }

  /**
   * @param rdd The RDD of reads that was output by [[maybeRealign]].
   * @param sl The requested storage level for caching, if requested.
   *   BQSR is a two pass algorithm (generate table, then recalibrate), so
   *   we allow users to request caching with the -cache flag.
   * @return If the user has set the -recalibrate_base_qualities flag, we
   *   estimate the empirical base qualities and recalibrate the reads. If
   *   the -known_snps flag is set, we use that file to mask off sites of
   *   known variation when recalibrating. If BQSR has not been requested,
   *   we return the input RDD.
   */
  private def maybeRecalibrate(rdd: AlignmentRecordRDD,
                               sl: StorageLevel): AlignmentRecordRDD = {
    if (args.recalibrateBaseQualities) {

      log.info("Recalibrating base qualities")

      // bqsr is a two pass algorithm, so cache the rdd if requested
      if (args.cache) {
        rdd.rdd.persist(sl)
      }

      // create the known sites file, if one is available
      val knownSnps: SnpTable = createKnownSnpsTable(rdd.rdd.context)

      // run bqsr
      val bqsredRdd = rdd.recalibateBaseQualities(
        rdd.rdd.context.broadcast(knownSnps),
        Option(args.observationsPath),
        stringency
      )

      // if we cached the input, unpersist it, as it is never reused after bqsr
      if (args.cache) {
        rdd.rdd.unpersist()
      }

      bqsredRdd
    } else {
      rdd
    }
  }

  /**
   * @param rdd The RDD of reads that was output by [[maybeRecalibrate]].
   * @return If -coalesce is set, coalesces the RDD to the number of paritions
   *   requested. Forces a shuffle if the number of partitions requested is
   *   smaller than the current number of partitions, or if the -force_shuffle
   *   flag is set. If -coalesce was not requested, returns the input RDD.
   */
  private def maybeCoalesce(rdd: AlignmentRecordRDD): AlignmentRecordRDD = {
    if (args.coalesce != -1) {
      log.info("Coalescing the number of partitions to '%d'".format(args.coalesce))
      if (args.coalesce > rdd.rdd.partitions.size || args.forceShuffle) {
        rdd.transform(_.coalesce(args.coalesce, shuffle = true))
      } else {
        rdd.transform(_.coalesce(args.coalesce, shuffle = false))
      }
    } else {
      rdd
    }
  }

  /**
   * @param rdd The RDD of reads that was output by [[maybeCoalesce]].
   * @param sl The requested storage level for caching, if requested.
   * @return If -sortReads was set, returns the sorted reads. If
   *   -sort_lexicographically is additionally set, sorts lexicographically,
   *   instead of by contig index. If no sorting was requested, returns
   *   the input RDD.
   */
  private def maybeSort(rdd: AlignmentRecordRDD,
                        sl: StorageLevel): AlignmentRecordRDD = {
    if (args.sortReads) {

      // cache the input if requested. sort is two stages:
      // 1. sample to create partitioner
      // 2. repartition and sort within partitions
      if (args.cache) {
        rdd.rdd.persist(sl)
      }

      log.info("Sorting reads")

      // are we sorting lexicographically or using legacy SAM sort order?
      val sortedRdd = if (args.sortLexicographically) {
        rdd.sortReadsByReferencePosition()
      } else {
        rdd.sortReadsByReferencePositionAndIndex()
      }

      // unpersist the cached rdd, if caching was requested
      if (args.cache) {
        rdd.rdd.unpersist()
      }

      sortedRdd
    } else {
      rdd
    }
  }

  /**
   * @param rdd The RDD of reads that was output by [[maybeSort]].
   * @param stringencyOpt An optional validation stringency.
   * @return If -add_md_tags is set, recomputes the MD tags for the reads
   *   provided. If some reads have tags, -md_tag_overwrite controls whether
   *   these tags are recomputed or not. If MD tagging isn't requested, we
   *   return the input RDD.
   */
  def maybeMdTag(rdd: AlignmentRecordRDD,
                 stringencyOpt: Option[ValidationStringency]): AlignmentRecordRDD = {
    if (args.mdTagsReferenceFile != null) {
      log.info(s"Adding MDTags to reads based on reference file ${args.mdTagsReferenceFile}")
      rdd.transform(r => {
        MDTagging(
          r,
          args.mdTagsReferenceFile,
          fragmentLength = args.mdTagsFragmentSize,
          overwriteExistingTags = args.mdTagsOverwrite,
          validationStringency = stringencyOpt.getOrElse(ValidationStringency.STRICT)
        )
      })
    } else {
      rdd
    }
  }

  def apply(rdd: AlignmentRecordRDD): AlignmentRecordRDD = {

    val sc = rdd.rdd.context
    val sl = StorageLevel.fromString(args.storageLevel)

    val stringencyOpt = Option(args.stringency).map(ValidationStringency.valueOf(_))

    // first repartition if needed
    val initialRdd = maybeRepartition(rdd)

    // then, mark duplicates, if desired
    val maybeDedupedRdd = maybeDedupe(initialRdd)

    // once we've deduped our reads, maybe realign them
    val maybeRealignedRdd = maybeRealign(maybeDedupedRdd, sl)

    // run BQSR
    val maybeRecalibratedRdd = maybeRecalibrate(maybeRealignedRdd, sl)

    // does the user want us to coalesce before saving?
    val finalPreprocessedRdd = maybeCoalesce(maybeRecalibratedRdd)

    // NOTE: For now, sorting needs to be the last transform
    val maybeSortedRdd = maybeSort(finalPreprocessedRdd, sl)

    // recompute md tags, if requested, and return
    maybeMdTag(maybeSortedRdd, stringencyOpt)
  }

  def forceNonParquet(): Boolean = {
    (args.forceLoadBam || args.forceLoadFastq || args.forceLoadIFastq)
  }

  def isNonParquet(inputPath: String): Boolean = {
    (
      inputPath.endsWith(".sam") ||
      inputPath.endsWith(".bam") ||
      inputPath.endsWith(".ifq") ||
      inputPath.endsWith(".fq") ||
      inputPath.endsWith(".fastq") ||
      inputPath.endsWith(".fa") ||
      inputPath.endsWith(".fasta")
    )
  }

  def run(sc: SparkContext) {
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

    val aRdd: AlignmentRecordRDD =
      if (args.forceLoadBam) {
        sc.loadBam(args.inputPath)
      } else if (args.forceLoadFastq) {
        sc.loadFastq(args.inputPath, Option(args.pairedFastqFile), Option(args.fastqRecordGroup), stringency)
      } else if (args.forceLoadIFastq) {
        sc.loadInterleavedFastq(args.inputPath)
      } else if (args.forceLoadParquet ||
        args.useAlignedReadPredicate ||
        args.limitProjection) {
        val pred = if (args.useAlignedReadPredicate) {
          Some((BooleanColumn("readMapped") === true))
        } else {
          None
        }

        val proj = if (args.limitProjection) {
          Some(Filter(AlignmentRecordField.attributes,
            AlignmentRecordField.origQual))
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
          stringency = stringency)
      }
    val rdd = aRdd.rdd
    val sd = aRdd.sequences
    val rgd = aRdd.recordGroups

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
            recordGroupOpt = Option(args.fastqRecordGroup)
          )
        })

    // if we have a second rdd that we are merging in, process the merger here
    val (mergedRdd, mergedSd, mergedRgd) = concatOpt.fold((rdd, sd, rgd))(t => {
      (rdd ++ t.rdd, sd ++ t.sequences, rgd ++ t.recordGroups)
    })

    // make a new aligned read rdd, that merges the two RDDs together
    val newRdd = AlignedReadRDD(mergedRdd, mergedSd, mergedRgd)

    // run our transformation
    val outputRdd = this.apply(newRdd)

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

    outputRdd.save(args,
      isSorted = (args.sortReads || args.sortLexicographically))
  }

  private def createKnownSnpsTable(sc: SparkContext): SnpTable = CreateKnownSnpsTable.time {
    Option(args.knownSnpsFile).fold(SnpTable())(f => SnpTable(sc.loadVariants(f).rdd.map(new RichVariant(_))))
  }
}
