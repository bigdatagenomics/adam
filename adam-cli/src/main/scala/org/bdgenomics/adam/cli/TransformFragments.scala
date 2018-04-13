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

import org.apache.spark.SparkContext
import org.bdgenomics.adam.io.FastqRecordReader
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.QualityScoreBin
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object TransformFragments extends BDGCommandCompanion {
  val commandName = "transformFragments"
  val commandDescription = "Convert alignment records into fragment records."

  def apply(cmdLine: Array[String]) = {
    new TransformFragments(Args4j[TransformFragmentsArgs](cmdLine))
  }
}

class TransformFragmentsArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The Fragment file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the transformed fragments", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-load_as_reads", usage = "Treats the input data as reads")
  var loadAsReads: Boolean = false
  @Args4jOption(required = false, name = "-save_as_reads", usage = "Saves the output data as reads")
  var saveAsReads: Boolean = false
  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false
  @Args4jOption(required = false, name = "-sort_reads", usage = "Sort the reads by referenceId and read position. Only valid if run with -save_as_reads")
  var sortReads: Boolean = false
  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output")
  var deferMerging: Boolean = false
  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false
  @Args4jOption(required = false, name = "-sort_lexicographically", usage = "Sort the reads lexicographically by contig name, instead of by index.")
  var sortLexicographically: Boolean = false
  @Args4jOption(required = false, name = "-mark_duplicate_reads", usage = "Mark duplicate reads")
  var markDuplicates: Boolean = false
  @Args4jOption(required = false, name = "-bin_quality_scores", usage = "Rewrites quality scores of reads into bins from a string of bin descriptions, e.g. 0,20,10;20,40,30.")
  var binQualityScores: String = null
  @Args4jOption(required = false, name = "-max_read_length", usage = "Maximum FASTQ read length, defaults to 10,000 base pairs (bp).")
  var maxReadLength: Int = 0

  // this is required because of the ADAMSaveAnyArgs trait... fix this trait???
  var sortFastqOutput = false
}

class TransformFragments(protected val args: TransformFragmentsArgs) extends BDGSparkCommand[TransformFragmentsArgs] with Logging {
  val companion = TransformFragments

  /**
   * @param reads An RDD of fragments.
   * @return If the mark duplicates argument is sent, deduplicates the reads.
   *   Else, returns the input reads.
   */
  def maybeDedupe(reads: FragmentDataset): FragmentDataset = {
    if (args.markDuplicates) {
      reads.markDuplicates()
    } else {
      reads
    }
  }

  /**
   * @param rdd An RDD of fragments.
   * @return If the binQualityScores argument is set, rewrites the quality scores of the
   *   reads into bins. Else, returns the original RDD.
   */
  private def maybeBin(rdd: FragmentDataset): FragmentDataset = {
    Option(args.binQualityScores).fold(rdd)(binDescription => {
      val bins = QualityScoreBin(binDescription)
      rdd.binQualityScores(bins)
    })
  }

  def run(sc: SparkContext) {
    if (args.loadAsReads && args.saveAsReads) {
      log.warn("If loading and saving as reads, consider using TransformAlignments instead.")
    }
    if (args.sortReads) {
      require(args.saveAsReads,
        "-sort_reads is only valid if -save_as_reads is given.")
    }
    if (args.sortLexicographically) {
      require(args.saveAsReads,
        "-sort_lexicographically is only valid if -save_as_reads is given.")
    }
    if (args.maxReadLength > 0) {
      FastqRecordReader.setMaxReadLength(sc.hadoopConfiguration, args.maxReadLength)
    }

    val rdd = if (args.loadAsReads) {
      sc.loadAlignments(args.inputPath)
        .toFragments
    } else {
      sc.loadFragments(args.inputPath)
    }

    // should we bin the quality scores?
    val maybeBinnedReads = maybeBin(rdd)

    // should we dedupe the reads?
    val maybeDedupedReads = maybeDedupe(maybeBinnedReads)

    if (args.saveAsReads) {
      // save rdd as reads
      val readRdd = maybeDedupedReads.toReads

      // prep to save
      val finalRdd = if (args.sortReads) {
        readRdd.sortReadsByReferencePosition()
      } else if (args.sortLexicographically) {
        readRdd.sortReadsByReferencePositionAndIndex()
      } else {
        readRdd
      }

      // save the file
      finalRdd.save(args,
        isSorted = args.sortReads || args.sortLexicographically)
    } else {
      maybeDedupedReads.saveAsParquet(args)
    }
  }
}
