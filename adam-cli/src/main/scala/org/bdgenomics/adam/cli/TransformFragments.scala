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

import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.bdgenomics.adam.cli.FileSystemUtils._
import org.bdgenomics.adam.io.FastqRecordReader
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.read.QualityScoreBin
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object TransformFragments extends BDGCommandCompanion {
  val commandName = "transformFragments"
  val commandDescription = "Convert alignments into fragment records."

  def apply(cmdLine: Array[String]) = {
    new TransformFragments(Args4j[TransformFragmentsArgs](cmdLine))
  }
}

class TransformFragmentsArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The Fragment file to apply the transforms to", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the transformed fragments", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-load_as_alignments", usage = "Treats the input data as alignments")
  var loadAsAlignments: Boolean = false

  @Args4jOption(required = false, name = "-save_as_alignments", usage = "Saves the output data as alignments")
  var saveAsAlignments: Boolean = false

  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false

  @Args4jOption(required = false, name = "-sort_by_read_name", usage = "Sort alignments by read name. Only valid with -save_as_alignments.")
  var sortByReadName: Boolean = false

  @Args4jOption(required = false, name = "-sort_by_reference_position", usage = "Sort alignments by reference position, with references ordered by name. Only valid with -save_as_alignments.")
  var sortByReferencePosition: Boolean = false

  @Args4jOption(required = false, name = "-sort_by_reference_position_and_index", usage = "Sort alignments by reference position, with references ordered by index. Only valid with -save_as_alignments.")
  var sortByReferencePositionAndIndex: Boolean = false

  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output")
  var deferMerging: Boolean = false

  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false

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
    checkWriteablePath(args.outputPath, sc.hadoopConfiguration)

    if (args.loadAsAlignments && args.saveAsAlignments) {
      warn("If loading and saving as alignments, consider using TransformAlignments instead")
    }
    if (args.sortByReadName || args.sortByReferencePosition || args.sortByReferencePositionAndIndex) {
      require(args.saveAsAlignments,
        "-sort_by_* flags are only valid if -save_as_alignments is given")
    }
    if (Seq(args.sortByReadName, args.sortByReferencePosition, args.sortByReferencePositionAndIndex).count(b => b) > 1) {
      throw new IllegalArgumentException(
        "only one of -sort_by_name, -sort_by_reference_position, and -sort_by_reference_position_and_index may be specified"
      )
    }
    if (args.maxReadLength > 0) {
      FastqRecordReader.setMaxReadLength(sc.hadoopConfiguration, args.maxReadLength)
    }

    val rdd = if (args.loadAsAlignments) {
      sc.loadAlignments(args.inputPath)
        .toFragments
    } else {
      sc.loadFragments(args.inputPath)
    }

    // should we bin the quality scores?
    val maybeBinnedReads = maybeBin(rdd)

    // should we dedupe the reads?
    val maybeDedupedReads = maybeDedupe(maybeBinnedReads)

    if (args.saveAsAlignments) {
      // save rdd as alignments
      val alignmentRdd = maybeDedupedReads.toAlignments

      // prep to save
      val finalRdd = if (args.sortByReadName) {
        alignmentRdd.sortByReadName()
      } else if (args.sortByReferencePosition) {
        alignmentRdd.sortByReferencePosition()
      } else if (args.sortByReferencePositionAndIndex) {
        alignmentRdd.sortByReferencePositionAndIndex()
      } else {
        alignmentRdd
      }

      // save the file
      finalRdd.save(args,
        isSorted = args.sortByReadName || args.sortByReferencePosition || args.sortByReferencePositionAndIndex)
    } else {
      maybeDedupedReads.saveAsParquet(args)
    }
  }
}
