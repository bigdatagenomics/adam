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
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.cli.FileSystemUtils._
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.ADAMSaveAnyArgs
import org.bdgenomics.formats.avro.Alignment
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

class ViewArgs extends Args4jBase with ParquetArgs with ADAMSaveAnyArgs with CramArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM, BAM or SAM file to view", index = 0)
  var inputPath: String = null

  // left null until constructor
  var outputPath: String = null

  @Args4jOption(
    required = false,
    name = "-f",
    metaVar = "N",
    usage = "Restrict to reads that match all of the bits in <N>"
  )
  var matchAllBits: Int = 0

  @Args4jOption(
    required = false,
    name = "-F",
    metaVar = "N",
    usage = "Restrict to reads that match none of the bits in <N>"
  )
  var mismatchAllBits: Int = 0

  @Args4jOption(
    required = false,
    name = "-g",
    metaVar = "N",
    usage = "Restrict to reads that match any of the bits in <N>"
  )
  var matchSomeBits: Int = 0

  @Args4jOption(
    required = false,
    name = "-G",
    metaVar = "N",
    usage = "Restrict to reads that mismatch at least one of the bits in <N>"
  )
  var mismatchSomeBits: Int = 0

  @Args4jOption(
    required = false,
    name = "-c",
    usage = "Print count of matching records, instead of the records themselves"
  )
  var printCount: Boolean = false

  @Args4jOption(
    required = false,
    name = "-o",
    metaVar = "<FILE>",
    usage = "Output to <FILE>; can also pass <FILE> as the second argument"
  )
  var outputPathArg: String = null

  @Args4jOption(required = false, name = "-single",
    usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false
  @Args4jOption(required = false, name = "-defer_merging",
    usage = "Defers merging single file output")
  var deferMerging: Boolean = false
  @Args4jOption(required = false, name = "-disable_fast_concat",
    usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false

  // required by ADAMAnySaveArgs
  var sortFastqOutput: Boolean = false
}

object View extends BDGCommandCompanion {
  val commandName = "view"
  val commandDescription = "View certain reads from an alignment-record file."

  def apply(cmdLine: Array[String]): View = {
    val args = Args4j[ViewArgs](cmdLine)
    if (args.outputPath == null && args.outputPathArg != null) {
      args.outputPath = args.outputPathArg
    }
    new View(args)
  }
}

/**
 * The `adam view` command implements some of the functionality of `samtools view`, specifically the -f, -F, -c, and -o
 * options, in an optionally distributed fashion.
 *
 * It is agnostic to its input and output being SAM, BAM, or ADAM files; when printing to stdout it prints SAM.
 */
class View(val args: ViewArgs) extends BDGSparkCommand[ViewArgs] {
  val companion = View

  type ReadFilter = (Alignment => Boolean)

  def getFilters(n: Int, matchValue: Boolean = true): List[ReadFilter] = {
    def getFilter(bit: Int, fn: ReadFilter): Option[ReadFilter] =
      if ((n & bit) > 0)
        Some(
          fn(_) == matchValue
        )
      else
        None

    List(
      getFilter(0x1, _.getReadPaired),
      getFilter(0x2, _.getProperPair),
      getFilter(0x4, !_.getReadMapped),

      // NOTE(ryan): for the "mate unmapped" flag, ADAM stores the inversion of what SAM stores (i.e. "mapped" vs.
      // "unmapped"); however, they will each generally default to false if the read is not paired (in which case the
      // "mate" doesn't exist). 0x8 is only really true if the read is paired *and* the mate is not mapped; simply
      // seeing the "mate mapped" flag set to false could just be an artifact of the read not being paired, so we add
      // the extra check here and below.
      getFilter(0x8, read => read.getReadPaired && !read.getMateMapped),
      getFilter(0x10, _.getReadNegativeStrand),
      getFilter(0x20, _.getMateNegativeStrand),
      getFilter(0x40, _.getReadInFragment == 0),
      getFilter(0x80, _.getReadInFragment == 1),
      getFilter(0x100, !_.getPrimaryAlignment),
      getFilter(0x200, _.getFailedVendorQualityChecks),
      getFilter(0x400, _.getDuplicateRead),
      getFilter(0x800, _.getSupplementaryAlignment)
    ).flatten
  }

  def applyFilters(reads: RDD[Alignment]): RDD[Alignment] = {
    val matchAllFilters: List[ReadFilter] = getFilters(args.matchAllBits, matchValue = true)
    val mismatchAllFilters: List[ReadFilter] = getFilters(args.mismatchAllBits, matchValue = false)
    val allFilters = matchAllFilters ++ mismatchAllFilters

    val matchSomeFilters: List[ReadFilter] = getFilters(args.matchSomeBits, matchValue = true)
    val mismatchSomeFilters: List[ReadFilter] = getFilters(args.mismatchSomeBits, matchValue = false)
    val someFilters = matchSomeFilters ++ mismatchSomeFilters

    if (allFilters.nonEmpty || someFilters.nonEmpty) {
      reads.filter(read =>
        allFilters.forall(_(read)) &&
          (matchSomeFilters.isEmpty || matchSomeFilters.exists(_(read))) &&
          (mismatchSomeFilters.isEmpty || mismatchSomeFilters.exists(_(read))))
    } else
      reads
  }

  def run(sc: SparkContext) = {
    checkWriteablePath(args.outputPath, sc.hadoopConfiguration)

    args.configureCramFormat(sc)

    val reads = sc.loadAlignments(args.inputPath)
      .transform((rdd: RDD[Alignment]) => applyFilters(rdd))

    if (args.outputPath != null) {
      reads.save(args)
    } else {
      if (args.printCount) {
        println(reads.rdd.count())
      } else {
        println(reads.saveAsSamString())
      }
    }
  }
}
