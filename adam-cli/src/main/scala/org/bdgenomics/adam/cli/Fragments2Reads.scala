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
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Fragments2Reads extends BDGCommandCompanion {
  val commandName = "fragments2reads"
  val commandDescription = "Convert alignment records into fragment records."

  def apply(cmdLine: Array[String]) = {
    new Fragments2Reads(Args4j[Fragments2ReadsArgs](cmdLine))
  }
}

class Fragments2ReadsArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "FRAGMENTS", usage = "The Fragment file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "READS", usage = "Location to write the transformed data as reads", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false
  @Args4jOption(required = false, name = "-sort_reads", usage = "Sort the reads by referenceId and read position")
  var sortReads: Boolean = false
  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output")
  var deferMerging: Boolean = false
  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false
  @Args4jOption(required = false, name = "-sort_lexicographically", usage = "Sort the reads lexicographically by contig name, instead of by index.")
  var sortLexicographically: Boolean = false
  @Args4jOption(required = false, name = "-mark_duplicate_reads", usage = "Mark duplicate reads")
  var markDuplicates: Boolean = false

  // this is required because of the ADAMSaveAnyArgs trait... fix this trait???
  var sortFastqOutput = false
}

class Fragments2Reads(protected val args: Fragments2ReadsArgs) extends BDGSparkCommand[Fragments2ReadsArgs] with Logging {
  val companion = Fragments2Reads

  def maybeDedupe(reads: FragmentRDD): FragmentRDD = {
    if (args.markDuplicates) {
      reads.markDuplicates()
    } else {
      reads
    }
  }

  def run(sc: SparkContext) {
    val rdd = sc.loadFragments(args.inputPath)

    // should we dedupe the reads?
    val maybeDedupedReads = maybeDedupe(rdd)

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
    finalRdd.save(args)
  }
}
