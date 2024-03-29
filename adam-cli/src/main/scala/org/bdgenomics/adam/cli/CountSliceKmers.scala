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
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.cli.FileSystemUtils._
import org.bdgenomics.adam.util.TextRddWriter._
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object CountSliceKmers extends BDGCommandCompanion {
  val commandName = "countSliceKmers"
  val commandDescription = "Counts the k-mers/q-mers from a slice dataset."

  def apply(cmdLine: Array[String]) = {
    new CountSliceKmers(Args4j[CountSliceKmersArgs](cmdLine))
  }
}

class CountSliceKmersArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM or FASTA file to count kmers from.", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location for storing k-mer counts.", index = 1)
  var outputPath: String = null

  @Argument(required = true, metaVar = "KMER_LENGTH", usage = "Length of k-mers.", index = 2)
  var kmerLength: Int = 0

  @Args4jOption(required = false, name = "-print_histogram", usage = "Prints a histogram of counts.")
  var printHistogram: Boolean = false

  @Args4jOption(required = false, name = "-maximum_length", usage = "Maximum slice length. Defaults to 10000L.")
  var maximumLength: Long = 10000L

  @Args4jOption(required = false, name = "-sort", usage = "Sort kmers before writing.")
  var sort: Boolean = false

  @Args4jOption(required = false, name = "-single", usage = "Save as a single file, for text format.")
  var asSingleFile: Boolean = false

  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false
}

class CountSliceKmers(protected val args: CountSliceKmersArgs) extends BDGSparkCommand[CountSliceKmersArgs] with Logging {
  val companion = CountSliceKmers

  def run(sc: SparkContext) {
    checkWriteablePath(args.outputPath, sc.hadoopConfiguration)

    // read from disk
    val slices = sc.loadSlices(args.inputPath, maximumLength = args.maximumLength)
    val withReferences = if (slices.references.size == 0) slices.createReferences() else slices

    // count kmers
    val countedKmers = withReferences.countKmers(args.kmerLength)

    // print histogram, if requested
    if (args.printHistogram) {
      // cache counted kmers
      countedKmers.cache()

      countedKmers.map(kv => kv._2.toLong)
        .countByValue()
        .toSeq
        .sortBy(kv => kv._1)
        .foreach(println)
    }

    val maybeSorted = if (args.sort) countedKmers.sortBy(_._1) else countedKmers

    // save as text file
    writeTextRdd(maybeSorted.map(kv => kv._1 + "\t" + kv._2),
      args.outputPath,
      asSingleFile = args.asSingleFile,
      disableFastConcat = args.disableFastConcat)
  }
}
