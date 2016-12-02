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
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object CountReadKmers extends BDGCommandCompanion {
  val commandName = "countKmers"
  val commandDescription = "Counts the k-mers/q-mers from a read dataset."

  def apply(cmdLine: Array[String]) = {
    new CountReadKmers(Args4j[CountReadKmersArgs](cmdLine))
  }
}

class CountReadKmersArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM, BAM or SAM file to count kmers from", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location for storing k-mer counts", index = 1)
  var outputPath: String = null
  @Argument(required = true, metaVar = "KMER_LENGTH", usage = "Length of k-mers", index = 2)
  var kmerLength: Int = 0
  @Args4jOption(required = false, name = "-print_histogram", usage = "Prints a histogram of counts.")
  var printHistogram: Boolean = false
  @Args4jOption(required = false, name = "-repartition", usage = "Set the number of partitions to map data to")
  var repartition: Int = -1
}

class CountReadKmers(protected val args: CountReadKmersArgs) extends BDGSparkCommand[CountReadKmersArgs] with Logging {
  val companion = CountReadKmers

  def run(sc: SparkContext) {

    // read from disk
    var adamRecords = sc.loadAlignments(
      args.inputPath,
      projection = Some(Projection(AlignmentRecordField.sequence))
    )

    if (args.repartition != -1) {
      log.info("Repartitioning reads to '%d' partitions".format(args.repartition))
      adamRecords = adamRecords.transform(_.repartition(args.repartition))
    }

    // count kmers
    val countedKmers = adamRecords.countKmers(args.kmerLength)

    // cache counted kmers
    countedKmers.cache()

    // print histogram, if requested
    if (args.printHistogram) {
      countedKmers.map(kv => kv._2.toLong)
        .countByValue()
        .toSeq
        .sortBy(kv => kv._1)
        .foreach(println)
    }

    // save as text file
    countedKmers.saveAsTextFile(args.outputPath)
  }
}
