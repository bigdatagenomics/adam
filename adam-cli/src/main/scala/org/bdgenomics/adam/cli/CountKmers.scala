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

import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.util.ParquetLogger
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import java.util.logging.Level

object CountKmers extends ADAMCommandCompanion {
  val commandName = "count_kmers"
  val commandDescription = "Counts the k-mers/q-mers from a read dataset."

  def apply(cmdLine: Array[String]) = {
    new CountKmers(Args4j[CountKmersArgs](cmdLine))
  }
}

class CountKmersArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM, BAM or SAM file to count kmers from", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location for storing k-mer counts", index = 1)
  var outputPath: String = null
  @Argument(required = true, metaVar = "KMER_LENGTH", usage = "Length of k-mers", index = 2)
  var kmerLength: Int = 0
  @Args4jOption(required = false, name = "-countQmers", usage = "Counts q-mers instead of k-mers.")
  var countQmers: Boolean = false
  @Args4jOption(required = false, name = "-printHistogram", usage = "Prints a histogram of counts.")
  var printHistogram: Boolean = false
}

class CountKmers(protected val args: CountKmersArgs) extends ADAMSparkCommand[CountKmersArgs] with Logging {
  val companion = CountKmers

  def run(sc: SparkContext, job: Job) {

    // Quiet Parquet...
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    // read from disk
    var adamRecords: RDD[ADAMRecord] = sc.adamLoad(args.inputPath)

    if (args.repartition != -1) {
      log.info("Repartitioning reads to '%d' partitions".format(args.repartition))
      adamRecords = adamRecords.repartition(args.repartition)
    }

    // count kmers
    val countedKmers = if (args.countQmers) {
      adamRecords.adamCountQmers(args.kmerLength)
    } else {
      adamRecords.adamCountKmers(args.kmerLength).map(kv => (kv._1, kv._2.toDouble))
    }

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
    if (args.countQmers) {
      countedKmers.map(kv => kv._1 + ", " + kv._2)
    } else {
      countedKmers.map(kv => kv._1 + ", " + kv._2.toLong)
    }.saveAsTextFile(args.outputPath)
  }

}
