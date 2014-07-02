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
import org.bdgenomics.adam.predicates.UniqueMappedReadPredicate
import org.kohsuke.args4j.{ Option => option, Argument }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ ADAMPileup, ADAMRecord }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Reads2Ref extends ADAMCommandCompanion {
  val commandName: String = "reads2ref"
  val commandDescription: String = "Convert an ADAM read-oriented file to an ADAM reference-oriented file"

  def apply(cmdLine: Array[String]) = {
    new Reads2Ref(Args4j[Reads2RefArgs](cmdLine))
  }
}

object Reads2RefArgs {
  val MIN_MAPQ_DEFAULT: Long = 30L
}

class Reads2RefArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(metaVar = "ADAMREADS", required = true, usage = "ADAM read-oriented data", index = 0)
  var readInput: String = _

  @Argument(metaVar = "DIR", required = true, usage = "Location to create reference-oriented ADAM data", index = 1)
  var pileupOutput: String = _

  @option(name = "-mapq", usage = "Minimal mapq value allowed for a read (default = 30)")
  var minMapq: Long = Reads2RefArgs.MIN_MAPQ_DEFAULT

  @option(name = "-aggregate", usage = "Aggregates data at each pileup position, to reduce storage cost.")
  var aggregate: Boolean = false

  @option(name = "-allowNonPrimaryAlignments", usage = "Converts reads that are not at their primary alignment positions to pileups.")
  var nonPrimary: Boolean = true
}

class Reads2Ref(protected val args: Reads2RefArgs) extends ADAMSparkCommand[Reads2RefArgs] {
  val companion = Reads2Ref

  def run(sc: SparkContext, job: Job) {
    val reads: RDD[ADAMRecord] = sc.adamLoad(args.readInput, Some(classOf[UniqueMappedReadPredicate]))

    val readCount = reads.count()

    val pileups: RDD[ADAMPileup] = reads.adamRecords2Pileup(args.nonPrimary)

    val pileupCount = pileups.count()

    val coverage = pileupCount / readCount

    if (args.aggregate) {
      pileups.adamAggregatePileups(coverage.toInt).adamSave(args.pileupOutput,
        blockSize = args.blockSize, pageSize = args.pageSize, compressCodec = args.compressionCodec,
        disableDictionaryEncoding = args.disableDictionary)
    } else {
      pileups.adamSave(args.pileupOutput, blockSize = args.blockSize, pageSize = args.pageSize,
        compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
    }
  }
}
