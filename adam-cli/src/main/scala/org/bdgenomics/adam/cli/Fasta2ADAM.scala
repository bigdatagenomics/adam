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

import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{ SparkContext, Logging }
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }

object Fasta2ADAM extends ADAMCommandCompanion {
  val commandName: String = "fasta2adam"
  val commandDescription: String = "Converts a text FASTA sequence file into an ADAMNucleotideContig Parquet file which represents assembled sequences."

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new Fasta2ADAM(Args4j[Fasta2ADAMArgs](cmdLine))
  }
}

class Fasta2ADAMArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "FASTA", usage = "The FASTA file to convert", index = 0)
  var fastaFile: String = null
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM data", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-verbose", usage = "Prints enhanced debugging info, including contents of seq dict.")
  var verbose: Boolean = false
  @Args4jOption(required = false, name = "-reads", usage = "Maps contig IDs to match contig IDs of reads.")
  var reads: String = ""
  @Args4jOption(required = false, name = "-fragment_length", usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
  var fragmentLength: Long = 10000L
}

class Fasta2ADAM(protected val args: Fasta2ADAMArgs) extends ADAMSparkCommand[Fasta2ADAMArgs] with Logging {
  val companion = Fasta2ADAM

  def run(sc: SparkContext, job: Job) {
    log.info("Loading FASTA data from disk.")
    val adamFasta = sc.adamSequenceLoad(args.fastaFile, args.fragmentLength)
    if (args.verbose) {
      println("FASTA contains:")
      println(adamFasta.adamGetSequenceDictionary())
    }

    val remapped = if (args.reads != "") {
      val readDict = sc.adamDictionaryLoad[ADAMRecord](args.reads)

      if (args.verbose) {
        println("Remapping with:")
        println(readDict)
      }

      val remapFasta = adamFasta.adamRewriteContigIds(readDict)

      if (args.verbose) {
        println("After remapping, have:")
        println(remapFasta.adamGetSequenceDictionary())
      }

      remapFasta
    } else {
      adamFasta
    }

    log.info("Writing records to disk.")
    remapped.adamSave(args.outputPath, blockSize = args.blockSize, pageSize = args.pageSize,
      compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
  }
}

