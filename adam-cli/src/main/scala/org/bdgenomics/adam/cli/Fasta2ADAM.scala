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
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Fasta2ADAM extends BDGCommandCompanion {
  val commandName: String = "fasta2adam"
  val commandDescription: String = "Converts a text FASTA sequence file into an ADAMNucleotideContig Parquet file which represents assembled sequences."

  def apply(cmdLine: Array[String]) = {
    new Fasta2ADAM(Args4j[Fasta2ADAMArgs](cmdLine))
  }
}

class Fasta2ADAMArgs extends Args4jBase with ParquetSaveArgs {
  @Argument(required = true, metaVar = "FASTA", usage = "The FASTA file to convert", index = 0)
  var fastaFile: String = null
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM data", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-verbose", usage = "Prints enhanced debugging info, including contents of seq dict.")
  var verbose: Boolean = false
  @Args4jOption(required = false, name = "-reads", usage = "Maps contig IDs to match contig IDs of reads.")
  var reads: String = ""
  @Args4jOption(required = false, name = "-fragment_length", usage = "Sets maximum fragment length. Default value is 10,000. Values greater than 1e9 should be avoided.")
  var maximumLength: Long = 10000L
  @Args4jOption(required = false, name = "-repartition", usage = "Sets the number of output partitions to write, if desired.")
  var partitions: Int = -1
}

class Fasta2ADAM(protected val args: Fasta2ADAMArgs) extends BDGSparkCommand[Fasta2ADAMArgs] with Logging {
  val companion = Fasta2ADAM

  def run(sc: SparkContext) {
    checkWriteablePath(args.outputPath, sc.hadoopConfiguration)

    info("Loading FASTA data from disk.")
    val adamFasta = sc.loadFasta(args.fastaFile, maximumLength = args.maximumLength)

    if (args.verbose) {
      info("FASTA contains: %s".format(adamFasta.sequences.toString))
    }

    info("Writing records to disk.")
    val finalFasta = if (args.partitions > 0) {
      adamFasta.transform(_.repartition(args.partitions))
    } else {
      adamFasta
    }

    finalFasta.saveAsParquet(args)
  }
}

