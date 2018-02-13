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
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

class ADAM2FastaArgs extends Args4jBase {
  @Argument(required = true, metaVar = "ADAM", usage = "The Parquet file to convert", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "FASTA", usage = "Location to write the FASTA to", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-single", usage = "Saves FASTA as single file")
  var asSingleFile: Boolean = false
  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output")
  var disableFastConcat: Boolean = false
  @Args4jOption(required = false, name = "-coalesce", usage = "Choose the number of partitions to coalesce down to.")
  var coalesce: Int = -1
  @Args4jOption(required = false, name = "-force_shuffle_coalesce", usage = "Force shuffle while partitioning, default false.")
  var forceShuffle: Boolean = false
  @Args4jOption(required = false, name = "-line_width", usage = "Hard wrap FASTA formatted sequence at line width, default 60")
  var lineWidth: Int = 60
}

object ADAM2Fasta extends BDGCommandCompanion {
  override val commandName = "adam2fasta"
  override val commandDescription = "Convert ADAM nucleotide contig fragments to FASTA files"

  override def apply(cmdLine: Array[String]): ADAM2Fasta =
    new ADAM2Fasta(Args4j[ADAM2FastaArgs](cmdLine))
}

class ADAM2Fasta(val args: ADAM2FastaArgs) extends BDGSparkCommand[ADAM2FastaArgs] with Logging {
  override val companion = ADAM2Fasta

  override def run(sc: SparkContext): Unit = {

    log.info("Loading ADAM nucleotide contig fragments from disk.")
    val contigFragments = sc.loadContigFragments(args.inputPath)

    log.info("Merging fragments and writing FASTA to disk.")
    val contigs = contigFragments.mergeFragments()

    val cc = if (args.coalesce > 0) {
      if (args.coalesce > contigs.rdd.partitions.length || args.forceShuffle) {
        contigs.transform(_.coalesce(args.coalesce, shuffle = true))
      } else {
        contigs.transform(_.coalesce(args.coalesce, shuffle = false))
      }
    } else {
      contigs
    }
    cc.saveAsFasta(
      args.outputPath,
      args.lineWidth,
      asSingleFile = args.asSingleFile,
      disableFastConcat = args.disableFastConcat
    )
  }
}
