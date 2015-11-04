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

import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.NucleotideContigFragmentField._
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4JOption }

class ADAM2FastaArgs extends ParquetLoadSaveArgs {
  @Args4JOption(required = false, name = "-coalesce", usage = "Choose the number of partitions to coalesce down to.")
  var coalesce: Int = -1
  @Args4JOption(required = false, name = "-force_shuffle_coalesce", usage = "Force shuffle while partitioning, default false.")
  var forceShuffle: Boolean = false
  @Args4JOption(required = false, name = "-line_width", usage = "Hard wrap FASTA formatted sequence at line width, default 60")
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
    val proj = Projection(contig, description, fragmentNumber, numberOfFragmentsInContig, fragmentSequence)

    log.info("Loading ADAM nucleotide contig fragments from disk.")
    val contigFragments: RDD[NucleotideContigFragment] = sc.loadParquet(args.inputPath, projection = Some(proj))

    log.info("Merging fragments and writing FASTA to disk.")
    val contigs = contigFragments
      .mergeFragments()
    val cc = if (args.coalesce > 0) {
      if (args.coalesce > contigs.partitions.size || args.forceShuffle) {
        contigs.coalesce(args.coalesce, shuffle = true)
      } else {
        contigs.coalesce(args.coalesce, shuffle = false)
      }
    } else {
      contigs
    }
    cc.saveAsFasta(args.outputPath, args.lineWidth)
  }
}
