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
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.formats.avro.Alphabet
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option ⇒ Args4jOption }

object TransformSequences extends BDGCommandCompanion {
  val commandName = "transformSequences"
  val commandDescription = "Convert a FASTA file as sequences into corresponding ADAM format and vice versa"

  def apply(cmdLine: Array[String]) = {
    new TransformSequences(Args4j[TransformSequencesArgs](cmdLine))
  }
}

class TransformSequencesArgs extends Args4jBase with ParquetSaveArgs {
  @Argument(required = true, metaVar = "INPUT",
    usage = "The sequence file to convert (e.g., .fa, .fasta). If extension is not detected, Parquet is assumed.", index = 0)
  var sequencesFile: String = _

  @Argument(required = true, metaVar = "OUTPUT",
    usage = "Location to write ADAM sequence data. If extension is not detected, Parquet is assumed.", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-create_reference",
    usage = "Create reference from sequence names and lengths. Defaults to false.")
  var createReference: Boolean = false

  @Args4jOption(required = false, name = "-single",
    usage = "Save as a single file, for the text formats.")
  var single: Boolean = false

  @Args4jOption(required = false, name = "-alphabet",
    usage = "Alphabet in which to interpret the loaded sequences { DNA, PROTEIN, RNA }. Defaults to Alphabet.DNA.")
  var alphabet: String = "DNA"

  @Args4jOption(required = false, name = "-disable_fast_concat",
    usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false
}

class TransformSequences(val args: TransformSequencesArgs)
    extends BDGSparkCommand[TransformSequencesArgs] {

  val companion = TransformSequences
  val alphabet = Alphabet.valueOf(args.alphabet)

  def run(sc: SparkContext) {
    val sequences = alphabet match {
      case Alphabet.DNA     => sc.loadDnaSequences(args.sequencesFile, optPredicate = None, optProjection = None)
      case Alphabet.PROTEIN => sc.loadProteinSequences(args.sequencesFile, optPredicate = None, optProjection = None)
      case Alphabet.RNA     => sc.loadRnaSequences(args.sequencesFile, optPredicate = None, optProjection = None)
    }
    val maybeCreateReference = if (args.createReference) sequences.createSequenceDictionary() else sequences
    maybeCreateReference.save(args.outputPath, args.single, args.disableFastConcat)
  }
}
