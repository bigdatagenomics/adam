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
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option ⇒ Args4jOption }

object TransformSlices extends BDGCommandCompanion {
  val commandName = "transformSlices"
  val commandDescription = "Convert a FASTA file as slices into corresponding ADAM format and vice versa"

  def apply(cmdLine: Array[String]) = {
    new TransformSlices(Args4j[TransformSlicesArgs](cmdLine))
  }
}

class TransformSlicesArgs extends Args4jBase with ParquetSaveArgs {
  @Argument(required = true, metaVar = "INPUT",
    usage = "The slice file to convert (e.g., .fa, .fasta). If extension is not detected, Parquet is assumed.", index = 0)
  var slicesFile: String = _

  @Argument(required = true, metaVar = "OUTPUT",
    usage = "Location to write ADAM slice data. If extension is not detected, Parquet is assumed.", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-maximum_length",
    usage = "Maximum slice length. Defaults to 10000L.")
  var maximumLength: Long = 10000L

  @Args4jOption(required = false, name = "-create_reference",
    usage = "Create reference from sequence names and lengths. Defaults to false.")
  var createReference: Boolean = false

  @Args4jOption(required = false, name = "-single",
    usage = "Save as a single file, for the text formats.")
  var single: Boolean = false

  @Args4jOption(required = false, name = "-disable_fast_concat",
    usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false
}

class TransformSlices(val args: TransformSlicesArgs)
    extends BDGSparkCommand[TransformSlicesArgs] {

  val companion = TransformSlices

  def run(sc: SparkContext) {
    val slices = sc.loadSlices(
      args.slicesFile,
      maximumLength = args.maximumLength,
      optPredicate = None,
      optProjection = None
    )
    val maybeCreateReference = if (args.createReference) slices.createSequenceDictionary() else slices
    maybeCreateReference.save(args.outputPath, args.single, args.disableFastConcat)
  }
}
