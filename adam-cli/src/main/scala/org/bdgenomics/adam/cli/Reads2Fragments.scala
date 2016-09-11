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
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Reads2Fragments extends BDGCommandCompanion {
  val commandName = "reads2fragments"
  val commandDescription = "Convert alignment records into fragment records."

  def apply(cmdLine: Array[String]) = {
    new Reads2Fragments(Args4j[Reads2FragmentsArgs](cmdLine))
  }
}

class Reads2FragmentsArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM, interleaved FASTQ, BAM, or SAM file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the transformed data in ADAM/Parquet format", index = 1)
  var outputPath: String = null

  // these are required because of the ADAMSaveAnyArgs trait... fix this trait???
  var asSingleFile = false
  var sortFastqOutput = false
  var deferMerging = false
}

class Reads2Fragments(protected val args: Reads2FragmentsArgs) extends BDGSparkCommand[Reads2FragmentsArgs] with Logging {
  val companion = Reads2Fragments

  def run(sc: SparkContext) {
    sc.loadAlignments(args.inputPath)
      .toFragments
      .saveAsParquet(args)
  }
}
