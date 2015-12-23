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
import org.bdgenomics.adam.models.{ RecordGroupDictionary, SequenceDictionary }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Fragments2Reads extends BDGCommandCompanion {
  val commandName = "fragments2reads"
  val commandDescription = "Convert alignment records into fragment records."

  def apply(cmdLine: Array[String]) = {
    new Fragments2Reads(Args4j[Fragments2ReadsArgs](cmdLine))
  }
}

class Fragments2ReadsArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The Fragment file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the transformed data in ADAM/Parquet format", index = 1)
  var outputPath: String = null

  // these are required because of the ADAMSaveAnyArgs trait... fix this trait???
  var asSingleFile = false
  var sortFastqOutput = false
}

class Fragments2Reads(protected val args: Fragments2ReadsArgs) extends BDGSparkCommand[Fragments2ReadsArgs] with Logging {
  val companion = Fragments2Reads

  def run(sc: SparkContext) {
    sc.loadFragments(args.inputPath)
      .toReads
      .adamSave(args,
        SequenceDictionary.empty,
        RecordGroupDictionary.empty)
  }
}
