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
import org.kohsuke.args4j.{ Argument, Option ⇒ Args4jOption }

object TransformFeatures extends BDGCommandCompanion {
  val commandName = "transformFeatures"
  val commandDescription = "Convert a file with sequence features into corresponding ADAM format and vice versa"

  def apply(cmdLine: Array[String]) = {
    new TransformFeatures(Args4j[TransformFeaturesArgs](cmdLine))
  }
}

class TransformFeaturesArgs extends Args4jBase with ParquetSaveArgs {
  @Argument(required = true, metaVar = "INPUT",
    usage = "The features file to convert (e.g., .bed, .gff/.gtf, .gff3, .interval_list, .narrowPeak). If extension is not detected, Parquet is assumed.", index = 0)
  var featuresFile: String = _

  @Argument(required = true, metaVar = "OUTPUT",
    usage = "Location to write ADAM features data. If extension is not detected, Parquet is assumed.", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-num_partitions",
    usage = "Number of partitions to load an interval file using.")
  var numPartitions: Int = _

  @Args4jOption(required = false, name = "-single",
    usage = "Save as a single file, for the text formats.")
  var single: Boolean = false
}

class TransformFeatures(val args: TransformFeaturesArgs)
    extends BDGSparkCommand[TransformFeaturesArgs] {
  val companion = TransformFeatures

  def run(sc: SparkContext) {
    sc.loadFeatures(args.featuresFile, None, Option(args.numPartitions))
      .save(args.outputPath, args.single)
  }
}
