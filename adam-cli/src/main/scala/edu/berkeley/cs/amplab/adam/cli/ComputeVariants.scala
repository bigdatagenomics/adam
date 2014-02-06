/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.kohsuke.args4j.{Option => option, Argument}

object ComputeVariants extends AdamCommandCompanion {
  val commandName: String = "compute_variants"
  val commandDescription: String = "Compute variant data from genotypes"

  def apply(cmdLine: Array[String]) = {
    new ComputeVariants(Args4j[ComputeVariantsArgs](cmdLine))
  }
}

class ComputeVariantsArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(metaVar = "GENOTYPES", required = true, usage = "ADAM genotype data", index = 0)
  var input: String = _

  @Argument(metaVar = "VARIANTS", required = true, usage = "Location to create ADAM variant context data", index = 1)
  var output: String = _

  @option(name = "-saveVariantsOnly", usage = "Only save ADAMVariants, not variant contexts.")
  var variantsOnly: Boolean = false

  @option(name = "-runValidation", usage = "Run validation on genotypes and print errors.")
  var validation: Boolean = false

  @option(name = "-runStrictValidation", usage = "Run validation on genotypes and stop on errors.")
  var strictValidation: Boolean = false
}

class ComputeVariants(protected val args: ComputeVariantsArgs) extends AdamSparkCommand[ComputeVariantsArgs] {
  val companion = ComputeVariants

  def run(sc: SparkContext, job: Job) {


  }
}
