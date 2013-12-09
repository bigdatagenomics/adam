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
package edu.berkeley.cs.amplab.adam.commands

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.{Option => option, Argument}
import edu.berkeley.cs.amplab.adam.predicates.LocusPredicate
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util._
import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotype, ADAMVariant}
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext

object ComputeVariants extends AdamCommandCompanion {
  val commandName: String = "computeVariants"
  val commandDescription: String = "Compute variant data from genotypes."

  def apply(cmdLine: Array[String]) = {
    new ComputeVariants(Args4j[ComputeVariantsArgs](cmdLine))
  }
}

class ComputeVariantsArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(metaVar = "GENOTYPES", required = true, usage = "ADAM genotype data", index = 0)
  var input: String = _

  @Argument(metaVar = "VARIANTS", required = true, usage = "Location to create ADAM variant context data", index = 1)
  var output: String = _

  @option (name = "-saveVariantsOnly", usage = "Only save ADAMVariants, not variant contexts.")
  var variantsOnly: Boolean = false

  @option (name = "-runValidation", usage = "Run validation on genotypes and print errors.")
  var validation: Boolean = false

  @option (name = "-runStrictValidation", usage = "Run validation on genotypes and stop on errors.")
  var strictValidation: Boolean = false
}

class ComputeVariants(protected val args: ComputeVariantsArgs) extends AdamSparkCommand[ComputeVariantsArgs] {
  val companion = ComputeVariants

  def run(sc: SparkContext, job: Job) {
    val genotypes: RDD[ADAMGenotype] = sc.adamLoad(args.input)

    // convert to variants
    val variants: RDD[ADAMVariant] = genotypes.adamConvertGenotypes(performValidation = args.validation || args.strictValidation,
                                                                    failOnValidationError = args.strictValidation)
    
    // save to disk
    if(args.variantsOnly) {
      variants.adamSave(args.output)
    } else {
      // convert to variant context, _then_ save
      val vc = ADAMVariantContext.mergeVariantsAndGenotypes(variants, genotypes)
      vc.adamSave(args.output)
    }
  }
}
