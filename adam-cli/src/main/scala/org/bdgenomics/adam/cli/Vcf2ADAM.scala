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
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }

object Vcf2ADAM extends BDGCommandCompanion {
  val commandName = "vcf2adam"
  val commandDescription = "Convert a VCF file to the corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Vcf2ADAM(Args4j[Vcf2ADAMArgs](cmdLine))
  }
}

class Vcf2ADAMArgs extends Args4jBase with ParquetSaveArgs {

  @Argument(required = true, metaVar = "VCF", usage = "The VCF file to convert", index = 0)
  var vcfPath: String = _

  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM Variant data", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-coalesce", usage = "Set the number of partitions written to the ADAM output directory")
  var coalesce: Int = -1

  @Args4jOption(required = false, name = "-force_shuffle_coalesce", usage = "Even if the repartitioned RDD has fewer partitions, force a shuffle.")
  var forceShuffle: Boolean = false

  @Args4jOption(required = false, name = "-only_variants", usage = "Output Variant objects instead of Genotypes")
  var onlyVariants: Boolean = false
}

class Vcf2ADAM(val args: Vcf2ADAMArgs) extends BDGSparkCommand[Vcf2ADAMArgs] with Logging {
  val companion = Vcf2ADAM

  def run(sc: SparkContext) {

    val variantContextRdd = sc.loadVcf(args.vcfPath)
    val variantContextsToSave = if (args.coalesce > 0) {
      variantContextRdd.transform(
        _.coalesce(args.coalesce, shuffle = args.coalesce > variantContextRdd.rdd.partitions.length || args.forceShuffle)
      )
    } else {
      variantContextRdd
    }

    if (args.onlyVariants) {
      variantContextsToSave
        .toVariantRDD
        .saveAsParquet(args)
    } else {
      variantContextsToSave
        .toGenotypeRDD
        .saveAsParquet(args)
    }
  }
}
