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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ SequenceDictionary, VariantContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import java.io.File

object Vcf2ADAM extends BDGCommandCompanion {
  val commandName = "vcf2adam"
  val commandDescription = "Convert a VCF file to the corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Vcf2ADAM(Args4j[Vcf2ADAMArgs](cmdLine))
  }
}

class Vcf2ADAMArgs extends Args4jBase with ParquetSaveArgs {
  @Args4jOption(required = false, name = "-dict", usage = "Reference dictionary")
  var dictionaryFile: File = _

  @Argument(required = true, metaVar = "VCF", usage = "The VCF file to convert", index = 0)
  var vcfPath: String = _

  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM Variant data", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-coalesce", usage = "Set the number of partitions written to the ADAM output directory")
  var coalesce: Int = -1

  @Args4jOption(required = false, name = "-onlyvariants", usage = "Output Variant objects instead of Genotypes")
  var onlyvariants: Boolean = false
}

class Vcf2ADAM(val args: Vcf2ADAMArgs) extends BDGSparkCommand[Vcf2ADAMArgs] with DictionaryCommand with Logging {
  val companion = Vcf2ADAM

  def run(sc: SparkContext) {

    var dictionary: Option[SequenceDictionary] = loadSequenceDictionary(args.dictionaryFile)
    if (dictionary.isDefined)
      log.info("Using contig translation")

    var adamVariants: RDD[VariantContext] = sc.loadVcf(args.vcfPath, sd = dictionary)
    if (args.coalesce > 0) {
      adamVariants = adamVariants.coalesce(args.coalesce, true)
    }

    if (args.onlyvariants) {
      adamVariants
        .map(v => v.variant.variant)
        .adamParquetSave(args)
    } else {
      adamVariants
        .flatMap(p => p.genotypes)
        .adamParquetSave(args)
    }
  }
}
