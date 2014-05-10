/*
 * Copyright (c) 2013. The Broad Institute of MIT/Harvard
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

package org.bdgenomics.adam.cli

import org.bdgenomics.adam.models.{ SequenceDictionary, ADAMVariantContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import java.io.File

object Vcf2ADAM extends ADAMCommandCompanion {
  val commandName = "vcf2adam"
  val commandDescription = "Convert a VCF file to the corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Vcf2ADAM(Args4j[Vcf2ADAMArgs](cmdLine))
  }
}

class Vcf2ADAMArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Args4jOption(required = false, name = "-dict", usage = "Reference dictionary")
  var dictionaryFile: File = _

  @Argument(required = true, metaVar = "VCF", usage = "The VCF file to convert", index = 0)
  var vcfPath: String = _

  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM Variant data", index = 1)
  var outputPath: String = null
}

class Vcf2ADAM(val args: Vcf2ADAMArgs) extends ADAMSparkCommand[Vcf2ADAMArgs] with DictionaryCommand with Logging {
  val companion = Vcf2ADAM

  def run(sc: SparkContext, job: Job) {

    var dictionary: Option[SequenceDictionary] = loadSequenceDictionary(args.dictionaryFile)
    if (dictionary.isDefined)
      log.info("Using contig translation")

    var adamVariants: RDD[ADAMVariantContext] = sc.adamVCFLoad(args.vcfPath, dict = dictionary)

    adamVariants.flatMap(p => p.genotypes).adamSave(
      args.outputPath,
      blockSize = args.blockSize,
      pageSize = args.pageSize,
      compressCodec = args.compressionCodec,
      disableDictionaryEncoding = args.disableDictionary)
  }
}
