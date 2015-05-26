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

import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkContext }
import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import scala.Option

object ADAM2Vcf extends BDGCommandCompanion {

  val commandName = "adam2vcf"
  val commandDescription = "Convert an ADAM variant to the VCF ADAM format"

  def apply(cmdLine: Array[String]) = {
    new ADAM2Vcf(Args4j[ADAM2VcfArgs](cmdLine))
  }
}

class ADAM2VcfArgs extends Args4jBase with ParquetArgs {
  @Args4jOption(required = false, name = "-dict", usage = "Reference dictionary")
  var dictionaryFile: File = _

  @Argument(required = true, metaVar = "ADAM", usage = "The ADAM variant files to convert", index = 0)
  var adamFile: String = _

  @Argument(required = true, metaVar = "VCF", usage = "Location to write VCF data", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-coalesce", usage = "Set the number of partitions written to the ADAM output directory")
  var coalesce: Int = -1

  @Args4jOption(required = false, name = "-sort_on_save", usage = "Sort the VCF output.")
  var sort: Boolean = false
}

class ADAM2Vcf(val args: ADAM2VcfArgs) extends BDGSparkCommand[ADAM2VcfArgs] with DictionaryCommand with Logging {
  val companion = ADAM2Vcf

  def run(sc: SparkContext) {
    var dictionary: Option[SequenceDictionary] = loadSequenceDictionary(args.dictionaryFile)
    if (dictionary.isDefined)
      log.info("Using contig translation")

    val adamGTs: RDD[Genotype] = sc.loadParquetGenotypes(args.adamFile)

    val coalesce = if (args.coalesce > 0) {
      Some(args.coalesce)
    } else {
      None
    }

    adamGTs.toVariantContext
      .saveAsVcf(args.outputPath, dict = dictionary, sortOnSave = args.sort, coalesceTo = coalesce)
  }
}
