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

import htsjdk.samtools.ValidationStringency
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ ADAMSaveAnyArgs, GenomicRDD }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option ⇒ Args4jOption }

object Vcf2Adam extends BDGCommandCompanion {
  val commandName = "vcf2adam"
  val commandDescription = "Transform VCF into genotypes and variants in ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Vcf2Adam(Args4j[Vcf2AdamArgs](cmdLine))
  }
}

class Vcf2AdamArgs extends Args4jBase with ParquetSaveArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The VCF file to transform (e.g., .vcf, .vcf.gz, .vcf.bgzf, .vcf.bgz).", index = 0)
  var inputPath: String = null

  // set to either variantsOutputPath or genotypesOutputPath as necessary
  var outputPath: String = null

  @Argument(required = true, metaVar = "VARIANTS", usage = "Location to write ADAM variants data in Parquet format.", index = 1)
  var variantsOutputPath: String = null

  @Argument(required = true, metaVar = "GENOTYPES", usage = "Location to write ADAM genotypes data in Parquet format.", index = 2)
  var genotypesOutputPath: String = null

  @Args4jOption(required = false, name = "-coalesce", usage = "Number of partitions written to the ADAM output directories.")
  var coalesce: Int = -1

  @Args4jOption(required = false, name = "-force_shuffle_coalesce", usage = "Even if the repartitioned RDDs have fewer partitions, force a shuffle.")
  var forceShuffle: Boolean = false

  @Args4jOption(required = false, name = "-sort_on_save", usage = "Sort by contig index.")
  var sort: Boolean = false

  @Args4jOption(required = false, name = "-sort_lexicographically_on_save", usage = "Sort by lexicographic order. Conflicts with -sort_on_save.")
  var sortLexicographically: Boolean = false

  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to STRICT.")
  var stringency: String = "STRICT"
}

/**
 * Transform VCF into genotypes and variants in ADAM format.
 */
class Vcf2Adam(val args: Vcf2AdamArgs)
    extends BDGSparkCommand[Vcf2AdamArgs] {
  val companion = Vcf2Adam
  val stringency = ValidationStringency.valueOf(args.stringency)

  /**
   * Coalesce the specified GenomicRDD if requested.
   *
   * @param rdd GenomicRDD to coalesce.
   * @return The specified GenomicRDD coalesced if requested.
   */
  private def maybeCoalesce[U <: GenomicRDD[_, U]](rdd: U): U = {
    if (args.coalesce != -1) {
      log.info("Coalescing the number of partitions to '%d'".format(args.coalesce))
      if (args.coalesce > rdd.rdd.partitions.length || args.forceShuffle) {
        rdd.transform(_.coalesce(args.coalesce, shuffle = true))
      } else {
        rdd.transform(_.coalesce(args.coalesce, shuffle = false))
      }
    } else {
      rdd
    }
  }

  /**
   * Sort the specified GenomicRDD if requested.
   *
   * @param rdd GenomicRDD to sort.
   * @return The specified GenomicRDD sorted if requested.
   */
  private def maybeSort[U <: GenomicRDD[_, U]](rdd: U): U = {
    if (args.sort) {
      log.info("Sorting before saving")
      rdd.sort()
    } else if (args.sortLexicographically) {
      log.info("Sorting lexicographically before saving")
      rdd.sortLexicographically()
    } else {
      rdd
    }
  }

  def run(sc: SparkContext) {
    require(!(args.sort && args.sortLexicographically),
      "Cannot set both -sort_on_save and -sort_lexicographically_on_save.")

    val variantContexts = sc.loadVcf(
      args.inputPath,
      stringency = stringency)

    // todo: cache variantContexts?  sort variantContexts first?

    args.outputPath = args.variantsOutputPath
    maybeSort(maybeCoalesce(variantContexts.toVariants())).saveAsParquet(args)

    args.outputPath = args.genotypesOutputPath
    maybeSort(maybeCoalesce(variantContexts.toGenotypes())).saveAsParquet(args)
  }
}
