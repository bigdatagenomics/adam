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
import org.bdgenomics.adam.rdd.variant.GenotypeRDD
import org.bdgenomics.formats.avro.{ Genotype, Variant }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option ⇒ Args4jOption }

object Adam2Vcf extends BDGCommandCompanion {
  val commandName = "adam2vcf"
  val commandDescription = "Convert variants and genotypes in ADAM format to VCF"

  def apply(cmdLine: Array[String]) = {
    new Adam2Vcf(Args4j[Adam2VcfArgs](cmdLine))
  }
}

class Adam2VcfArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "VARIANTS", usage = "The variants file to convert (e.g., .vcf, .vcf.gz, .vcf.bgzf, .vcf.bgz). If extension is not detected, Parquet is assumed.", index = 0)
  var variantsInputPath: String = null

  @Argument(required = true, metaVar = "GENOTYPES", usage = "The genotypes file to convert (e.g., .vcf, .vcf.gz, .vcf.bgzf, .vcf.bgz). If extension is not detected, Parquet is assumed.", index = 1)
  var genotypesInputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write VCF.", index = 2)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-sort_on_save", usage = "Sort VCF output by contig index.")
  var sort: Boolean = false

  @Args4jOption(required = false, name = "-sort_lexicographically_on_save", usage = "Sort VCF output by lexicographic order. Conflicts with -sort_on_save.")
  var sortLexicographically: Boolean = false

  @Args4jOption(required = false, name = "-single", usage = "Save as a single VCF file.")
  var asSingleFile: Boolean = false

  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output.")
  var deferMerging: Boolean = false

  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false

  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to STRICT.")
  var stringency: String = "STRICT"

  // must be defined due to ADAMSaveAnyArgs, but unused here
  var sortFastqOutput: Boolean = false
}

/**
 * Convert variants and genotypes in ADAM format to VCF.
 */
class Adam2Vcf(val args: Adam2VcfArgs)
    extends BDGSparkCommand[Adam2VcfArgs] {
  val companion = Adam2Vcf
  val stringency = ValidationStringency.valueOf(args.stringency)

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

    val variants = sc.loadVariants(
      args.variantsInputPath,
      optPredicate = None,
      optProjection = None,
      stringency = stringency)

    val genotypes = sc.loadGenotypes(
      args.genotypesInputPath,
      optPredicate = None,
      optProjection = None,
      stringency = stringency)

    val join = variants.shuffleRegionJoin(genotypes)
    val updatedGenotypes = join.rdd.map(pair => {
      val v = Variant.newBuilder(pair._2.getVariant)
        .setAnnotation(pair._1.getAnnotation)
        .build()

      Genotype.newBuilder(pair._2)
        .setVariant(v)
        .build()
    })
    val updatedGenotypeRdd = GenotypeRDD.apply(updatedGenotypes, genotypes.sequences, genotypes.samples, genotypes.headerLines)
    maybeSort(updatedGenotypeRdd.toVariantContexts).saveAsVcf(args)
  }
}
