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
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{ ADAMSaveAnyArgs, GenomicDataset }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option ⇒ Args4jOption }

object TransformGenotypes extends BDGCommandCompanion {
  val commandName = "transformGenotypes"
  val commandDescription = "Convert a file with genotypes into corresponding ADAM format and vice versa"

  def apply(cmdLine: Array[String]) = {
    new TransformGenotypes(Args4j[TransformGenotypesArgs](cmdLine))
  }
}

class TransformGenotypesArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The genotypes file to convert (e.g., .vcf, .vcf.gz, .vcf.bgzf, .vcf.bgz). If extension is not detected, Parquet is assumed.", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write ADAM genotypes data. If extension is not detected, Parquet is assumed.", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-coalesce", usage = "Number of partitions written to the ADAM output directory.")
  var coalesce: Int = -1

  @Args4jOption(required = false, name = "-force_shuffle_coalesce", usage = "Even if the repartitioned RDD has fewer partitions, force a shuffle.")
  var forceShuffle: Boolean = false

  @Args4jOption(required = false, name = "-nested_annotations", usage = "Populate the variant.annotation field in the Genotype records. Disabled by default.")
  var nestedAnnotations: Boolean = false

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

  @Args4jOption(required = false, name = "-partition_by_start_pos", usage = "Save the data partitioned by genomic range bins based on start pos using Hive-style partitioning.")
  var partitionByStartPos: Boolean = false

  @Args4jOption(required = false, name = "-partition_bin_size", usage = "Partition bin size used in Hive-style partitioning. Defaults to 1Mbp (1,000,000) base pairs).")
  var partitionedBinSize = 1000000

  // must be defined due to ADAMSaveAnyArgs, but unused here
  var sortFastqOutput: Boolean = false
}

/**
 * Convert a file with genotypes into corresponding ADAM format and vice versa.
 */
class TransformGenotypes(val args: TransformGenotypesArgs)
    extends BDGSparkCommand[TransformGenotypesArgs] {
  val companion = TransformGenotypes
  val stringency = ValidationStringency.valueOf(args.stringency)

  /**
   * Coalesce the specified GenomicDataset if requested.
   *
   * @param rdd GenomicDataset to coalesce.
   * @return The specified GenomicDataset coalesced if requested.
   */
  private def maybeCoalesce[U <: GenomicDataset[_, _, U]](rdd: U): U = {
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
   * Sort the specified GenomicDataset if requested.
   *
   * @param rdd GenomicDataset to sort.
   * @return The specified GenomicDataset sorted if requested.
   */
  private def maybeSort[U <: GenomicDataset[_, _, U]](rdd: U): U = {
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

    if (args.nestedAnnotations) {
      log.info("Populating the variant.annotation field in the Genotype records")
      sc.hadoopConfiguration.setBoolean(VariantContextConverter.nestAnnotationInGenotypesProperty, true)
    }

    val genotypes = sc.loadGenotypes(
      args.inputPath,
      optPredicate = None,
      optProjection = None,
      stringency = stringency)

    if (args.outputPath.endsWith(".vcf")) {
      maybeSort(maybeCoalesce(genotypes.toVariantContexts)).saveAsVcf(args)
    } else {
      if (args.partitionByStartPos) {
        maybeSort(maybeCoalesce(genotypes)).saveAsPartitionedParquet(args.outputPath, partitionSize = args.partitionedBinSize)
      } else {
        maybeSort(maybeCoalesce(genotypes)).saveAsParquet(args)
      }

    }
  }
}
