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
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }

object ADAM2Vcf extends BDGCommandCompanion {

  val commandName = "adam2vcf"
  val commandDescription = "Convert an ADAM variant to the VCF ADAM format"

  def apply(cmdLine: Array[String]) = {
    new ADAM2Vcf(Args4j[ADAM2VcfArgs](cmdLine))
  }
}

class ADAM2VcfArgs extends Args4jBase with ParquetArgs {

  @Argument(required = true, metaVar = "ADAM", usage = "The ADAM variant files to convert", index = 0)
  var adamFile: String = _

  @Argument(required = true, metaVar = "VCF", usage = "Location to write VCF data", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-coalesce", usage = "Set the number of partitions written to the ADAM output directory")
  var coalesce: Int = -1

  @Args4jOption(required = false, name = "-sort_on_save", usage = "Sort the VCF output by contig index.")
  var sort: Boolean = false

  @Args4jOption(required = false,
    name = "-sort_lexicographically_on_save",
    usage = "Sort the VCF output by lexicographic order. Conflicts with -sort_on_save.")
  var sortLexicographically: Boolean = false

  @Args4jOption(required = false, name = "-single", usage = "Save as a single VCF file.")
  var single: Boolean = false

  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to STRICT")
  var stringency: String = "STRICT"
}

class ADAM2Vcf(val args: ADAM2VcfArgs) extends BDGSparkCommand[ADAM2VcfArgs] with Logging {
  val companion = ADAM2Vcf
  val stringency = ValidationStringency.valueOf(args.stringency)

  def run(sc: SparkContext) {
    require(!(args.sort && args.sortLexicographically),
      "Cannot set both -sort_on_save and -sort_lexicographically_on_save.")

    val adamGTs = sc.loadParquetGenotypes(args.adamFile)

    val coalesce = if (args.coalesce > 0) {
      Some(args.coalesce)
    } else {
      None
    }

    // convert to variant contexts and prep for save
    val variantContexts = adamGTs.toVariantContextRDD
    val maybeCoalescedVcs = if (args.coalesce > 0) {
      variantContexts.transform(_.coalesce(args.coalesce))
    } else {
      variantContexts
    }

    // sort if requested
    val maybeSortedVcs = if (args.sort) {
      maybeCoalescedVcs.sort()
    } else if (args.sortLexicographically) {
      maybeCoalescedVcs.sortLexicographically()
    } else {
      maybeCoalescedVcs
    }

    maybeSortedVcs.saveAsVcf(args.outputPath,
      asSingleFile = args.single,
      stringency)
  }
}
