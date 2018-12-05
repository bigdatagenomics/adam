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
import org.bdgenomics.adam.projections.AlignmentRecordField._
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

/**
 * Reads2Coverage (accessible as the command 'reads2coverage' through the CLI) takes two arguments,
 * an INPUT and OUTPUT, and calculates the number of reads from INPUT at every location in
 * the file. Optional arguments are only_negative_strands, only_positive_strands and collapse.
 * only_negative_strands and only_positive_strands save coverage computed from only negative and positive strands,
 * respectively. Collapse specifies whether saved coverage should merge neighboring coverage with the same counts
 * to one record.
 */
object Reads2Coverage extends BDGCommandCompanion {
  val commandName: String = "reads2coverage"
  val commandDescription: String = "Calculate the coverage from a given ADAM file"

  def apply(cmdLine: Array[String]): BDGCommand = {
    new Reads2Coverage(Args4j[Reads2CoverageArgs](cmdLine))
  }
}

class Reads2CoverageArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The reads file to use to calculate depths", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write the coverage data to", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-collapse", usage = "Collapses neighboring coverage records " +
    "of equal counts into the same record")
  var collapse: Boolean = false
  @Args4jOption(required = false, name = "-only_negative_strands", usage = "Compute coverage for negative strands")
  var onlyNegativeStrands: Boolean = false
  @Args4jOption(required = false, name = "-only_positive_strands", usage = "Compute coverage for positive strands")
  var onlyPositiveStrands: Boolean = false
  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false
  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false
  @Args4jOption(required = false, name = "-sort_lexicographically", usage = "Sort the reads lexicographically by contig name, instead of by index.")
  var sortLexicographically: Boolean = false
}

class Reads2Coverage(protected val args: Reads2CoverageArgs) extends BDGSparkCommand[Reads2CoverageArgs] {
  val companion: BDGCommandCompanion = Reads2Coverage

  def run(sc: SparkContext): Unit = {
    if (args.sortLexicographically) {
      require(args.collapse,
        "-sort_lexicographically can only be provided when collapsing (-collapse).")
    }

    // If saving strand specific coverage, require that only one direction is specified
    require(!(args.onlyNegativeStrands && args.onlyPositiveStrands),
      "Cannot compute coverage for both negative and positive strands separately")

    // load reads
    val readsRdd: AlignmentRecordDataset = sc.loadAlignments(args.inputPath)

    val finalReads = if (args.onlyNegativeStrands && !args.onlyPositiveStrands) {
      readsRdd.transform(rdd => rdd.filter(_.getReadNegativeStrand))
    } else if (!args.onlyNegativeStrands && args.onlyPositiveStrands) {
      readsRdd.transform(rdd => rdd.filter(!_.getReadNegativeStrand))
    } else {
      readsRdd
    }

    val coverage = finalReads.toCoverage()

    val maybeCollapsedCoverage = if (args.collapse) {
      val sortedCoverage = if (args.sortLexicographically) {
        coverage.sortLexicographically()
      } else {
        coverage.sort()
      }
      sortedCoverage.collapse()
    } else {
      coverage
    }

    maybeCollapsedCoverage.save(args.outputPath,
      asSingleFile = args.asSingleFile,
      disableFastConcat = args.disableFastConcat)
  }
}
