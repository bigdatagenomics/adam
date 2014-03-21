/*
 * Copyright (c) 2013. Mount Sinai School of Medicine
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

package edu.berkeley.cs.amplab.adam.cli

import edu.berkeley.cs.amplab.adam.avro.ADAMGenotype
import edu.berkeley.cs.amplab.adam.rdd.AdamContext. _
import edu.berkeley.cs.amplab.adam.rdd.{GenotypesSummary, GenotypesSummaryFormatting}
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import org.kohsuke.args4j
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.apache.hadoop.mapreduce.Job

object SummarizeGenotypes extends AdamCommandCompanion {

  val commandName = "summarize_genotypes"
  val commandDescription = "Print statistics of genotypes and variants in an ADAM file"

  def apply(cmdLine: Array[String]) = {
    new SummarizeGenotypes(Args4j[SummarizeGenotypesArgs](cmdLine))
  }
}

class SummarizeGenotypesArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @args4j.Argument(required = true, metaVar = "ADAM", usage = "The ADAM variant files to print stats for", index = 0)
  var adamFile: String = _

  @args4j.Option(required = false, name = "-format", usage = "Format: one of human, csv. Default: human.")
  var format: String = "human"

}

class SummarizeGenotypes(val args: SummarizeGenotypesArgs) extends AdamSparkCommand[SummarizeGenotypesArgs] with Logging {
  val companion = SummarizeGenotypes

  def run(sc: SparkContext, job: Job) {
    val adamGTs: RDD[ADAMGenotype] = sc.adamLoad(args.adamFile)
    val stats = GenotypesSummary(adamGTs)
    args.format match {
      case "human" => {
        println(GenotypesSummaryFormatting.format_human_readable(stats))
      }
      case "csv" => {
        println(GenotypesSummaryFormatting.format_csv(stats))
      }
      case _ => {
        log.error("Invalid -format: %s".format(args.format))
      }
    }
  }
}
