/*
 * Copyright (c) 2013. Regents of the University of California
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
import edu.berkeley.cs.amplab.adam.rdd.{ComputeVariantStatistics, VariantStatistics}
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import org.kohsuke.args4j.Argument
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.apache.hadoop.mapreduce.Job

object VariantStats extends AdamCommandCompanion {

  val commandName = "variant_stats"
  val commandDescription = "Print statistics on variants in an ADAM file (similar to vcf-stats from VCFTools) "

  def apply(cmdLine: Array[String]) = {
    new VariantStats(Args4j[VariantStatsArgs](cmdLine))
  }
}

class VariantStatsArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "ADAM", usage = "The ADAM variant files to print stats for", index = 0)
  var adamFile: String = _
}

class VariantStats(val args: VariantStatsArgs) extends AdamSparkCommand[VariantStatsArgs] with Logging {
  val companion = VariantStats

  def run(sc: SparkContext, job: Job) {
    val adamGTs: RDD[ADAMGenotype] = sc.adamLoad(args.adamFile)
    //val stats: VariantStats = sc.adamVariantStats()
    val stats: VariantStatistics = ComputeVariantStatistics(adamGTs)
    println(stats.toString())
    /*
    println(
      "%d total variants".format(stats.numGenotypes)
    )
    */
  }
}
