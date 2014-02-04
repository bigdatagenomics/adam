/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.cli

import edu.berkeley.cs.amplab.adam.util._
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.kohsuke.args4j.Argument
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.rdd.compare.{CompareAdam => CoreCompareAdam}

object CompareAdam extends AdamCommandCompanion with Serializable {
  val commandName: String = "compare"
  val commandDescription: String = "Compare two ADAM files based on read name"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new CompareAdam(Args4j[CompareAdamArgs](cmdLine))
  }
}


class CompareAdamArgs extends Args4jBase with SparkArgs with ParquetArgs with Serializable {
  @Argument(required = true, metaVar = "INPUT1", usage = "The first ADAM file to compare", index = 0)
  val input1Path: String = null

  @Argument(required = true, metaVar = "INPUT2", usage = "The second ADAM file to compare", index = 1)
  val input2Path: String = null
}

class CompareAdam(protected val args: CompareAdamArgs) extends AdamSparkCommand[CompareAdamArgs] with Serializable {
  val companion: AdamCommandCompanion = CompareAdam

  def run(sc: SparkContext, job: Job): Unit = {

    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val (comp1, comp2) =
      sc.adamCompareFiles(args.input1Path, args.input2Path, CoreCompareAdam.readLocationsMatchPredicate)

    assert(comp1.matching == comp2.matching, "Matching numbers should be identical for pairwise comparison")

    println("# Reads in INPUT1:        %d".format(comp1.total))
    println("# Reads in INPUT2:        %d".format(comp2.total))
    println("# Reads Unique to INPUT1: %d".format(comp1.unique))
    println("# Reads Unique to INPUT2: %d".format(comp2.unique))
    println("# Matching Reads:         %d".format(comp1.matching))
  }

}

