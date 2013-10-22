package edu.berkeley.cs.amplab.adam.commands

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

import edu.berkeley.cs.amplab.adam.util.{Args4jBase, Args4j}
import edu.berkeley.cs.amplab.adam.avro.ADAMPileup
import org.kohsuke.args4j.Argument
import spark.{RDD, SparkContext}
import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.predicates.LocusPredicate
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._

object PileupAggregator extends AdamCommandCompanion {
  val commandName: String = "aggregate_pileups"
  val commandDescription: String = "Aggregates pileups in an ADAM reference-oriented file"

  def apply(cmdLine: Array[String]) = {
    new PileupAggregator(Args4j[PileupAggregatorArgs](cmdLine))
  }
}

class PileupAggregatorArgs extends Args4jBase with SparkArgs with ParquetArgs {

  @Argument(metaVar = "ADAMPILEUPS", required = true, usage = "ADAM reference-oriented data", index = 0)
  var readInput: String = _

  @Argument(metaVar = "DIR", required = true, usage = "Location to store aggregated\nreference-oriented ADAM data", index = 1)
  var pileupOutput: String = _
}

class PileupAggregator(protected val args: PileupAggregatorArgs)
  extends AdamSparkCommand[PileupAggregatorArgs] {

  val companion = PileupAggregator

  def run(sc: SparkContext, job: Job) {
    val pileups: RDD[ADAMPileup] = sc.adamLoad(args.readInput, predicate = Some(classOf[LocusPredicate]))
    pileups.adamAggregatePileups().adamSave(args.pileupOutput, args)
  }

}