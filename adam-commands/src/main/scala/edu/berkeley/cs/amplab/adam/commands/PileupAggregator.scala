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
import net.sf.samtools.TextCigarCodec
import edu.berkeley.cs.amplab.adam.avro.{ADAMRecord, ADAMPileup, Base}
import org.kohsuke.args4j.Argument
import spark.{RDD, SparkContext}
import spark.SparkContext._
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import parquet.avro.AvroReadSupport
import edu.berkeley.cs.amplab.adam.predicates.LocusPredicate
import parquet.hadoop.util.ContextUtil

object PileupAggregator extends AdamCommandCompanion {
  val commandName: String = "aggregate_pileups"
  val commandDescription: String = "Aggregates pileups in an ADAM reference-oriented file"
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

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

class PileupAggregatorHelper extends Serializable {

  def mapPileup(a: ADAMPileup): (Option[Long], Option[Base], Option[java.lang.Integer], Option[CharSequence]) = {
    (Option(a.getPosition), Option(a.getReadBase), Option(a.getRangeOffset), Option(a.getRecordGroupSample))
  }

  def aggregatePileup(pileupList: List[ADAMPileup]): List[ADAMPileup] = {

    def combineEvidence(pileupGroup: List[ADAMPileup]): ADAMPileup = {
      val pileup = pileupGroup.reduce((a: ADAMPileup, b: ADAMPileup) => {
        a.setMapQuality(a.getMapQuality + b.getMapQuality)
        a.setSangerQuality(a.getSangerQuality + b.getSangerQuality)
        a.setCountAtPosition(a.getCountAtPosition + b.getCountAtPosition)
        a.setNumSoftClipped(a.getNumSoftClipped + b.getNumSoftClipped)
        a.setNumReverseStrand(a.getNumReverseStrand + b.getNumReverseStrand)

        a
      })

      val num = pileup.getCountAtPosition

      pileup.setMapQuality(pileup.getMapQuality / num)
      pileup.setSangerQuality(pileup.getSangerQuality / num)

      pileup
    }

    List(combineEvidence(pileupList))
  }

  def aggregate(pileups: RDD[ADAMPileup]): RDD[ADAMPileup] = {
    def flatten(kv: ((Option[Long], Option[Base], Option[java.lang.Integer], Option[CharSequence]), Seq[ADAMPileup])): List[ADAMPileup] = {
      aggregatePileup(kv._2.toList)
    }

    pileups.groupBy(mapPileup).flatMap(flatten)
  }
}

class PileupAggregator(protected val args: PileupAggregatorArgs)
  extends AdamSparkCommand[PileupAggregatorArgs] with ParquetCommand {

  val companion = PileupAggregator

  def run(sc: SparkContext, job: Job) {
    setupParquetOutputFormat(args, job, ADAMPileup.SCHEMA$)

    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[ADAMRecord]])
    ParquetInputFormat.setUnboundRecordFilter(job, classOf[LocusPredicate])
    val reads = sc.newAPIHadoopFile(args.readInput,
      classOf[ParquetInputFormat[ADAMPileup]], classOf[Void], classOf[ADAMPileup],
      ContextUtil.getConfiguration(job))

    val nonNullReads: RDD[ADAMPileup] = reads filter (r => r._2 != null) map (r => r._2)

    val worker = new PileupAggregatorHelper

    val pileups = worker.aggregate(nonNullReads).map(p => (null, p))

    pileups.saveAsNewAPIHadoopFile(args.pileupOutput,
      classOf[Void], classOf[ADAMPileup], classOf[ParquetOutputFormat[ADAMPileup]],
      ContextUtil.getConfiguration(job))
  }

}