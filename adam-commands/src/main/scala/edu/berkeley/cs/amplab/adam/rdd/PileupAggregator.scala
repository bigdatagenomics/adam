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

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.{Base, ADAMPileup}
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

private[rdd] class PileupAggregator extends Serializable with Logging {

  def mapPileup(a: ADAMPileup): (Option[Base], Option[java.lang.Integer], Option[CharSequence]) = {
    (Option(a.getReadBase), Option(a.getRangeOffset), Option(a.getRecordGroupSample))
  }

  def aggregatePileup(pileupList: List[ADAMPileup]): ADAMPileup = {

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

    combineEvidence(pileupList)
  }

  def aggregate(pileups: RDD[ADAMPileup], coverage: Int = 30): RDD[ADAMPileup] = {
    def flatten(kv: Seq[ADAMPileup]): List[ADAMPileup] = {
      val splitUp = kv.toList.groupBy(mapPileup)
      
      splitUp.map(kv => aggregatePileup(kv._2)).toList
    }

    log.info ("Aggregating " + pileups.count + " pileups.")

    /* need to manually set partitions here, as this is a large reduction if there are long reads, or there is high coverage.
     * if this is not set, then you will encounter out-of-memory errors, as the working set for each reducer becomes very large.
     * as a first order approximation, we use coverage as a proxy for setting the number of reducers needed.
     */
    val grouping = pileups.groupBy((p: ADAMPileup) => p.getPosition, pileups.partitions.length * coverage / 2)

    log.info ("Pileups grouped into " + grouping.count + " positions.")

    val aggregated = grouping.flatMap(kv => flatten(kv._2))

    log.info ("Aggregation reduces down to " + aggregated.count + " pileups.")

    aggregated
  }
}
