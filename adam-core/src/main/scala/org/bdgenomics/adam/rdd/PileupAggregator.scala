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
package org.bdgenomics.adam.rdd

import org.bdgenomics.formats.avro.{ Base, ADAMContig, ADAMPileup }
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

private[rdd] class PileupAggregator(validate: Boolean = false) extends Serializable with Logging {

  /**
   * Maps a pileup to uniquify by virtue of base, indel status, and sample.
   *
   * @param a Pileup to uniquify.
   * @return Key uniquified by base, indel position, and sample.
   */
  def mapPileup(a: ADAMPileup): (Option[Base], Option[java.lang.Integer], Option[CharSequence]) = {
    (Option(a.getReadBase), Option(a.getRangeOffset), Option(a.getRecordGroupSample))
  }

  /**
   * Reduces down the data between several pileup bases.
   *
   * @param pileupList List of pileup bases to reduce.
   * @return Single pileup base with bases reduced down.
   *
   * @note All bases are expected to be from the same sample, and to have the same base and indel location.
   */
  protected def aggregatePileup(pileupList: List[ADAMPileup]): ADAMPileup = {

    def combineEvidence(pileupGroup: List[ADAMPileup]): ADAMPileup = {
      val pileup = pileupGroup.reduce((a: ADAMPileup, b: ADAMPileup) => {
        if (validate) {
          require(Option(a.getMapQuality).isDefined &&
            Option(a.getSangerQuality).isDefined &&
            Option(a.getCountAtPosition).isDefined &&
            Option(a.getNumSoftClipped).isDefined &&
            Option(a.getNumReverseStrand).isDefined &&
            Option(a.getReadName).isDefined &&
            Option(a.getReadStart).isDefined &&
            Option(a.getReadEnd).isDefined,
            "Cannot aggregate pileup with required fields null: " + a)

          require(Option(b.getMapQuality).isDefined &&
            Option(b.getSangerQuality).isDefined &&
            Option(b.getCountAtPosition).isDefined &&
            Option(b.getNumSoftClipped).isDefined &&
            Option(b.getNumReverseStrand).isDefined &&
            Option(b.getReadName).isDefined &&
            Option(b.getReadStart).isDefined &&
            Option(b.getReadEnd).isDefined,
            "Cannot aggregate pileup with required fields null: " + b)
        }

        // We have to duplicate the existing contig so it doesn't get
        // inadvertently modified later on.
        val contig = ADAMContig.newBuilder(a.getContig).build

        // set copied fields
        val c = ADAMPileup.newBuilder()
          .setContig(contig)
          .setPosition(a.getPosition)
          .setRangeOffset(a.getRangeOffset)
          .setRangeLength(a.getRangeLength)
          .setReferenceBase(a.getReferenceBase)
          .setReadBase(a.getReadBase)

        // merge read group fields
        val rgsc = List(Option(a.getRecordGroupSequencingCenter), Option(b.getRecordGroupSequencingCenter))
          .flatMap((p: Option[CharSequence]) => p)
          .distinct
        if (rgsc.length != 0) {
          c.setRecordGroupSequencingCenter(rgsc.reduce((a: CharSequence, b: CharSequence) => a + "," + b))
        }

        val rgd = List(Option(a.getRecordGroupDescription), Option(b.getRecordGroupDescription))
          .flatMap((p: Option[CharSequence]) => p)
          .distinct
        if (rgd.length != 0) {
          c.setRecordGroupDescription(rgd.reduce((a: CharSequence, b: CharSequence) => a + "," + b))
        }

        val rgrde = List(Option(a.getRecordGroupRunDateEpoch), Option(b.getRecordGroupRunDateEpoch))
          .flatMap((p: Option[java.lang.Long]) => p)
          .distinct
        if (rgrde.length == 1) {
          // cannot join multiple values together - behavior is not defined
          c.setRecordGroupRunDateEpoch(rgrde.head)
        }

        val rgfo = List(Option(a.getRecordGroupFlowOrder), Option(b.getRecordGroupFlowOrder))
          .flatMap((p: Option[CharSequence]) => p)
          .distinct
        if (rgfo.length != 0) {
          c.setRecordGroupFlowOrder(rgfo.reduce((a: CharSequence, b: CharSequence) => a + "," + b))
        }

        val rgks = List(Option(a.getRecordGroupKeySequence), Option(b.getRecordGroupKeySequence))
          .flatMap((p: Option[CharSequence]) => p)
          .distinct
        if (rgks.length != 0) {
          c.setRecordGroupKeySequence(rgks.reduce((a: CharSequence, b: CharSequence) => a + "," + b))
        }

        val rgl = List(Option(a.getRecordGroupLibrary), Option(b.getRecordGroupLibrary))
          .flatMap((p: Option[CharSequence]) => p)
          .distinct
        if (rgl.length != 0) {
          c.setRecordGroupLibrary(rgl.reduce((a: CharSequence, b: CharSequence) => a + "," + b))
        }

        val rgpmis = List(Option(a.getRecordGroupPredictedMedianInsertSize), Option(b.getRecordGroupPredictedMedianInsertSize))
          .flatMap((p: Option[java.lang.Integer]) => p)
          .distinct
        if (rgpmis.length == 1) {
          // cannot combine two values here - behavior is not defined
          c.setRecordGroupPredictedMedianInsertSize(rgpmis.head)
        }

        val rgp = List(Option(a.getRecordGroupPlatform), Option(b.getRecordGroupPlatform))
          .flatMap((p: Option[CharSequence]) => p)
          .distinct
        if (rgp.length != 0) {
          c.setRecordGroupPlatform(rgp.reduce((a: CharSequence, b: CharSequence) => a + "," + b))
        }

        val rgpu = List(Option(a.getRecordGroupPlatformUnit), Option(b.getRecordGroupPlatformUnit))
          .flatMap((p: Option[CharSequence]) => p)
          .distinct
        if (rgpu.length != 0) {
          c.setRecordGroupPlatformUnit(rgpu.reduce((a: CharSequence, b: CharSequence) => a + "," + b))
        }

        val rgs = List(Option(a.getRecordGroupSample), Option(b.getRecordGroupSample))
          .flatMap((p: Option[CharSequence]) => p)
          .distinct
        if (rgs.length != 0) {
          c.setRecordGroupSample(rgs.reduce((a: CharSequence, b: CharSequence) => a + "," + b))
        }

        // set new fields
        c.setMapQuality(a.getMapQuality * a.getCountAtPosition +
          b.getMapQuality * b.getCountAtPosition)
          .setSangerQuality(a.getSangerQuality * a.getCountAtPosition +
            b.getSangerQuality * b.getCountAtPosition)
          .setCountAtPosition(a.getCountAtPosition + b.getCountAtPosition)
          .setNumSoftClipped(a.getNumSoftClipped + b.getNumSoftClipped)
          .setNumReverseStrand(a.getNumReverseStrand + b.getNumReverseStrand)
          .setReadName(a.getReadName + "," + b.getReadName)
          .setReadStart(a.getReadStart.toLong min b.getReadStart.toLong)
          .setReadEnd(a.getReadEnd.toLong max b.getReadEnd.toLong)

        c.build()
      })

      val num = pileup.getCountAtPosition

      // phred score is logarithmic so geometric mean is sum / count
      pileup.setMapQuality(pileup.getMapQuality / num)
      pileup.setSangerQuality(pileup.getSangerQuality / num)

      pileup
    }

    combineEvidence(pileupList)
  }

  /**
   * Groups pileups together and then aggregates their data together.
   *
   * @param kv Group of pileups to aggregate.
   * @return Aggregated pileups.
   */
  def flatten(kv: Iterable[ADAMPileup]): List[ADAMPileup] = {
    val splitUp = kv.toList.groupBy(mapPileup)

    splitUp.map(kv => aggregatePileup(kv._2)).toList
  }

  /**
   * Performs aggregation across an RDD.
   *
   * @param pileups RDD of pileup bases to aggregate.
   * @param coverage Parameter showing average coverage. Default is 30. Used to increase number of reducers.
   * @return RDD of aggregated bases.
   */
  def aggregate(pileups: RDD[ADAMPileup], coverage: Int = 30): RDD[ADAMPileup] = {

    log.info("Aggregating " + pileups.count + " pileups.")

    /* need to manually set partitions here, as this is a large reduction if there are long reads, or there is high coverage.
     * if this is not set, then you will encounter out-of-memory errors, as the working set for each reducer becomes very large.
     * as a first order approximation, we use coverage as a proxy for setting the number of reducers needed.
     */
    val grouping = pileups.groupBy((p: ADAMPileup) => ReferencePosition(p),
      pileups.partitions.length * coverage / 2)

    log.info("Pileups grouped into " + grouping.count + " positions.")

    val aggregated = grouping.flatMap(kv => flatten(kv._2))

    log.info("Aggregation reduces down to " + aggregated.count + " pileups.")

    aggregated
  }
}
