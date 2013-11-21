package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.{Base, ADAMPileup}
import org.apache.spark.rdd.RDD

private[rdd] class PileupAggregator extends Serializable {

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
