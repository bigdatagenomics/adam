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

import edu.berkeley.cs.amplab.adam.avro.{Base, ADAMPileup, ADAMRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.util._
import net.sf.samtools.{CigarOperator, TextCigarCodec}
import scala.collection.JavaConverters._
import scala.collection.immutable.StringOps

private[rdd] object Reads2PileupProcessor {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton
}

private[rdd] class Reads2PileupProcessor extends Serializable with Logging {

  def readToPileups(record: ADAMRecord): List[ADAMPileup] = {
    if (record == null || record.getCigar == null || record.getMismatchingPositions == null) {
      // TODO: log this later... We can't create a pileup without the CIGAR and MD tag
      // in the future, we can also get reference information from a reference file
      return List.empty
    }

    def baseFromSequence(pos: Int): Base = {
      Base.valueOf(record.getSequence.subSequence(pos, pos + 1).toString)
    }

    def sangerScoreToInt(score: String, position: Int): Int = score(position).toInt - 33

    def populatePileupFromReference(record: ADAMRecord, referencePos: Long, isReverseStrand: Boolean, readPos: Int): ADAMPileup.Builder = {

      var reverseStrandCount = 0

      if (isReverseStrand) {
        reverseStrandCount = 1
      }

      // check read mapping locations
      assert(record.getStart != null, "Read is mapped but has a null start position.")

      val end: Long = record.end match {
        case Some(o) => o.asInstanceOf[Long]
        case None => -1L
      }

      assert(end != -1L, "Read is mapped but has a null end position.")

      ADAMPileup.newBuilder()
        .setReferenceName(record.getReferenceName)
        .setReferenceId(record.getReferenceId)
        .setMapQuality(record.getMapq)
        .setPosition(referencePos)
        .setRecordGroupSequencingCenter(record.getRecordGroupSequencingCenter)
        .setRecordGroupDescription(record.getRecordGroupDescription)
        .setRecordGroupRunDateEpoch(record.getRecordGroupRunDateEpoch)
        .setRecordGroupFlowOrder(record.getRecordGroupFlowOrder)
        .setRecordGroupKeySequence(record.getRecordGroupKeySequence)
        .setRecordGroupLibrary(record.getRecordGroupLibrary)
        .setRecordGroupPredictedMedianInsertSize(record.getRecordGroupPredictedMedianInsertSize)
        .setRecordGroupPlatform(record.getRecordGroupPlatform)
        .setRecordGroupPlatformUnit(record.getRecordGroupPlatformUnit)
        .setRecordGroupSample(record.getRecordGroupSample)
        .setSangerQuality(record.qualityScores(readPos))
        .setNumReverseStrand(reverseStrandCount)
        .setNumSoftClipped(0)
        .setReadName(record.getReadName)
        .setReadStart(record.getStart)
        .setReadEnd(end)
        .setCountAtPosition(1)

    }

    var referencePos = record.getStart
    val isReverseStrand = record.getReadNegativeStrand
    var readPos = 0

    val cigar = Reads2PileupProcessor.CIGAR_CODEC.decode(record.getCigar.toString)
    val mdTag = MdTag(record.getMismatchingPositions.toString, referencePos)

    var pileupList = List[ADAMPileup]()

    cigar.getCigarElements.asScala.foreach(cigarElement =>
      cigarElement.getOperator match {

        // INSERT
        case CigarOperator.I =>
          var insertPos = 0

          for (b <- new StringOps(record.getSequence.toString.substring(readPos, readPos + cigarElement.getLength))) {
            val insertBase = Base.valueOf(b.toString)

            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand, readPos)
              .setReadBase(insertBase)
              .setRangeOffset(insertPos)
              .setRangeLength(cigarElement.getLength)
              .setReferenceBase(null)
              .build()
            pileupList ::= pileup
            // Consumes the read bases but NOT the reference bases
            readPos += 1
            insertPos += 1
          }
        // MATCH (sequence match or mismatch)
        case CigarOperator.M =>

          for (i <- 0 until cigarElement.getLength) {

            val referenceBase = if (mdTag.isMatch(referencePos)) {
              baseFromSequence(readPos)
            } else {
              mdTag.mismatchedBase(referencePos) match {
                case None => throw new IllegalArgumentException("Cigar match has no MD (mis)match @" + referencePos + " " + record.getCigar + " " + record.getMismatchingPositions) fillInStackTrace()
                case Some(read) => Base.valueOf(read.toString)
              }
            }

            // sequence match
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand, readPos)
              .setReadBase(baseFromSequence(readPos))
              .setReferenceBase(referenceBase)
              .build()
            pileupList ::= pileup

            readPos += 1
            referencePos += 1
          }

        // DELETE
        case CigarOperator.D =>
          for (i <- 0 until cigarElement.getLength) {
            val deletedBase = mdTag.deletedBase(referencePos)
            if (deletedBase.isEmpty) {
              throw new IllegalArgumentException("CIGAR delete but the MD tag is not a delete")
            }
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand, readPos)
              .setReferenceBase(Base.valueOf(deletedBase.get.toString))
              .setRangeOffset(i)
              .setRangeLength(cigarElement.getLength)
              .build()

            pileupList ::= pileup
            // Consume reference bases but not read bases
            referencePos += 1
          }

        // Soft clip
        case CigarOperator.S =>

          var clipPos = 0

          for (i <- 0 until cigarElement.getLength) {
            val readBase = baseFromSequence(readPos)

            // sequence match
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand, readPos)
              .setReadBase(readBase)
              .setNumSoftClipped(1)
              .setRangeOffset(clipPos)
              .setRangeLength(cigarElement.getLength)
              .setReferenceBase(null)
              .build()
            pileupList ::= pileup

            readPos += 1
            clipPos += 1
          }

        // All other cases (TODO: add X and EQ?)
        case _ =>
          if (cigarElement.getOperator.consumesReadBases()) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases()) {
            referencePos += cigarElement.getLength
          }
      }
    )

    pileupList
  }

  def process(reads: RDD[ADAMRecord]): RDD[ADAMPileup] = {
    log.info("Converting " + reads.count + " reads into pileups.")

    val pileups: RDD[ADAMPileup] = reads.flatMap(readToPileups(_))

    log.info("Total of " + pileups.count + " pileups after conversion.")

    pileups
  }
}
