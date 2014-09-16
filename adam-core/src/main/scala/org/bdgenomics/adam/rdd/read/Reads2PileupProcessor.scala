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
package org.bdgenomics.adam.rdd.read

import htsjdk.samtools.{ CigarOperator, TextCigarCodec }
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rich.RichAlignmentRecord._
import org.bdgenomics.adam.util._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Base, Pileup }
import scala.collection.JavaConverters._
import scala.collection.immutable.StringOps

private[rdd] object Reads2PileupProcessor {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton
}

/**
 * Class that converts reads into pileups.
 *
 * @param createSecondaryAlignments If true, we process reads that are not at their primary alignment.
 * If false, we only process reads that are at their primary alignment. Default is false.
 */
private[rdd] class Reads2PileupProcessor(createSecondaryAlignments: Boolean = false)
    extends Serializable with Logging {

  /**
   * Converts a single read into a list of pileups.
   *
   * @param record Read to convert.
   * @return A list of pileups.
   */
  def readToPileups(record: AlignmentRecord): List[Pileup] = {
    if (record == null ||
      record.getCigar == null ||
      record.getReadMapped == null ||
      !record.getReadMapped ||
      record.getPrimaryAlignment == null) {
      // TODO: log this later... We can't create a pileup without the CIGAR
      return List.empty
    }

    if (!(createSecondaryAlignments || record.getPrimaryAlignment)) {
      return List.empty
    }

    def baseFromSequence(pos: Int): Base = {
      Base.valueOf(record.getSequence.subSequence(pos, pos + 1).toString)
    }

    def sangerScoreToInt(score: String, position: Int): Int = score(position).toInt - 33

    def populatePileupFromReference(record: AlignmentRecord, referencePos: Long, isReverseStrand: Boolean, readPos: Int): Pileup.Builder = {

      var reverseStrandCount = 0

      if (isReverseStrand) {
        reverseStrandCount = 1
      }

      // check read mapping locations
      assert(record.getStart != null, "Read is mapped but has a null start position.")

      val end = record.getEnd
      assert(end != -1L, "Read is mapped but has a null end position. Read:\n" + record)

      Pileup.newBuilder()
        .setContig(record.getContig)
        .setMapQuality(record.getMapq)
        .setPosition(referencePos)
        .setSampleId(record.getRecordGroupSample)
        .setSangerQuality(record.qualityScores(readPos))

    }

    var referencePos = record.getStart
    val isReverseStrand = record.getReadNegativeStrand
    var readPos = 0

    val cigar = Reads2PileupProcessor.CIGAR_CODEC.decode(record.getCigar.toString)
    val mdTag: Option[MdTag] = if (record.getMismatchingPositions == null) {
      None
    } else {
      Some(MdTag(record.getMismatchingPositions.toString, referencePos))
    }

    var pileupList = List[Pileup]()

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

            val referenceBase: Option[Base] = if (mdTag.isDefined && mdTag.get.isMatch(referencePos)) {
              Some(baseFromSequence(readPos))
            } else {
              if (mdTag.isDefined) {
                mdTag.get.mismatchedBase(referencePos) match {
                  case None       => throw new IllegalArgumentException("Cigar match has no MD (mis)match @" + referencePos + " " + record.getCigar + " " + record.getMismatchingPositions) fillInStackTrace ()
                  case Some(read) => Some(Base.valueOf(read.toString))
                }
              } else {
                None
              }
            }

            // sequence match
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand, readPos)
              .setReadBase(baseFromSequence(readPos))

            referenceBase.foreach(b => pileup.setReferenceBase(b))

            pileupList ::= pileup.build()

            readPos += 1
            referencePos += 1
          }

        // DELETE
        case CigarOperator.D =>
          for (i <- 0 until cigarElement.getLength) {
            val deletedBase: Option[Char] = if (mdTag.isDefined) {
              val db = mdTag.get.deletedBase(referencePos)

              if (db.isEmpty) {
                throw new IllegalArgumentException("CIGAR delete but the MD tag is not a delete")
              }

              db
            } else {
              None
            }

            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand, readPos)
              .setRangeOffset(i)
              .setRangeLength(cigarElement.getLength)

            deletedBase.foreach(b => pileup.setReferenceBase(Base.valueOf(b.toString)))

            pileupList ::= pileup.build()
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
              .setRangeOffset(clipPos)
              .setRangeLength(cigarElement.getLength)
              .setReferenceBase(null)
              .build()
            pileupList ::= pileup

            readPos += 1
            clipPos += 1
          }

        case CigarOperator.EQ =>

          for (i <- 0 until cigarElement.getLength) {

            val referenceBase = baseFromSequence(readPos)

            // sequence match
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand, readPos)
              .setReadBase(baseFromSequence(readPos))
              .setReferenceBase(referenceBase)
              .build()
            pileupList ::= pileup

            readPos += 1
            referencePos += 1
          }

        case CigarOperator.X =>

          for (i <- 0 until cigarElement.getLength) {

            val referenceBase: Option[Base] = if (mdTag.isDefined) {
              mdTag.get.mismatchedBase(referencePos) match {
                case None       => throw new IllegalArgumentException("Cigar match has no MD (mis)match @" + referencePos + " " + record.getCigar + " " + record.getMismatchingPositions) fillInStackTrace ()
                case Some(read) => Some(Base.valueOf(read.toString))
              }
            } else {
              None
            }

            // sequence match
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand, readPos)
              .setReadBase(baseFromSequence(readPos))

            referenceBase.foreach(b => pileup.setReferenceBase(b))

            pileupList ::= pileup.build()

            readPos += 1
            referencePos += 1
          }

        // All other cases)
        case _ =>
          if (cigarElement.getOperator.consumesReadBases()) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases()) {
            referencePos += cigarElement.getLength
          }
      })

    pileupList
  }

  /**
   * Converts an rdd of reads into pileups.
   *
   * @param reads An RDD of reads to convert into pileups.
   * @return An RDD of pileups without known grouping.
   */
  def process(reads: RDD[AlignmentRecord]): RDD[Pileup] = {
    reads.flatMap(readToPileups)
  }
}
