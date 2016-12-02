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
package org.bdgenomics.adam.rich

import htsjdk.samtools.{
  Cigar,
  CigarElement,
  CigarOperator,
  TextCigarCodec
}
import org.bdgenomics.adam.models.{
  Attribute,
  MdTag,
  ReferencePosition,
  ReferenceRegion
}
import org.bdgenomics.adam.util.AttributeUtils
import org.bdgenomics.formats.avro.{ AlignmentRecord, Strand }
import scala.collection.JavaConversions._
import scala.collection.immutable.NumericRange
import scala.math.max

object RichAlignmentRecord {

  @deprecated("Use explicit conversion wherever possible in new development.",
    since = "0.21.0")
  implicit def recordToRichRecord(record: AlignmentRecord): RichAlignmentRecord = new RichAlignmentRecord(record)

  @deprecated("Use explicit conversion wherever possible in new development.",
    since = "0.21.0")
  implicit def richRecordToRecord(record: RichAlignmentRecord): AlignmentRecord = record.record
}

/**
 * An enriched version of an Avro AlignmentRecord.
 *
 * @param record The underlying read.
 */
case class RichAlignmentRecord(record: AlignmentRecord) {

  /**
   * The quality scores as a list of integers. Assumes Illumina (33) encoding.
   */
  lazy val qualityScores: Array[Int] = {
    record.getQual.toCharArray.map(q => q - 33)
  }

  /**
   * On access, parses the attribute tags ("key:type:value" triples) into usable
   * records.
   */
  lazy val tags: Seq[Attribute] = AttributeUtils.parseAttributes(record.getAttributes)

  /**
   * Parses the text CIGAR representation of the alignment.
   */
  lazy val samtoolsCigar: Cigar = {
    TextCigarCodec.decode(record.getCigar)
  }

  /**
   * The MdTag if the read is mapped, None otherwise
   */
  lazy val mdTag: Option[MdTag] = {
    if (record.getReadMapped && record.getMismatchingPositions != null) {
      Some(MdTag(record.getMismatchingPositions, record.getStart, TextCigarCodec.decode(record.getCigar)))
    } else {
      None
    }
  }

  private def isClipped(el: CigarElement) = {
    el.getOperator == CigarOperator.SOFT_CLIP ||
      el.getOperator == CigarOperator.HARD_CLIP
  }

  /**
   * The position of the unclipped end if the read is mapped, None otherwise.
   *
   * @note The unclipped position assumes that any clipped bases would've been
   *   aligned as an alignment match.
   */
  lazy val unclippedEnd: Long = {
    max(0L, samtoolsCigar.getCigarElements.reverse.takeWhile(isClipped).foldLeft(record.getEnd)({
      (pos, cigarEl) => pos + cigarEl.getLength
    }))
  }

  /**
   * The position of the unclipped start if the read is mapped, None otherwise.
   *
   * @note The unclipped position assumes that any clipped bases would've been
   *   aligned as an alignment match.
   */
  lazy val unclippedStart: Long = {
    max(0L, samtoolsCigar.getCigarElements.takeWhile(isClipped).foldLeft(record.getStart)({
      (pos, cigarEl) => pos - cigarEl.getLength
    }))
  }

  /**
   * @return The position of the five prime end of the read.
   */
  def fivePrimePosition: Long = {
    if (record.getReadNegativeStrand) unclippedEnd else unclippedStart
  }

  /**
   * @return The position of the five prime end of the read, wrapped as a
   *   reference position.
   */
  def fivePrimeReferencePosition: ReferencePosition = {
    val strand = if (record.getReadNegativeStrand) {
      Strand.REVERSE
    } else {
      Strand.FORWARD
    }
    ReferencePosition(record.getContigName, fivePrimePosition, strand)
  }

  /**
   * @param pos The reference position to check for overlap.
   * @return Returns true if this read overlaps the given reference position.
   */
  def overlapsReferencePosition(pos: ReferencePosition): Boolean = {
    ReferenceRegion.opt(record).exists(_.overlaps(pos))
  }

  /**
   * @param pos The reference position to check for a mismatch.
   * @return Returns true if this read overlaps the given reference position, and
   *   the base aligned at this position is a mismatch against the reference genome..
   */
  def isMismatchAtReferencePosition(pos: ReferencePosition): Option[Boolean] = {
    if (mdTag.isEmpty || !overlapsReferencePosition(pos)) {
      None
    } else {
      mdTag.map(!_.isMatch(pos))
    }
  }

  /**
   * @param offset The index into the read sequence.
   * @return If the read is not aligned, returns a None. Else, returns a wrapped
   *   boolean stating whether this read is a sequence mismatch against the
   *   reference at the given offset within the read?
   */
  def isMismatchAtReadOffset(offset: Int): Option[Boolean] = {
    // careful about offsets that are within an insertion!
    if (referencePositions.isEmpty) {
      None
    } else {
      readOffsetToReferencePosition(offset)
        .flatMap(pos => isMismatchAtReferencePosition(pos))
    }
  }

  private def getReferenceContext(
    readOffset: Int,
    referencePosition: Long,
    cigarElem: CigarElement,
    elemOffset: Int): ReferenceSequenceContext = {
    val position = if (record.getReadMapped) {
      Some(ReferencePosition(record.getContigName, referencePosition))
    } else {
      None
    }

    def getReferenceBase(cigarElement: CigarElement,
                         refPos: Long,
                         readPos: Int): Option[Char] = {
      mdTag.flatMap(tag => {
        cigarElement.getOperator match {
          case CigarOperator.M =>
            if (!tag.isMatch(refPos)) {
              tag.mismatchedBase(refPos)
            } else {
              Some(record.getSequence()(readPos))
            }
          case CigarOperator.D =>
            // if a delete, get from the delete pool
            tag.deletedBase(refPos)
          case _ => None
        }
      })
    }

    val referenceBase = getReferenceBase(cigarElem, referencePosition, readOffset)
    ReferenceSequenceContext(position, referenceBase, cigarElem, elemOffset)
  }

  private[rich] lazy val referencePositions: Seq[Option[ReferencePosition]] = {
    referenceContexts.map(ref => ref.flatMap(_.pos))
  }

  private[rich] lazy val referenceContexts: Seq[Option[ReferenceSequenceContext]] = {
    if (record.getReadMapped) {
      val resultTuple = samtoolsCigar.getCigarElements.foldLeft((unclippedStart, List[Option[ReferenceSequenceContext]]()))((runningPos, elem) => {
        // runningPos is a tuple, the first element holds the starting position of the next CigarOperator
        // and the second element is the list of positions up to this point
        val op = elem.getOperator
        val currentRefPos = runningPos._1
        val resultAccum = runningPos._2
        val advanceReference = op.consumesReferenceBases || op == CigarOperator.S
        val newRefPos = currentRefPos + (if (advanceReference) elem.getLength else 0)
        val resultParts: Seq[Option[ReferenceSequenceContext]] =
          if (op.consumesReadBases) {
            val range = NumericRange(currentRefPos, currentRefPos + elem.getLength, 1L)
            range.zipWithIndex.map(kv =>
              if (advanceReference)
                Some(getReferenceContext(resultAccum.size + kv._2, kv._1, elem, kv._2))
              else None)
          } else {
            Seq.empty
          }
        (newRefPos, resultAccum ++ resultParts)
      })
      val results = resultTuple._2
      results.toIndexedSeq
    } else {
      qualityScores.map(t => None)
    }
  }

  private[rich] def readOffsetToReferencePosition(offset: Int): Option[ReferencePosition] = {
    if (record.getReadMapped) {
      referencePositions(offset)
    } else {
      None
    }
  }

  @deprecated("don't use ReferenceSequenceContext in new development",
    since = "0.21.0")
  private[rich] def readOffsetToReferenceSequenceContext(
    offset: Int): Option[ReferenceSequenceContext] = {
    if (record.getReadMapped) {
      referenceContexts(offset)
    } else {
      None
    }
  }
}
