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

import htsjdk.samtools.{ Cigar, CigarElement, CigarOperator, TextCigarCodec }
import java.util.regex.Pattern
import org.bdgenomics.adam.models.{ Attribute, ReferencePosition, ReferenceRegion }
import org.bdgenomics.adam.util._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Strand }
import scala.collection.JavaConversions._
import scala.collection.immutable.NumericRange
import scala.math.max

object RichAlignmentRecord {
  val ILLUMINA_READNAME_REGEX = "[a-zA-Z0-9]+:[0-9]:([0-9]+):([0-9]+):([0-9]+).*".r

  val cigarPattern = Pattern.compile("([0-9]+)([MIDNSHPX=])")

  /**
   * Parses a CIGAR string, and returns the aligned length with respect to the
   * reference genome (i.e. skipping clipping, padding, and insertion operators)
   *
   * @param cigar The CIGAR string whose reference length is to be measured
   * @return A non-negative integer, the sum of the MDNX= operators in the CIGAR string.
   */
  def referenceLengthFromCigar(cigar: String): Int = {
    val m = cigarPattern.matcher(cigar)
    var i = 0
    var len: Int = 0
    while (i < cigar.length) {
      if (m.find(i)) {
        val op = m.group(2)
        if ("MDNX=".indexOf(op) != -1) {
          len += m.group(1).toInt
        }
      } else {
        return len
      }
      i = m.end()
    }
    len
  }

  def apply(record: AlignmentRecord) = {
    new RichAlignmentRecord(record)
  }

  implicit def recordToRichRecord(record: AlignmentRecord): RichAlignmentRecord = new RichAlignmentRecord(record)
  implicit def richRecordToRecord(record: RichAlignmentRecord): AlignmentRecord = record.record
}

class IlluminaOptics(val tile: Long, val x: Long, val y: Long) {}

class RichAlignmentRecord(val record: AlignmentRecord) {

  lazy val referenceLength: Int = RichAlignmentRecord.referenceLengthFromCigar(record.getCigar)

  // Returns the quality scores as a list of bytes
  lazy val qualityScores: Array[Int] = {
    record.getQual.toCharArray.map(q => q - 33)
  }

  // Parse the tags ("key:type:value" triples)
  lazy val tags: Seq[Attribute] = AttributeUtils.parseAttributes(record.getAttributes)

  // Parses the readname to Illumina optics information
  lazy val illuminaOptics: Option[IlluminaOptics] = {
    try {
      val RichAlignmentRecord.ILLUMINA_READNAME_REGEX(tile, x, y) = record.getReadName
      Some(new IlluminaOptics(tile.toInt, x.toInt, y.toInt))
    } catch {
      case e: MatchError => None
    }
  }

  lazy val samtoolsCigar: Cigar = {
    TextCigarCodec.decode(record.getCigar)
  }

  // Returns the MdTag if the read is mapped, None otherwise
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

  // Returns the position of the unclipped end if the read is mapped, None otherwise
  lazy val unclippedEnd: Long = {
    max(0L, samtoolsCigar.getCigarElements.reverse.takeWhile(isClipped).foldLeft(record.getEnd)({
      (pos, cigarEl) => pos + cigarEl.getLength
    }))
  }

  // Returns the position of the unclipped start if the read is mapped, None otherwise.
  lazy val unclippedStart: Long = {
    max(0L, samtoolsCigar.getCigarElements.takeWhile(isClipped).foldLeft(record.getStart)({
      (pos, cigarEl) => pos - cigarEl.getLength
    }))
  }

  // Return the 5 prime position.
  def fivePrimePosition: Long = {
    if (record.getReadNegativeStrand) unclippedEnd else unclippedStart
  }

  def fivePrimeReferencePosition: ReferencePosition = {
    try {
      val strand = if (record.getReadNegativeStrand) {
        Strand.REVERSE
      } else {
        Strand.FORWARD
      }
      ReferencePosition(record.getContigName, fivePrimePosition, strand)
    } catch {
      case e: Throwable => {
        println("caught " + e + " when trying to get position for " + record)
        throw e
      }
    }
  }

  // Does this read overlap with the given reference position?
  def overlapsReferencePosition(pos: ReferencePosition): Boolean = {
    ReferenceRegion.opt(record).exists(_.overlaps(pos))
  }

  // Does this read mismatch the reference at the given reference position?
  def isMismatchAtReferencePosition(pos: ReferencePosition): Option[Boolean] = {
    if (mdTag.isEmpty || !overlapsReferencePosition(pos)) {
      None
    } else {
      mdTag.map(!_.isMatch(pos))
    }
  }

  // Does this read mismatch the reference at the given offset within the read?
  def isMismatchAtReadOffset(offset: Int): Option[Boolean] = {
    // careful about offsets that are within an insertion!
    if (referencePositions.isEmpty) {
      None
    } else {
      readOffsetToReferencePosition(offset).flatMap(pos => isMismatchAtReferencePosition(pos))
    }
  }

  def getReferenceContext(readOffset: Int, referencePosition: Long, cigarElem: CigarElement, elemOffset: Int): ReferenceSequenceContext = {
    val position = if (record.getReadMapped) {
      Some(ReferencePosition(record.getContigName, referencePosition))
    } else {
      None
    }

    def getReferenceBase(cigarElement: CigarElement, refPos: Long, readPos: Int): Option[Char] = {
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

  lazy val referencePositions: Seq[Option[ReferencePosition]] = referenceContexts.map(ref => ref.flatMap(_.pos))

  lazy val referenceContexts: Seq[Option[ReferenceSequenceContext]] = {
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

  def readOffsetToReferencePosition(offset: Int): Option[ReferencePosition] = {
    if (record.getReadMapped) {
      referencePositions(offset)
    } else {
      None
    }
  }

  def readOffsetToReferenceSequenceContext(offset: Int): Option[ReferenceSequenceContext] = {
    if (record.getReadMapped) {
      referenceContexts(offset)
    } else {
      None
    }
  }

}
