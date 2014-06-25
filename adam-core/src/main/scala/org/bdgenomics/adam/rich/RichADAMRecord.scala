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

import org.bdgenomics.formats.avro.ADAMRecord
import net.sf.samtools.{ CigarElement, CigarOperator, Cigar, TextCigarCodec }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util._
import scala.Some
import scala.collection.immutable.NumericRange
import org.bdgenomics.adam.models.{ ReferenceRegion, ReferencePosition, Attribute }
import java.util.regex.Pattern

object RichADAMRecord {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton
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

  def apply(record: ADAMRecord) = {
    new RichADAMRecord(record)
  }

  implicit def recordToRichRecord(record: ADAMRecord): RichADAMRecord = new RichADAMRecord(record)
  implicit def richRecordToRecord(record: RichADAMRecord): ADAMRecord = record.record
}

class IlluminaOptics(val tile: Long, val x: Long, val y: Long) {}

class RichADAMRecord(val record: ADAMRecord) {

  lazy val referenceLength: Int = RichADAMRecord.referenceLengthFromCigar(record.getCigar.toString)

  lazy val readRegion = ReferenceRegion(this)

  // Returns the quality scores as a list of bytes
  lazy val qualityScores: Array[Int] = record.getQual.toString.toCharArray.map(q => (q - 33))

  // Parse the tags ("key:type:value" triples)
  lazy val tags: Seq[Attribute] = AttributeUtils.parseAttributes(record.getAttributes.toString)

  // Parses the readname to Illumina optics information
  lazy val illuminaOptics: Option[IlluminaOptics] = {
    try {
      val RichADAMRecord.ILLUMINA_READNAME_REGEX(tile, x, y) = record.getReadName
      Some(new IlluminaOptics(tile.toInt, x.toInt, y.toInt))
    } catch {
      case e: MatchError => None
    }
  }

  lazy val samtoolsCigar: Cigar = {
    RichADAMRecord.CIGAR_CODEC.decode(record.getCigar.toString)
  }

  // Returns the MdTag if the read is mapped, None otherwise
  lazy val mdTag: Option[MdTag] = {
    if (record.getReadMapped && record.getMismatchingPositions != null) {
      Some(MdTag(record.getMismatchingPositions, record.getStart))
    } else {
      None
    }
  }

  private def isClipped(el: CigarElement) = {
    el.getOperator == CigarOperator.SOFT_CLIP ||
      el.getOperator == CigarOperator.HARD_CLIP
  }

  // Returns the exclusive end position if the read is mapped, None otherwise
  lazy val end: Option[Long] = {
    if (record.getReadMapped) {
      Some(samtoolsCigar.getCigarElements
        .filter(p => p.getOperator.consumesReferenceBases())
        .foldLeft(record.getStart) {
          (pos, cigarEl) => pos + cigarEl.getLength
        })
    } else {
      None
    }
  }

  // Returns the position of the unclipped end if the read is mapped, None otherwise
  lazy val unclippedEnd: Option[Long] = {
    if (record.getReadMapped) {
      Some(samtoolsCigar.getCigarElements.reverse.takeWhile(isClipped).foldLeft(end.get) {
        (pos, cigarEl) => pos + cigarEl.getLength
      })
    } else {
      None
    }
  }

  // Returns the position of the unclipped start if the read is mapped, None otherwise.
  lazy val unclippedStart: Option[Long] = {
    if (record.getReadMapped) {
      Some(samtoolsCigar.getCigarElements.takeWhile(isClipped).foldLeft(record.getStart) {
        (pos, cigarEl) => pos - cigarEl.getLength
      })
    } else {
      None
    }
  }

  // Return the 5 prime position.
  def fivePrimePosition: Option[Long] = {
    if (record.getReadMapped) {
      if (record.getReadNegativeStrand) unclippedEnd else unclippedStart
    } else {
      None
    }
  }

  // Does this read overlap with the given reference position?
  def overlapsReferencePosition(pos: ReferencePosition): Option[Boolean] = {
    readRegion.map(_.contains(pos))
  }

  // Does this read mismatch the reference at the given reference position?
  def isMismatchAtReferencePosition(pos: ReferencePosition): Option[Boolean] = {
    if (mdTag.isEmpty || !overlapsReferencePosition(pos).get) {
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
    val position = if (ReferencePosition.mappedPositionCheck(record)) {
      Some(new ReferencePosition(record.getContig.getContigName.toString, referencePosition))
    } else {
      None
    }

    def getReferenceBase(cigarElement: CigarElement, refPos: Long, readPos: Int): Option[Char] = {
      mdTag.flatMap(tag => {
        cigarElement.getOperator match {
          case CigarOperator.M => {
            if (!tag.isMatch(refPos)) {
              tag.mismatchedBase(refPos)
            } else {
              Some(record.getSequence()(readPos))
            }
          }
          case CigarOperator.D => {
            // if a delete, get from the delete pool
            tag.deletedBase(refPos)
          }
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
      val resultTuple = samtoolsCigar.getCigarElements.foldLeft((unclippedStart.get, List[Option[ReferenceSequenceContext]]()))((runningPos, elem) => {
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
