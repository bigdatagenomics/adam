/*
 * Copyright (c) 2013-2014. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.rich

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.{Attribute, ReferenceRegion}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util._
import net.sf.samtools.{CigarElement, CigarOperator, Cigar, TextCigarCodec}
import scala.Some
import scala.collection.immutable.NumericRange
import scala.concurrent.JavaConversions._

object RichADAMRecord {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton
  val ILLUMINA_READNAME_REGEX = "[a-zA-Z0-9]+:[0-9]:([0-9]+):([0-9]+):([0-9]+).*".r

  def apply(record: ADAMRecord) = {
    new RichADAMRecord(record)
  }

  implicit def recordToRichRecord(record: ADAMRecord): RichADAMRecord = new RichADAMRecord(record)
  implicit def richRecordToRecord(record: RichADAMRecord): ADAMRecord = record.record
}

class IlluminaOptics(val tile: Long, val x: Long, val y: Long) {}

class RichADAMRecord(val record: ADAMRecord) {

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
    if (record.getReadMapped) {
      Some(MdTag(record.getMismatchingPositions, record.getStart))
    } else {
      None
    }
  }

  lazy val region: Option[ReferenceRegion] = {
    if (record.getReadMapped) {
      Some(ReferenceRegion(record.getReferenceId, record.getStart, end.get))
    } else {
      None
    }
  }

  private def isClipped(el: CigarElement) = {
    el.getOperator == CigarOperator.SOFT_CLIP ||
      el.getOperator == CigarOperator.HARD_CLIP
  }

  // Returns the end position if the read is mapped, None otherwise
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

  lazy val mdEvent: Option[MdTag] = {
    if (record.getMismatchingPositions != null) {
      Some(MdTag(record.getMismatchingPositions.toString, record.getStart))
    } else {
      None
    }
  }

  // Does this read overlap with the given reference position?
  // FIXME: doesn't check contig! should use ReferenceLocation, not Long
  def overlapsReferencePosition(pos: Long): Option[Boolean] = {
    if (record.getReadMapped) {
      Some(record.getStart <= pos && pos < end.get)
    } else {
      None
    }
  }

  // Does this read mismatch the reference at the given reference position?
  def isMismatchAtReferencePosition(pos: Long): Option[Boolean] = {
    if (mdEvent.isEmpty || !overlapsReferencePosition(pos).get) {
      None
    } else {
      Some(!mdEvent.get.isMatch(pos))
    }
  }

  // Does this read mismatch the reference at the given offset within the read?
  def isMismatchAtReadOffset(offset: Int): Option[Boolean] = {
    // careful about offsets that are within an insertion!
    if (referencePositions.isEmpty) {
      None
    } else {
      readOffsetToReferencePosition(offset).flatMap(isMismatchAtReferencePosition)
    }
  }

  lazy val referencePositions: Seq[Option[Long]] = {
    if (record.getReadMapped) {
      val resultTuple = samtoolsCigar.getCigarElements.foldLeft((unclippedStart.get, List[Option[Long]]()))((runningPos, elem) => {
        // runningPos is a tuple, the first element holds the starting position of the next CigarOperator
        // and the second element is the list of positions up to this point
        val op = elem.getOperator
        val currentRefPos = runningPos._1
        val resultAccum = runningPos._2
        val advanceReference = op.consumesReferenceBases || op == CigarOperator.S
        val newRefPos = currentRefPos + (if(advanceReference) elem.getLength else 0)
        val resultParts: Seq[Option[Long]] =
          if(op.consumesReadBases) {
            val range = NumericRange(currentRefPos, currentRefPos + elem.getLength, 1L)
            range.map(pos => if(advanceReference) Some(pos) else None)
          } else {
            Seq.empty
          }
        (newRefPos, resultAccum ++ resultParts)
      })

      val endRefPos = resultTuple._1
      val results = resultTuple._2
      results.toIndexedSeq
    } else {
      qualityScores.map(t => None)
    }
  }

  def readOffsetToReferencePosition(offset: Int): Option[Long] = {
    if (record.getReadMapped) {
      referencePositions(offset)
    } else {
      None
    }
  }
}
