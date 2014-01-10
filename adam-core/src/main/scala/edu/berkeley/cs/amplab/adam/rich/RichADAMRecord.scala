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
package edu.berkeley.cs.amplab.adam.rich

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import net.sf.samtools.{CigarElement, CigarOperator, Cigar, TextCigarCodec}
import scala.collection.JavaConversions._
import edu.berkeley.cs.amplab.adam.util.MdTag

object RichADAMRecord {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton
  val ILLUMINA_READNAME_REGEX = "[a-zA-Z0-9]+:[0-9]:([0-9]+):([0-9]+):([0-9]+).*".r

  def apply(record: ADAMRecord) = {
    new RichADAMRecord(record)
  }

  implicit def recordToRichRecord(record: ADAMRecord): RichADAMRecord = new RichADAMRecord(record)
}

class IlluminaOptics(val tile: Long, val x: Long, val y: Long) {}

class RichADAMRecord(record: ADAMRecord) {

  // Returns the quality scores as a list of bytes
  lazy val qualityScores: Array[Byte] = record.getQual.toString.toCharArray.map(q => (q - 33).toByte)

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

  lazy val mdEvent: Option[MdTag] = if (record.getMismatchingPositions != null) {
    Some(MdTag(record.getMismatchingPositions.toString, record.getStart))
  } else {
    None
  }

  def overlapsPosition(pos: Long): Option[Boolean] = {
    if (record.getReadMapped) {
      Some(record.getStart <= pos && end.get > pos)
    } else {
      None
    }
  }

  def isMismatchBase(pos: Long): Option[Boolean] = {
    if (mdEvent.isEmpty || !overlapsPosition(pos).get)
      None
    else
      Some(!mdEvent.get.isMatch(pos))
  }

  def isMismatchBase(offset: Int): Option[Boolean] = {
    // careful about offsets that are within an insertion!
    if (referencePositions.isEmpty) {
      None
    } else {
      val pos = referencePositions(offset)
      if (pos.isEmpty)
        None
      else
        isMismatchBase(pos.get)
    }
  }

  lazy val referencePositions: Seq[Option[Long]] = {
    if (record.getReadMapped) {
      samtoolsCigar.getCigarElements.foldLeft((unclippedStart.get, List[Option[Long]]()))((runningPos, elem) => {
        // runningPos is a tuple, the first element holds the starting position of the next CigarOperator
        // and the second element is the list of positions up to this point
        val posAtCigar = runningPos._1
        val basePositions = runningPos._2
        elem.getOperator match {
          case CigarOperator.M |
               CigarOperator.X |
               CigarOperator.EQ |
               CigarOperator.S => {
            val positions = Range(posAtCigar.toInt, posAtCigar.toInt + elem.getLength)
            (positions.last + 1, basePositions ++ positions.map(t => Some(t.toLong)))
          }
          case CigarOperator.H => {
            runningPos /* do nothing */
          }
          case CigarOperator.D |
               CigarOperator.P |
               CigarOperator.N => {
            (posAtCigar + elem.getLength, basePositions)
          }
          case CigarOperator.I => {
            (posAtCigar, basePositions ++ (1 to elem.getLength).map(t => None))
          }
        }
      })._2
    } else {
      qualityScores.map(t => None)
    }
  }

  // get the reference position of an offset within the read
  def getPosition(offset: Int): Option[Long] = if (record.getReadMapped) referencePositions(offset) else None
}
