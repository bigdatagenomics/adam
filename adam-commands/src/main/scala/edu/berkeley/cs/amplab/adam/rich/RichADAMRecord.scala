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
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions._

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

  lazy val phredQuals = {
    record.getQual.map(p => p - 33)
  }

  // Calculates the sum of the phred scores that are greater than or equal to 15
  lazy val score = phredQuals.filter(15 <=).sum

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
    RichADAMRecord.CIGAR_CODEC.decode(record.getCigar)
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

}
