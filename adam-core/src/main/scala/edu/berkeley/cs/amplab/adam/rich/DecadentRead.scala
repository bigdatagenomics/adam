/*
 * Copyright (c) 2014 The Regents of the University of California
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
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.util.MdTag
import edu.berkeley.cs.amplab.adam.util.QualityScore
import edu.berkeley.cs.amplab.adam.util.Util
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

object DecadentRead {
  type Residue = DecadentRead#Residue

  // Constructors
  def apply(record: ADAMRecord): DecadentRead = DecadentRead(RichADAMRecord(record))

  def apply(rich: RichADAMRecord): DecadentRead = {
    try {
      new DecadentRead(rich)
    } catch {
      case exc: Exception =>
        val msg = "Error \"%s\" while constructing DecadentRead from ADAMRecord(%s)".format(exc.getMessage, rich.record)
        throw new IllegalArgumentException(msg, exc)
    }
  }

  /**
   * cloy (verb)
   *   1. To fill to loathing; to surfeit.
   *   2. To clog, to glut, or satisfy, as the appetite; to satiate.
   *   3. To fill up or choke up; to stop up.
   */
  def cloy(rdd: RDD[ADAMRecord]): RDD[DecadentRead] = rdd.map(DecadentRead.apply)

  // The inevitable counterpart of the above.
  implicit def decay(rdd: RDD[DecadentRead]): RDD[ADAMRecord] = rdd.map(_.record)
}

class DecadentRead(val record: RichADAMRecord) extends Logging {
  // Can't be a primary alignment unless it has been aligned
  //
  // FIXME: This is currently unenforceable; SAMRecordConverter currently
  // sets PrimaryAlignment by default even on unmapped reads
  //require(!record.getPrimaryAlignment || record.getReadMapped, "Unaligned read can't be a primary alignment")

  // Should have quality scores for all residues
  require(record.getSequence.length == record.qualityScores.length, "sequence and qualityScores must be same length")

  // MapQ should be valid
  require(record.getMapq >= 0 && record.getMapq <= 255, "MapQ must be in [0, 255]")

  // A "residue" is an individual monomer in a polymeric chain, such as DNA.
  class Residue private[DecadentRead](val position: Int) {
    def read = DecadentRead.this

    def base: Char = read.baseSequence(position)

    def quality = QualityScore(record.qualityScores(position))

    def isMismatch(includeInsertions: Boolean = true): Boolean =
      assumingAligned(record.isMismatchAtReadOffset(position).getOrElse(includeInsertions))

    def isSNP: Boolean = isMismatch(false)

    def isInsertion: Boolean =
      assumingAligned(record.isMismatchAtReadOffset(position).isEmpty)

    def referenceLocationOption: Option[ReferenceLocation] =
      assumingAligned(
        record.readOffsetToReferencePosition(position).
        map(refOffset => new ReferenceLocation(record.getReferenceName.toString, refOffset)))

    def referenceLocation: ReferenceLocation =
      referenceLocationOption.getOrElse(
        throw new IllegalArgumentException("Residue has no reference location (may be an insertion)"))
  }

  lazy val readGroup: String = record.getRecordGroupName.toString

  private lazy val baseSequence: String = record.getSequence.toString

  lazy val sequence: IndexedSeq[Residue] = Range(0, baseSequence.length).map(new Residue(_))

  def isAligned: Boolean = record.getReadMapped

  def alignmentQuality: Option[QualityScore] = assumingAligned {
    if(record.getMapq == 255) None else Some(QualityScore(record.getMapq))
  }

  def ensureAligned: Boolean =
    isAligned || (throw new IllegalArgumentException("Read has not been aligned to a reference"))

  def isPrimaryAlignment: Boolean = isAligned && record.getPrimaryAlignment

  def isDuplicate: Boolean = record.getDuplicateRead

  def isNegativeRead: Boolean = record.getReadNegativeStrand

  // Is this the most representative record for this read?
  def isCanonicalRecord: Boolean = isPrimaryAlignment && !isDuplicate

  def mismatchesOption: Option[MdTag] = record.mdEvent

  def mismatches: MdTag =
    mismatchesOption.getOrElse(throw new IllegalArgumentException("Read has no MD tag"))

  private def assumingAligned[T](func: => T): T = {
    ensureAligned
    func
  }
}

// TODO: merge with models.ReferencePosition
class ReferenceLocation(val contig: String, val offset: Long) {
  override def toString = "%s@%s".format(contig, offset)

  override def equals(other: Any): Boolean = other match {
    case that: ReferenceLocation =>
      this.contig == that.contig && this.offset == that.offset

    case _ => false
  }

  override def hashCode = Util.hashCombine(0x922927F8, contig.hashCode, offset.hashCode)
}
