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

package edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.ReferenceRegion
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import net.sf.samtools.CigarOperator
import org.apache.spark.Logging
import scala.collection.immutable.{TreeSet, HashSet, NumericRange}
import scala.util.Sorting.quickSort

object ZippedTargetOrdering extends Ordering[(IndelRealignmentTarget, Int)] {

  /**
   * Order two indel realignment targets by earlier starting position.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return Comparison done by starting position.
   */
  def compare (a: (IndelRealignmentTarget, Int), b: (IndelRealignmentTarget, Int)) : Int = {
    TargetOrdering.compare(a._1, b._1)
  }
}

object TargetOrdering extends Ordering[IndelRealignmentTarget] {

  /**
   * Order two indel realignment targets by earlier starting position.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return Comparison done by starting position.
   */
  def compare (a: IndelRealignmentTarget, b: IndelRealignmentTarget) : Int = a.readRange compare b.readRange

  /**
   * Check to see if an indel realignment target contains the given read.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if read alignment is contained in target span.
   */
  def contains (target: IndelRealignmentTarget, read: ADAMRecord) : Boolean = {
    val reg = RichADAMRecord(read).region

    reg.forall(r => target.readRange.overlaps(r))
  }

  /**
   * Compares a read to an indel realignment target to see if it starts before the start of the indel realignment target.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if start of read is before the start of the indel alignment target.
   */
  def lt (target: IndelRealignmentTarget, read: RichADAMRecord) : Boolean = {
    val region = read.region
    
    region.forall(r => target.readRange.compare(r) < 0)
  }

  /**
   * Compares two indel realignment targets to see if they overlap.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return True if two targets overlap.
   */
  def overlap (a: IndelRealignmentTarget, b: IndelRealignmentTarget) : Boolean = {    
    (a.variation.isDefined && a.variation.forall(_.overlaps(b.readRange))) || 
    (b.variation.isDefined && b.variation.forall(_.overlaps(a.readRange)))
  }
}

object IndelRealignmentTarget {

  /**
   * Generates 1+ indel realignment targets from a single read.
   *
   * @param read Read to use for generation.
   * @param maxIndelSize Maximum allowable size of an indel.
   * @return Set of generated realignment targets.
   */
  def apply(read: RichADAMRecord,
            maxIndelSize: Int): Seq[IndelRealignmentTarget] = {

    val region = read.region.get
    val refId = read.record.getReferenceId
    var pos = List[ReferenceRegion]()
    var referencePos = read.record.getStart
    var cigar = read.samtoolsCigar
    var mdTag = read.mdTag.get

    cigar.getCigarElements.foreach(cigarElement =>
      cigarElement.getOperator match {
        // INSERT
        case CigarOperator.I => {
          if (cigarElement.getLength <= maxIndelSize) {
            pos ::= ReferenceRegion(refId, referencePos, referencePos + 1)
          }
        }
        // DELETE
        case CigarOperator.D => {
          if (cigarElement.getLength <= maxIndelSize) {
            pos ::= ReferenceRegion(refId, referencePos, referencePos + cigarElement.getLength)
          }
          referencePos += cigarElement.getLength
        }
        case _ => {
          if (cigarElement.getOperator.consumesReferenceBases()) {
            referencePos += cigarElement.getLength
          }
        }
      }
    )

    // if we have indels, emit those targets, else emit a target for this read
    if (pos.length == 0) {
      Seq(new IndelRealignmentTarget(None, region))
    } else {
      pos.map(ir => new IndelRealignmentTarget(Some(ir), region))
        .toSeq
    }
  }
}

class IndelRealignmentTarget(val variation: Option[ReferenceRegion],
                             val readRange: ReferenceRegion) extends Logging {

  /**
   * Merges two indel realignment targets.
   *
   * @param target Target to merge in.
   * @return Merged target.
   */
  def merge(target: IndelRealignmentTarget): IndelRealignmentTarget = {
    assert(readRange.isAdjacent(target.readRange) || readRange.overlaps(target.readRange),
           "Targets do not overlap, and therefore cannot be merged.")

    val newVar = if (variation.isDefined && target.variation.isDefined) {
      Some(variation.get.hull(target.variation.get))
    } else if (variation.isDefined) {
      variation
    } else if (target.variation.isDefined) {
      target.variation
    } else {
      None
    }

    new IndelRealignmentTarget(newVar, readRange.merge(target.readRange))
  }

  def isEmpty(): Boolean = {
    variation.isEmpty
  }
}

class TargetSetSerializer extends Serializer[TargetSet] {

  def write (kryo: Kryo, output: Output, obj: TargetSet) = {
    kryo.writeClassAndObject(output, obj.set.toList)
  }

  def read (kryo: Kryo, input: Input, klazz: Class[TargetSet]) : TargetSet = {
    new TargetSet(new TreeSet()(TargetOrdering)
      .union(kryo.readClassAndObject(input).asInstanceOf[List[IndelRealignmentTarget]].toSet))
  }
}

class ZippedTargetSetSerializer extends Serializer[ZippedTargetSet] {

  def write (kryo: Kryo, output: Output, obj: ZippedTargetSet) = {
    kryo.writeClassAndObject(output, obj.set.toList)
  }

  def read (kryo: Kryo, input: Input, klazz: Class[ZippedTargetSet]) : ZippedTargetSet = {
    new ZippedTargetSet(new TreeSet()(ZippedTargetOrdering)
      .union(kryo.readClassAndObject(input).asInstanceOf[List[(IndelRealignmentTarget, Int)]].toSet))
  }
}

object TargetSet {
  def apply(): TargetSet = {
    new TargetSet(TreeSet[IndelRealignmentTarget]()(TargetOrdering))
  }
}

// These two case classes are needed to get around some serialization issues
case class TargetSet (set: TreeSet[IndelRealignmentTarget]) extends Serializable {
}

case class ZippedTargetSet (set: TreeSet[(IndelRealignmentTarget, Int)]) extends Serializable {
}

