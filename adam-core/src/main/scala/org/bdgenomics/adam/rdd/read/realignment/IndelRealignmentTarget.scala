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
package org.bdgenomics.adam.rdd.read.realignment

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import htsjdk.samtools.CigarOperator
import org.bdgenomics.utils.misc.Logging
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.instrumentation.Timers._
import scala.collection.JavaConversions._
import scala.collection.immutable.TreeSet

private[realignment] object TargetOrdering extends Ordering[IndelRealignmentTarget] {

  /**
   * Order two indel realignment targets by earlier starting position.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return Comparison done by starting position.
   */
  def compare(a: IndelRealignmentTarget, b: IndelRealignmentTarget): Int = a.readRange compareTo b.readRange

  /**
   * Check to see if an indel realignment target contains the given read.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if read alignment is contained in target span.
   */
  def contains(target: IndelRealignmentTarget, read: AlignmentRecord): Boolean = {
    ReferenceRegion.opt(read).exists(target.readRange.overlaps)
  }

  /**
   * Compares a read to an indel realignment target to see if the target is before the read.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if start of read is before the start of the indel alignment target.
   */
  def lt(target: IndelRealignmentTarget, read: RichAlignmentRecord): Boolean = {
    ReferenceRegion.opt(read.record).exists(target.readRange.compareTo(_) < 0)
  }

  /**
   * Compares two indel realignment targets to see if they overlap.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return True if two targets overlap.
   */
  def overlap(a: IndelRealignmentTarget, b: IndelRealignmentTarget): Boolean = {
    (a.variation.isDefined && a.variation.forall(_.overlaps(b.readRange))) ||
      (b.variation.isDefined && b.variation.forall(_.overlaps(a.readRange)))
  }
}

private[realignment] object IndelRealignmentTarget {

  /**
   * Generates 1+ indel realignment targets from a single read.
   *
   * @param read Read to use for generation.
   * @param maxIndelSize Maximum allowable size of an indel.
   * @return Set of generated realignment targets.
   */
  def apply(
    read: RichAlignmentRecord,
    maxIndelSize: Int): Seq[IndelRealignmentTarget] = CreateIndelRealignmentTargets.time {

    val region = ReferenceRegion.unstranded(read.record)
    val refId = read.record.getReferenceName
    var pos = List[ReferenceRegion]()
    var referencePos = read.record.getStart
    val cigar = read.samtoolsCigar

    cigar.getCigarElements.foreach(cigarElement =>
      cigarElement.getOperator match {
        // INSERT
        case CigarOperator.I =>
          if (cigarElement.getLength <= maxIndelSize) {
            pos ::= ReferenceRegion(refId, referencePos, referencePos + 1)
          }
        // DELETE
        case CigarOperator.D =>
          if (cigarElement.getLength <= maxIndelSize) {
            pos ::= ReferenceRegion(refId, referencePos, referencePos + cigarElement.getLength)
          }
          referencePos += cigarElement.getLength
        case _ =>
          if (cigarElement.getOperator.consumesReferenceBases()) {
            referencePos += cigarElement.getLength
          }
      })

    // if we have indels, emit those targets, else emit a target for this read
    if (pos.size != 1) {
      Seq(new IndelRealignmentTarget(None, region))
    } else {
      pos.map(ir => new IndelRealignmentTarget(Some(ir), region))
        .toSeq
    }
  }
}

private[adam] class IndelRealignmentTargetSerializer extends Serializer[IndelRealignmentTarget] {

  def write(kryo: Kryo, output: Output, obj: IndelRealignmentTarget) = {
    output.writeString(obj.readRange.referenceName)
    output.writeLong(obj.readRange.start)
    output.writeLong(obj.readRange.end)
    output.writeBoolean(obj.variation.isDefined)
    obj.variation.foreach(r => {
      output.writeLong(r.start)
      output.writeLong(r.end)
    })
  }

  def read(kryo: Kryo, input: Input, klazz: Class[IndelRealignmentTarget]): IndelRealignmentTarget = {
    val refName = input.readString()
    val readRange = ReferenceRegion(refName, input.readLong(), input.readLong())
    val variation = if (input.readBoolean()) {
      Some(ReferenceRegion(refName, input.readLong(), input.readLong()))
    } else {
      None
    }
    new IndelRealignmentTarget(variation, readRange)
  }
}

private[adam] class IndelRealignmentTarget(
    val variation: Option[ReferenceRegion],
    val readRange: ReferenceRegion) extends Logging with Serializable {

  assert(variation.map(r => r.referenceName).forall(_ == readRange.referenceName))

  override def toString(): String = {
    variation + " over " + readRange
  }

  /**
   * Merges two indel realignment targets.
   *
   * @param target Target to merge in.
   * @return Merged target.
   */
  def merge(target: IndelRealignmentTarget): IndelRealignmentTarget = {
    assert(readRange.isAdjacent(target.readRange) || readRange.overlaps(target.readRange),
      "Targets do not overlap, and therefore cannot be merged.")

    val newVar = (variation, target.variation) match {
      case (Some(v), Some(tv)) => Some(v.hull(tv))
      case (Some(v), _)        => Some(v)
      case (_, Some(tv))       => Some(tv)
      case _                   => None
    }

    new IndelRealignmentTarget(newVar, readRange.merge(target.readRange))
  }

  def isEmpty: Boolean = {
    variation.isEmpty
  }
}

private[adam] class TargetSetSerializer extends Serializer[TargetSet] {

  val irts = new IndelRealignmentTargetSerializer()

  def write(kryo: Kryo, output: Output, obj: TargetSet) = {
    output.writeInt(obj.set.size)
    obj.set.foreach(innerObj => {
      irts.write(kryo, output, innerObj)
    })
  }

  def read(kryo: Kryo, input: Input, klazz: Class[TargetSet]): TargetSet = {
    val size = input.readInt()
    val array = new Array[IndelRealignmentTarget](size)
    (0 until size).foreach(i => {
      array(i) = irts.read(kryo, input, classOf[IndelRealignmentTarget])
    })
    new TargetSet(TreeSet(array: _*)(TargetOrdering))
  }
}

private[adam] class IndelRealignmentTargetArraySerializer extends Serializer[Array[IndelRealignmentTarget]] {

  private val irts = new IndelRealignmentTargetSerializer

  def write(kryo: Kryo, output: Output, obj: Array[IndelRealignmentTarget]) = {
    output.writeInt(obj.length)
    obj.foreach(irts.write(kryo, output, _))
  }

  def read(kryo: Kryo, input: Input, klazz: Class[Array[IndelRealignmentTarget]]): Array[IndelRealignmentTarget] = {
    val arrSize = input.readInt()
    val arr = new Array[IndelRealignmentTarget](arrSize)
    (0 until arrSize).foreach(idx => {
      arr(idx) = irts.read(kryo, input, classOf[IndelRealignmentTarget])
    })
    arr
  }
}

private[realignment] object TargetSet {
  def apply(): TargetSet = {
    new TargetSet(TreeSet[IndelRealignmentTarget]()(TargetOrdering))
  }
}

// this case class is needed to get around some serialization issues (type erasure)
private[adam] case class TargetSet(set: TreeSet[IndelRealignmentTarget]) extends Serializable {
}
