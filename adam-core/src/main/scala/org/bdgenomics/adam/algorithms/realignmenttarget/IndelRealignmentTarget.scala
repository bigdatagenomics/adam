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
package org.bdgenomics.adam.algorithms.realignmenttarget

import org.bdgenomics.formats.avro.{ ADAMPileup, ADAMRecord }
import org.bdgenomics.adam.rich.RichADAMRecord._
import scala.collection.immutable.{ TreeSet, HashSet, NumericRange }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.apache.spark.Logging
import scala.util.Sorting.quickSort

object ZippedTargetOrdering extends Ordering[(IndelRealignmentTarget, Int)] {

  /**
   * Order two indel realignment targets by earlier starting position.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return Comparison done by starting position.
   */
  def compare(a: (IndelRealignmentTarget, Int), b: (IndelRealignmentTarget, Int)): Int = {
    a._1.getReadRange.start compare b._1.getReadRange.start
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
  def compare(a: IndelRealignmentTarget, b: IndelRealignmentTarget): Int = a.getReadRange.start compare b.getReadRange.start

  /**
   * Compares a read to an indel realignment target to see if it starts before the start of the indel realignment target.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if start of read is before the start of the indel alignment target.
   */
  def lt(target: IndelRealignmentTarget, read: ADAMRecord): Boolean = target.getReadRange.start < read.getStart

  /**
   * Check to see if an indel realignment target and a read are mapped over the same length.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if read alignment span is identical to the target span.
   */
  def equals(target: IndelRealignmentTarget, read: ADAMRecord): Boolean = {
    (target.getReadRange.start == read.getStart) && (target.getReadRange.end == read.end.get)
  }

  /**
   * Check to see if an indel realignment target contains the given read.
   *
   * @param target Realignment target to compare.
   * @param read Read to compare.
   * @return True if read alignment is contained in target span.
   */
  def contains(target: IndelRealignmentTarget, read: ADAMRecord): Boolean = {
    (target.getReadRange.start <= read.getStart) && (target.getReadRange.end >= read.end.get - 1) // -1 since read end is non-inclusive
  }

  /**
   * Compares two indel realignment targets to see if they overlap.
   *
   * @param a Indel realignment target to compare.
   * @param b Indel realignment target to compare.
   * @return True if two targets overlap.
   */
  def overlap(a: IndelRealignmentTarget, b: IndelRealignmentTarget): Boolean = {
    // Note: the last two conditions were added for completeness; they should generally not
    // be necessary although maybe in weird cases (indel on both reads in a mate pair that
    // span a structural variant) and then one probably would not want to re-align these
    // together.
    // TODO: introduce an upper bound on re-align distance as GATK does??
    ((a.getReadRange.start >= b.getReadRange.start && a.getReadRange.start <= b.getReadRange.end) ||
      (a.getReadRange.end >= b.getReadRange.start && a.getReadRange.end <= b.getReadRange.start) ||
      (a.getReadRange.start >= b.getReadRange.start && a.getReadRange.end <= b.getReadRange.end) ||
      (b.getReadRange.start >= a.getReadRange.start && b.getReadRange.end <= a.getReadRange.end))
  }
}

abstract class GenericRange(val readRange: NumericRange[Long]) {

  def getReadRange(): NumericRange[Long] = readRange

  def merge(r: GenericRange): GenericRange

  def compareRange(other: GenericRange): Int

  def compareReadRange(other: GenericRange) = {
    if (readRange.start != other.getReadRange().start)
      readRange.start.compareTo(other.getReadRange().start)
    else
      readRange.end.compareTo(other.getReadRange().end)
  }
}

object IndelRange {
  val emptyRange = IndelRange(
    new NumericRange.Inclusive[Long](-1, -1, 1),
    new NumericRange.Inclusive[Long](-1, -1, 1))
}

case class IndelRange(indelRange: NumericRange[Long], override val readRange: NumericRange[Long]) extends GenericRange(readRange) with Ordered[IndelRange] {

  /**
   * Merge two identical indel ranges.
   *
   * @param ir Indel range to merge in.
   * @return Merged range.
   */
  override def merge(ir: GenericRange): IndelRange = {
    if (this == IndelRange.emptyRange)
      ir

    assert(indelRange == ir.asInstanceOf[IndelRange].getIndelRange)
    // do not need to check read range - read range must contain indel range, so if
    // indel range is the same, read ranges will overlap

    new IndelRange(indelRange,
      new NumericRange.Inclusive[Long](
        readRange.start min ir.readRange.start,
        readRange.end max ir.readRange.end,
        1))
  }

  def getIndelRange(): NumericRange[Long] = indelRange

  override def compareRange(other: GenericRange): Int =
    if (indelRange.start != other.asInstanceOf[IndelRange].indelRange.start)
      indelRange.start.compareTo(other.asInstanceOf[IndelRange].indelRange.start)
    else
      indelRange.end.compareTo(other.asInstanceOf[IndelRange].indelRange.end)

  override def compare(other: IndelRange): Int = {
    val cmp = compareRange(other)
    if (cmp != 0)
      cmp
    else
      super.compareReadRange(other)
  }
}

class IndelRangeSerializer extends Serializer[IndelRange] {
  def write(kryo: Kryo, output: Output, obj: IndelRange) = {
    output.writeLong(obj.getIndelRange().start)
    output.writeLong(obj.getIndelRange().end)
    output.writeLong(obj.getReadRange().start)
    output.writeLong(obj.getReadRange().end)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[IndelRange]): IndelRange = {
    val irStart = input.readLong()
    val irEnd = input.readLong()
    val rrStart = input.readLong()
    val rrEnd = input.readLong()
    new IndelRange(
      new NumericRange.Inclusive[Long](irStart, irEnd, 1),
      new NumericRange.Inclusive[Long](rrStart, rrEnd, 1))
  }
}

object SNPRange {
  val emptyRange = SNPRange(
    -1L,
    new NumericRange.Inclusive[Long](-1, -1, 1))
}

case class SNPRange(snpSite: Long, override val readRange: NumericRange[Long]) extends GenericRange(readRange) with Ordered[SNPRange] {

  /**
   * Merge two identical SNP sites.
   *
   * @param sr SNP range to merge in.
   * @return Merged SNP range.
   */
  override def merge(sr: GenericRange): SNPRange = {
    if (this == SNPRange.emptyRange)
      sr

    assert(snpSite == sr.asInstanceOf[SNPRange].getSNPSite)
    // do not need to check read range - read range must contain snp site, so if
    // snp site is the same, read ranges will overlap

    new SNPRange(snpSite,
      new NumericRange.Inclusive[Long](
        readRange.start min sr.readRange.start,
        readRange.end max sr.readRange.end,
        1))
  }

  def getSNPSite(): Long = snpSite

  override def compare(other: SNPRange): Int = {
    val cmp = compareRange(other)
    if (cmp != 0)
      cmp
    else
      super.compareReadRange(other)
  }

  override def compareRange(other: GenericRange): Int =
    snpSite.compareTo(other.asInstanceOf[SNPRange].snpSite)
}

class SNPRangeSerializer extends Serializer[SNPRange] {
  def write(kryo: Kryo, output: Output, obj: SNPRange) = {
    output.writeLong(obj.getSNPSite())
    output.writeLong(obj.getReadRange().start)
    output.writeLong(obj.getReadRange().end)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[SNPRange]): SNPRange = {
    val SNPSite = input.readLong()
    val rrStart = input.readLong()
    val rrEnd = input.readLong()
    new SNPRange(
      SNPSite,
      new NumericRange.Inclusive[Long](rrStart, rrEnd, 1))
  }
}

object IndelRealignmentTarget {

  // threshold for determining whether a pileup contains sufficient mismatch evidence
  val mismatchThreshold = 0.15

  /**
   * Generates an indel realignment target from a pileup.
   *
   * @param rod Base pileup.
   * @return Generated realignment target.
   */
  def apply(rod: Iterable[ADAMPileup]): IndelRealignmentTarget = {

    /**
     * If we have a indel in a pileup position, generates an indel range.
     *
     * @param pileup Single pileup position.
     * @return Indel range.
     */
    def mapEvent(pileup: ADAMPileup): IndelRange = {
      Option(pileup.getReadBase) match {
        case None => {
          // deletion
          new IndelRange(
            new NumericRange.Inclusive[Long](
              pileup.getPosition.toLong - pileup.getRangeOffset.toLong,
              pileup.getPosition.toLong + pileup.getRangeLength.toLong - pileup.getRangeOffset.toLong - 1,
              1),
            new NumericRange.Inclusive[Long](pileup.getReadStart.toLong, pileup.getReadEnd.toLong - 1, 1))
        }
        case Some(o) => {
          // insert
          new IndelRange(
            new NumericRange.Inclusive[Long](pileup.getPosition.toLong, pileup.getPosition.toLong, 1),
            new NumericRange.Inclusive[Long](pileup.getReadStart.toLong, pileup.getReadEnd.toLong - 1, 1))
        }
      }
    }

    /**
     * If we have a point event, generates a SNPRange.
     *
     * @param pileup Pileup position with mismatch evidence.
     * @return SNP range.
     */
    def mapPoint(pileup: ADAMPileup): SNPRange = {
      val range: NumericRange.Inclusive[Long] =
        new NumericRange.Inclusive[Long](pileup.getReadStart.toLong, pileup.getReadEnd.toLong - 1, 1)
      new SNPRange(pileup.getPosition, range)
    }

    // segregate into indels, matches, and mismatches
    val indels = extractIndels(rod)
    val matches = extractMatches(rod)
    val mismatches = extractMismatches(rod)

    // TODO: this assumes Sanger encoding; how about older data? Should there be a property somewhere?
    // calculate the quality of the matches and the mismatches
    val matchQuality: Int =
      if (matches.size > 0)
        matches.map(_.getSangerQuality).reduce(_ + _)
      else
        0
    val mismatchQuality: Int =
      if (mismatches.size > 0)
        mismatches.map(_.getSangerQuality).reduce(_ + _)
      else
        0

    // check our mismatch ratio - if we have a sufficiently high ratio of mismatch quality, generate a snp event, else just generate indel events
    if (matchQuality == 0 || mismatchQuality.toDouble / matchQuality.toDouble >= mismatchThreshold) {
      new IndelRealignmentTarget(
        new HashSet[IndelRange]().union(indels.map(mapEvent).toSet),
        new HashSet[SNPRange]().union(mismatches.map(mapPoint).toSet))
    } else {
      new IndelRealignmentTarget(
        new HashSet[IndelRange]().union(indels.map(mapEvent).toSet), HashSet[SNPRange]())
    }
  }

  def extractMismatches(rod: Iterable[ADAMPileup]): Iterable[ADAMPileup] = {
    rod.filter(r => r.getRangeOffset == null && r.getNumSoftClipped == 0)
      .filter(r => r.getReadBase != r.getReferenceBase)
  }

  def extractMatches(rod: Iterable[ADAMPileup]): Iterable[ADAMPileup] =
    rod.filter(r => r.getRangeOffset == null && r.getNumSoftClipped == 0)
      .filter(r => r.getReadBase == r.getReferenceBase)

  def extractIndels(rod: Iterable[ADAMPileup]): Iterable[ADAMPileup] =
    rod.filter(_.getRangeOffset != null)

  /**
   * @return An empty target that has no indel nor SNP evidence.
   */
  def emptyTarget(): IndelRealignmentTarget = {
    new IndelRealignmentTarget(new HashSet[IndelRange](), new HashSet[SNPRange]())
  }
}

class RangeAccumulator[T <: GenericRange](val data: List[T], val previous: T) {
  def accumulate(current: T): RangeAccumulator[T] = {
    if (previous == null)
      new RangeAccumulator[T](data, current)
    else if (previous.compareRange(current) == 0)
      new RangeAccumulator[T](data, previous.merge(current).asInstanceOf[T])
    else
      new RangeAccumulator[T](previous :: data, current)
  }
}

class IndelRealignmentTarget(val indelSet: Set[IndelRange], val snpSet: Set[SNPRange]) extends Logging {

  // the maximum range covered by either snps or indels
  def readRange: NumericRange.Inclusive[Long] = {
    (
      indelSet.toList.map(_.getReadRange.asInstanceOf[NumericRange.Inclusive[Long]]) ++
      snpSet.toList.map(_.getReadRange.asInstanceOf[NumericRange.Inclusive[Long]])).reduce(
        (a: NumericRange.Inclusive[Long], b: NumericRange.Inclusive[Long]) =>
          new NumericRange.Inclusive[Long]((a.start min b.start), (a.end max b.end), 1))
  }

  /**
   * Merges two indel realignment targets.
   *
   * @param target Target to merge in.
   * @return Merged target.
   */
  def merge(target: IndelRealignmentTarget): IndelRealignmentTarget = {

    // TODO: this is unnecessarily wasteful; if the sets themselves
    // were sorted (requires refactoring) we could achieve the same
    // in a single merge (as in mergesort) operation. This should
    // be done once correctness has been established
    val currentIndelSet = indelSet.union(target.getIndelSet()).toArray
    quickSort(currentIndelSet)

    val accumulator: RangeAccumulator[IndelRange] = new RangeAccumulator[IndelRange](List(), null)
    val newIndelSetAccumulated: RangeAccumulator[IndelRange] = currentIndelSet.foldLeft(accumulator) {
      (acc, elem) => acc.accumulate(elem)
    }

    if (newIndelSetAccumulated.previous == null) // without the if we end up with a singleton set with null as element
      new IndelRealignmentTarget(newIndelSetAccumulated.data.toSet, snpSet ++ target.getSNPSet)
    else
      new IndelRealignmentTarget(newIndelSetAccumulated.data.toSet + newIndelSetAccumulated.previous, snpSet ++ target.getSNPSet)
  }

  def isEmpty(): Boolean = {
    indelSet.isEmpty && snpSet.isEmpty
  }

  def getReadRange(): NumericRange[Long] = {
    if ((snpSet != null || indelSet != null)
      && (readRange == null))
      log.warn("snpSet or indelSet non-empty but readRange empty!")
    readRange
  }

  def getSortKey(): Long = {
    if (readRange != null)
      readRange.start
    else if (!getIndelSet().isEmpty && getSNPSet().isEmpty)
      getIndelSet().head.getReadRange().start
    else if (getIndelSet().isEmpty && !getSNPSet().isEmpty)
      getSNPSet().head.getReadRange().start
    else {
      log.error("unknown sort key for IndelRealignmentTarget")
      -1.toLong
    }

  }

  protected[realignmenttarget] def getSNPSet(): Set[SNPRange] = snpSet

  protected[realignmenttarget] def getIndelSet(): Set[IndelRange] = indelSet

}

class TreeSetSerializer extends Serializer[TreeSet[IndelRealignmentTarget]] {

  def write(kryo: Kryo, output: Output, obj: TreeSet[IndelRealignmentTarget]) = {
    kryo.writeClassAndObject(output, obj.toList)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[TreeSet[IndelRealignmentTarget]]): TreeSet[IndelRealignmentTarget] = {
    new TreeSet()(TargetOrdering).union(kryo.readClassAndObject(input).asInstanceOf[List[IndelRealignmentTarget]].toSet)
  }
}
