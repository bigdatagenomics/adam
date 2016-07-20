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
package org.bdgenomics.adam.models

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.intervaltree.Interval

import scala.math.{ max, min }

trait ReferenceOrdering[T <: ReferenceRegion] extends Ordering[T] {
  private def regionCompare(
    a: T,
    b: T): Int = {
    if (a.referenceName != b.referenceName) {
      a.referenceName.compareTo(b.referenceName)
    } else if (a.start != b.start) {
      a.start.compareTo(b.start)
    } else {
      a.end.compareTo(b.end)
    }
  }

  def compare(
    a: T,
    b: T): Int = {
    val rc = regionCompare(a, b)
    if (rc == 0) {
      a.orientation.ordinal compare b.orientation.ordinal
    } else {
      rc
    }
  }
}

trait OptionalReferenceOrdering[T <: ReferenceRegion] extends Ordering[Option[T]] {
  val baseOrdering: ReferenceOrdering[T]

  def compare(a: Option[T],
              b: Option[T]): Int = (a, b) match {
    case (None, None)         => 0
    case (Some(pa), Some(pb)) => baseOrdering.compare(pa, pb)
    case (Some(pa), None)     => -1
    case (None, Some(pb))     => -1
  }
}

object RegionOrdering extends ReferenceOrdering[ReferenceRegion] {
}
object OptionalRegionOrdering extends OptionalReferenceOrdering[ReferenceRegion] {
  val baseOrdering = RegionOrdering
}

object ReferenceRegion {

  implicit def orderingForPositions = RegionOrdering
  implicit def orderingForOptionalPositions = OptionalRegionOrdering

  /**
   * Generates a reference region from read data. Returns None if the read is not mapped;
   * else, returns the inclusive region from the start to the end of the read alignment.
   *
   * @param record Read to create region from.
   * @return Region corresponding to inclusive region of read alignment, if read is mapped.
   */
  def opt(record: AlignmentRecord): Option[ReferenceRegion] = {
    if (record.getReadMapped) {
      Some(apply(record))
    } else {
      None
    }
  }

  /**
   * Builds a reference region for a called genotype.
   *
   * @param genotype Called genotype to extract region from.
   * @return The site where this genotype lives.
   */
  def apply(genotype: Genotype): ReferenceRegion = {
    ReferenceRegion(genotype.getVariant)
  }

  /**
   * Builds a reference region for a variant site.
   *
   * @param variant Variant to extract region from.
   * @return The site where this variant covers.
   */
  def apply(variant: Variant): ReferenceRegion = {
    ReferenceRegion(variant.getContigName, variant.getStart, variant.getEnd)
  }

  def apply(annotation: DatabaseVariantAnnotation): ReferenceRegion = {
    ReferenceRegion(annotation.getVariant)
  }

  def apply(record: AlignmentRecord): ReferenceRegion = {
    require(record.getReadMapped,
      "Cannot build reference region for unmapped read %s.".format(record))
    require(record.getContigName != null &&
      record.getStart != null &&
      record.getEnd != null,
      "Read %s contains required fields that are null.".format(record))
    ReferenceRegion(record.getContigName, record.getStart, record.getEnd)
  }

  /**
   * Generates a region from a given position -- the region will have a length of 1.
   * @param pos The position to convert
   * @return A 1-wide region at the same location as pos
   */
  def apply(pos: ReferencePosition): ReferenceRegion =
    ReferenceRegion(pos.referenceName, pos.pos, pos.pos + 1)

  /**
   * Generates a reference region from assembly data. Returns None if the assembly does not
   * have an ID or a start position.
   *
   * @param fragment Assembly fragment from which to generate data.
   * @return Region corresponding to inclusive region of contig fragment.
   */
  def apply(fragment: NucleotideContigFragment): Option[ReferenceRegion] = {
    for {
      contig <- Option(fragment.getContig)
      contigName <- Option(contig.getContigName)
      startPosition <- Option(fragment.getFragmentStartPosition)
      fragmentSequence = fragment.getFragmentSequence
    } yield {
      ReferenceRegion(
        contig.getContigName,
        fragment.getFragmentStartPosition,
        fragment.getFragmentStartPosition + fragmentSequence.length
      )
    }
  }

  /**
   * Builds a reference region for a feature.
   *
   * @param feature Feature to extract ReferenceRegion from
   * @return Extracted ReferenceRegion
   */
  def apply(feature: Feature): ReferenceRegion = {
    new ReferenceRegion(feature.getContigName, feature.getStart, feature.getEnd)
  }

  /**
   * Builds a reference region for a coverage site.
   *
   * @param coverage Coverage to extract ReferenceRegion from
   * @return Extracted ReferenceRegion
   */
  def apply(coverage: Coverage): ReferenceRegion = {
    new ReferenceRegion(coverage.contigName, coverage.start, coverage.end)
  }
}

/**
 * Represents a contiguous region of the reference genome.
 *
 * @param referenceName The name of the sequence (chromosome) in the reference genome
 * @param start The 0-based residue-coordinate for the start of the region
 * @param end The 0-based residue-coordinate for the first residue <i>after</i> the start
 *            which is <i>not</i> in the region -- i.e. [start, end) define a 0-based
 *            half-open interval.
 */
case class ReferenceRegion(
  referenceName: String,
  start: Long,
  end: Long,
  orientation: Strand = Strand.INDEPENDENT)
    extends Comparable[ReferenceRegion]
    with Interval {

  assert(start >= 0 && end >= start, "Failed when trying to create region %s %d %d on %s strand.".format(referenceName, start, end, orientation))

  def disorient: ReferenceRegion = new ReferenceRegion(referenceName, start, end)

  /**
   * Merges two reference regions that are contiguous.
   *
   * @throws AssertionError Thrown if regions are not overlapping or adjacent.
   *
   * @param region Other region to merge with this region.
   * @return The merger of both unions.
   *
   * @see hull
   */
  def merge(region: ReferenceRegion): ReferenceRegion = {
    assert(overlaps(region) || isAdjacent(region), "Cannot merge two regions that do not overlap or are not adjacent")
    hull(region)
  }

  /**
   * Calculates the intersection of two reference regions.
   *
   * @param region Region to intersect with.
   * @return A smaller reference region.
   */
  def intersection(region: ReferenceRegion): ReferenceRegion = {
    assert(overlaps(region), "Cannot calculate the intersection of non-overlapping regions.")
    ReferenceRegion(referenceName, max(start, region.start), min(end, region.end))
  }

  /**
   * Creates a region corresponding to the convex hull of two regions. Has no preconditions about the adjacency or
   * overlap of two regions. However, regions must be in the same reference space.
   *
   * @throws AssertionError Thrown if regions are in different reference spaces.
   *
   * @param region Other region to compute hull of with this region.
   * @return The convex hull of both unions.
   *
   * @see merge
   */
  def hull(region: ReferenceRegion): ReferenceRegion = {
    assert(orientation == region.orientation, "Cannot compute convex hull of differently oriented regions.")
    assert(referenceName == region.referenceName, "Cannot compute convex hull of regions on different references.")
    ReferenceRegion(referenceName, min(start, region.start), max(end, region.end))
  }

  /**
   * Returns whether two regions are adjacent. Adjacent regions do not overlap, but have no separation between start/end.
   *
   * @param region Region to compare against.
   * @return True if regions are adjacent.
   */
  def isAdjacent(region: ReferenceRegion): Boolean =
    distance(region).exists(_ == 1)

  /**
   * Returns the distance between this reference region and another region in the reference space.
   *
   * @note Distance here is defined as the minimum distance between any point within this region, and
   * any point within the other region we are measuring against. If the two sets overlap, the distance
   * will be 0. If the sets abut, the distance will be 1. Else, the distance will be greater.
   *
   * @param other Region to compare against.
   * @return Returns an option containing the distance between two points. If the point is not in
   * our reference space, we return an empty option (None).
   */
  def distance(other: ReferenceRegion): Option[Long] =
    if (referenceName == other.referenceName && orientation == other.orientation)
      if (overlaps(other))
        Some(0)
      else if (other.start >= end)
        Some(other.start - end + 1)
      else
        Some(start - other.end + 1)
    else
      None

  /**
   * @param by The number of bases to extend the region by from both the start
   *   and the end.
   * @return Returns a new reference region where the start and end have been
   *   moved.
   */
  def pad(by: Long): ReferenceRegion = {
    pad(by, by)
  }

  /**
   * @param byStart The number of bases to move the start position forward by.
   * @param byEnd The number of bases to move the end position back by.
   * @return Returns a new reference region where the start and/or end have been
   *   moved.
   */
  def pad(byStart: Long, byEnd: Long): ReferenceRegion = {
    new ReferenceRegion(referenceName,
      start - byStart,
      end + byEnd,
      orientation)
  }

  def contains(other: ReferencePosition): Boolean = {
    orientation == other.orientation &&
      referenceName == other.referenceName &&
      start <= other.pos && end > other.pos
  }

  def contains(other: ReferenceRegion): Boolean = {
    orientation == other.orientation &&
      referenceName == other.referenceName &&
      start <= other.start && end >= other.end
  }

  def overlaps(other: ReferenceRegion): Boolean = {
    orientation == other.orientation &&
      referenceName == other.referenceName &&
      end > other.start && start < other.end
  }

  def compareTo(that: ReferenceRegion): Int = {
    RegionOrdering.compare(this, that)
  }

  def length(): Long = {
    end - start
  }

  override def hashCode: Int = {
    var result = 37
    result = 41 * result + (if (referenceName != null) referenceName.hashCode else 0)
    result = 41 * result + start.hashCode
    result = 41 * result + end.hashCode
    result = 41 * result + (if (orientation != null) orientation.ordinal() else 0)
    result
  }
}

class ReferenceRegionSerializer extends Serializer[ReferenceRegion] {
  private val enumValues = Strand.values()

  def write(kryo: Kryo, output: Output, obj: ReferenceRegion) = {
    output.writeString(obj.referenceName)
    output.writeLong(obj.start)
    output.writeLong(obj.end)
    output.writeInt(obj.orientation.ordinal())
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferenceRegion]): ReferenceRegion = {
    val referenceName = input.readString()
    val start = input.readLong()
    val end = input.readLong()
    val orientation = input.readInt()
    new ReferenceRegion(referenceName, start, end, enumValues(orientation))
  }
}
