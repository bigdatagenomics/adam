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
import org.bdgenomics.utils.intervalarray.Interval
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
      a.strand.ordinal compare b.strand.ordinal
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

/**
 * A sort order that orders all given regions lexicographically by contig and
 * numerically within a single contig, and puts all non-provided regions at
 * the end. Regions are compared by start position first. If start positions
 * are equal, then we compare by end position.
 */
object RegionOrdering extends ReferenceOrdering[ReferenceRegion] {
}

/**
 * A sort order that orders all given regions lexicographically by contig and
 * numerically within a single contig, and puts all non-provided regions at
 * the end. An extension of PositionOrdering to Optional data.
 *
 * @see PositionOrdering
 */
object OptionalRegionOrdering extends OptionalReferenceOrdering[ReferenceRegion] {
  val baseOrdering = RegionOrdering
}

/**
 * A companion object for creating and ordering ReferenceRegions.
 */
object ReferenceRegion {

  implicit def orderingForPositions = RegionOrdering
  implicit def orderingForOptionalPositions = OptionalRegionOrdering

  /**
   * Creates a reference region that starts at the beginning of a contig.
   *
   * @param referenceName The name of the reference contig that this region is
   *   on.
   * @param end The end position for this region.
   * @param strand The strand of the genome that this region exists on.
   * @return Returns a reference region that goes from the start of a contig to
   *   a user provided end point.
   */
  def fromStart(referenceName: String,
                end: Long,
                strand: Strand = Strand.INDEPENDENT): ReferenceRegion = {
    ReferenceRegion(referenceName, 0L, end, strand = strand)
  }

  /**
   * Creates a reference region that has an open end point.
   *
   * @param referenceName The name of the reference contig that this region is
   *   on.
   * @param start The start position for this region.
   * @param strand The strand of the genome that this region exists on.
   * @return Returns a reference region that goes from a user provided starting
   *   point to the end of a contig.
   */
  def toEnd(referenceName: String,
            start: Long,
            strand: Strand = Strand.INDEPENDENT): ReferenceRegion = {
    ReferenceRegion(referenceName, start, Long.MaxValue, strand = strand)
  }

  /**
   * Creates a reference region that covers the entirety of a contig.
   *
   * @param referenceName The name of the reference contig to cover.
   * @param strand The strand of the genome that this region exists on.
   * @return Returns a reference region that covers the entirety of a contig.
   */
  def all(referenceName: String,
          strand: Strand = Strand.INDEPENDENT): ReferenceRegion = {
    ReferenceRegion(referenceName, 0L, Long.MaxValue, strand = strand)
  }

  /**
   * Generates a reference region from read data. Returns None if the read is not mapped;
   * else, returns the inclusive region from the start to the end of the read alignment.
   *
   * @param record Read to create region from.
   * @return Region corresponding to inclusive region of read alignment, if read is mapped.
   */
  def opt(record: AlignmentRecord): Option[ReferenceRegion] = {
    if (record.getReadMapped) {
      Some(unstranded(record))
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
    ReferenceRegion(genotype.getContigName,
      genotype.getStart,
      genotype.getEnd)
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

  private def checkRead(record: AlignmentRecord) {
    require(record.getReadMapped,
      "Cannot build reference region for unmapped read %s.".format(record))
    require(record.getContigName != null &&
      record.getStart != null &&
      record.getEnd != null,
      "Read %s contains required fields that are null.".format(record))
  }

  /**
   * Builds a reference region with independent strand for an aligned read.
   *
   * @throws IllegalArgumentException If this read is not aligned or alignment
   *   data is null.
   *
   * @param record The read to extract the reference region from.
   * @return Returns the reference region covered by this read's alignment.
   *
   * @see stranded
   */
  def unstranded(record: AlignmentRecord): ReferenceRegion = {
    checkRead(record)
    ReferenceRegion(record.getContigName, record.getStart, record.getEnd)
  }

  /**
   * Builds a reference region for an aligned read with strand set.
   *
   * @throws IllegalArgumentException If this read is not aligned, alignment
   *   data is null, or strand is not set.
   *
   * @param record The read to extract the reference region from.
   * @return Returns the reference region covered by this read's alignment.
   *
   * @see unstranded
   */
  def stranded(record: AlignmentRecord): ReferenceRegion = {
    checkRead(record)

    val strand = Option(record.getReadNegativeStrand)
      .map(b => b: Boolean) match {
        case None => throw new IllegalArgumentException(
          "Alignment strand not set for %s".format(record))
        case Some(true)  => Strand.REVERSE
        case Some(false) => Strand.FORWARD
      }
    new ReferenceRegion(record.getContigName,
      record.getStart,
      record.getEnd,
      strand = strand)
  }

  /**
   * Generates a region from a given position -- the region will have a length of 1.
   * @param pos The position to convert
   * @return A 1-wide region at the same location as pos
   */
  def apply(pos: ReferencePosition): ReferenceRegion = {
    ReferenceRegion(pos.referenceName,
      pos.pos,
      pos.pos + 1,
      strand = pos.strand)
  }

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

  private def checkFeature(record: Feature) {
    require(record.getContigName != null &&
      record.getStart != null &&
      record.getEnd != null,
      "Feature %s contains required fields that are null.".format(record))
  }

  /**
   * Builds a reference region for a feature without strand information.
   *
   * @param feature Feature to extract ReferenceRegion from.
   * @return Extracted ReferenceRegion
   *
   * @see stranded
   */
  def unstranded(feature: Feature): ReferenceRegion = {
    checkFeature(feature)
    new ReferenceRegion(feature.getContigName, feature.getStart, feature.getEnd)
  }

  /**
   * Builds a reference region for a feature with strand set.
   *
   * @param feature Feature to extract ReferenceRegion from.
   * @return Extracted ReferenceRegion
   *
   * @throws IllegalArgumentException Throws an exception if the strand is null
   *   in the provided feature.
   *
   * @see unstranded
   */
  def stranded(feature: Feature): ReferenceRegion = {
    checkFeature(feature)
    require(feature.getStrand != null,
      "Strand is not defined in feature %s.".format(feature))
    new ReferenceRegion(feature.getContigName,
      feature.getStart,
      feature.getEnd,
      strand = feature.getStrand)
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
 * @param strand The strand of the genome that this region exists on.
 */
case class ReferenceRegion(
  referenceName: String,
  start: Long,
  end: Long,
  strand: Strand = Strand.INDEPENDENT)
    extends Comparable[ReferenceRegion]
    with Interval[ReferenceRegion] {

  assert(start >= 0 && end >= start,
    "Failed when trying to create region %s %d %d on %s strand.".format(
      referenceName, start, end, strand))

  /**
   * @return Returns a copy of this reference region that is on the independent
   *   strand.
   */
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
    assert(strand == region.strand, "Cannot compute convex hull of differently oriented regions.")
    assert(referenceName == region.referenceName, "Cannot compute convex hull of regions on different references.")
    ReferenceRegion(referenceName, min(start, region.start), max(end, region.end))
  }

  /**
   * Returns whether two regions are adjacent.
   *
   * Adjacent regions do not overlap, but have no separation between start/end.
   *
   * @param region Region to compare against.
   * @return True if regions are adjacent.
   */
  def isAdjacent(region: ReferenceRegion): Boolean =
    distance(region).exists(_ == 1)

  /**
   * Returns the distance between this reference region and another region in
   * the reference space.
   *
   * @note Distance here is defined as the minimum distance between any point
   * within this region, and any point within the other region we are measuring
   * against. If the two sets overlap, the distance will be 0. If the sets abut,
   * the distance will be 1. Else, the distance will be greater.
   *
   * @param other Region to compare against.
   * @return Returns an option containing the distance between two points. If
   *   the point is not in our reference space, we return an empty option.
   */
  def distance(other: ReferenceRegion): Option[Long] = {
    if (referenceName == other.referenceName && strand == other.strand) {
      if (overlaps(other)) {
        Some(0)
      } else if (other.start >= end) {
        Some(other.start - end + 1)
      } else {
        Some(start - other.end + 1)
      }
    } else {
      None
    }
  }

  /**
   * Extends the current reference region at both the start and end.
   *
   * @param by The number of bases to extend the region by from both the start
   *   and the end.
   * @return Returns a new reference region where the start and end have been
   *   moved.
   */
  def pad(by: Long): ReferenceRegion = {
    pad(by, by)
  }

  /**
   * Extends the current reference region at both the start and end, but by
   * different numbers of bases.
   *
   * @param byStart The number of bases to move the start position forward by.
   * @param byEnd The number of bases to move the end position back by.
   * @return Returns a new reference region where the start and/or end have been
   *   moved.
   */
  def pad(byStart: Long, byEnd: Long): ReferenceRegion = {
    new ReferenceRegion(referenceName,
      start - byStart,
      end + byEnd,
      strand)
  }

  /**
   * Checks if a position is wholly within our region.
   *
   * @param other The reference position to compare against.
   * @return True if the position is within our region.
   */
  def contains(other: ReferencePosition): Boolean = {
    strand == other.strand &&
      referenceName == other.referenceName &&
      start <= other.pos && end > other.pos
  }

  /**
   * Checks if another region is wholly within our region.
   *
   * @param other The region to compare against.
   * @return True if the region is wholly contained within our region.
   */
  def contains(other: ReferenceRegion): Boolean = {
    strand == other.strand &&
      referenceName == other.referenceName &&
      start <= other.start && end >= other.end
  }

  /**
   * Checks if our region overlaps (wholly or partially) another region,
   * independent of strand.
   *
   * @param other The region to compare against.
   * @return True if any section of the two regions overlap.
   */
  def covers(other: ReferenceRegion): Boolean = {
    referenceName == other.referenceName &&
      end > other.start && start < other.end
  }

  /**
   * Checks if our region overlaps (wholly or partially) another region.
   *
   * @param other The region to compare against.
   * @return True if any section of the two regions overlap.
   */
  def overlaps(other: ReferenceRegion): Boolean = {
    strand == other.strand &&
      referenceName == other.referenceName &&
      end > other.start && start < other.end
  }

  /**
   * Compares between two regions using the RegionOrdering.
   *
   * @param that The region to compare against.
   * @return An ordering depending on which region comes first.
   */
  def compareTo(that: ReferenceRegion): Int = {
    RegionOrdering.compare(this, that)
  }

  /**
   * @return The length of this region in bases.
   */
  def length(): Long = {
    end - start
  }

  override def hashCode: Int = {
    var result = 37
    result = 41 * result + (if (referenceName != null) referenceName.hashCode else 0)
    result = 41 * result + start.hashCode
    result = 41 * result + end.hashCode
    result = 41 * result + (if (strand != null) strand.ordinal() else 0)
    result
  }
}

class ReferenceRegionSerializer extends Serializer[ReferenceRegion] {
  private val enumValues = Strand.values()

  def write(kryo: Kryo, output: Output, obj: ReferenceRegion) = {
    output.writeString(obj.referenceName)
    output.writeLong(obj.start)
    output.writeLong(obj.end)
    output.writeInt(obj.strand.ordinal())
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferenceRegion]): ReferenceRegion = {
    val referenceName = input.readString()
    val start = input.readLong()
    val end = input.readLong()
    val strand = input.readInt()
    new ReferenceRegion(referenceName, start, end, enumValues(strand))
  }
}
