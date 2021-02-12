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

import java.lang.{ Long => JLong }

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.apache.parquet.filter2.predicate.{ FilterApi, FilterPredicate }
import org.apache.parquet.filter2.predicate.Operators.{ BinaryColumn, LongColumn }
import org.apache.parquet.io.api.Binary
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.interval.array.Interval
import scala.math.{ max, min }

trait ReferenceOrdering[T <: ReferenceRegion] extends Ordering[T] {
  private def regionCompare(a: T, b: T): Int = {
    if (a.referenceName != b.referenceName) {
      a.referenceName.compareTo(b.referenceName)
    } else if (a.start != b.start) {
      a.start.compareTo(b.start)
    } else {
      a.end.compareTo(b.end)
    }
  }

  def compare(a: T, b: T): Int = {
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
 * A sort order that orders all given regions lexicographically by reference and
 * numerically within a single reference sequence, and puts all non-provided regions at
 * the end. Regions are compared by start position first. If start positions
 * are equal, then we compare by end position.
 */
object RegionOrdering extends ReferenceOrdering[ReferenceRegion] {
}

/**
 * A sort order that orders all given regions lexicographically by reference and
 * numerically within a single reference sequence, and puts all non-provided regions at
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
   * Parses a set of comma delimited loci from a string.
   *
   * Acceptable strings include:
   * - ctg:start-end
   * - ctg:pos
   *
   * @param loci The string describing the loci to create reference regions for.
   * @return Returns an iterable collection of reference regions.
   */
  def fromString(loci: String): Iterable[ReferenceRegion] = {
    loci
      .split(",")
      .map(_.trim)
      .map(token => {
        require(token.nonEmpty, "reference region must not be empty")
        val colonIdx = token.lastIndexOf(":")
        if (colonIdx == -1) {
          all(token)
        } else {
          val referenceName = token.substring(0, colonIdx)
          val dashIdx = token.lastIndexOf("-")
          if (dashIdx == -1) {
            if (token.endsWith("+")) {
              toEnd(referenceName, token.substring(colonIdx + 1, token.length() - 1).toLong)
            } else {
              val position = token.substring(colonIdx + 1).toLong
              apply(referenceName, position, position + 1L)
            }
          } else {
            val start = token.substring(colonIdx + 1, dashIdx).toLong
            val end = token.substring(dashIdx + 1).toLong
            apply(referenceName, start, end)
          }
        }
      })
  }

  /**
   * Creates a reference region that starts at the beginning of a reference sequence.
   *
   * @param referenceName The name of the reference sequence that this region is
   *   on.
   * @param end The end position for this region.
   * @param strand The strand of the genome that this region exists on.
   * @return Returns a reference region that goes from the start of a reference sequence to
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
   * @param referenceName The name of the reference sequence that this region is
   *   on.
   * @param start The start position for this region.
   * @param strand The strand of the genome that this region exists on.
   * @return Returns a reference region that goes from a user provided starting
   *   point to the end of a reference sequence.
   */
  def toEnd(referenceName: String,
            start: Long,
            strand: Strand = Strand.INDEPENDENT): ReferenceRegion = {
    ReferenceRegion(referenceName, start, Long.MaxValue, strand = strand)
  }

  /**
   * Creates a reference region that covers the entirety of a reference sequence.
   *
   * @param referenceName The name of the reference sequence to cover.
   * @param strand The strand of the genome that this region exists on.
   * @return Returns a reference region that covers the entirety of a reference sequence.
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
  def opt(record: Alignment): Option[ReferenceRegion] = {
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
    if (genotype.getStart != -1) {
      ReferenceRegion(genotype.getReferenceName,
        genotype.getStart,
        genotype.getEnd)
    } else {
      ReferenceRegion(genotype.getReferenceName,
        0L,
        1L)
    }
  }

  /**
   * Builds a reference region for a variant site.
   *
   * @param variant Variant to extract region from.
   * @return The site where this variant covers.
   */
  def apply(variant: Variant): ReferenceRegion = {
    if (variant.getStart != -1L) {
      ReferenceRegion(variant.getReferenceName, variant.getStart, variant.getEnd)
    } else {
      ReferenceRegion(variant.getReferenceName, 0L, 1L)
    }
  }

  /**
   * Builds a referenceRegion from genomic coordinates.
   *
   * @param referenceName reference name
   * @param start start position
   * @param end end position
   * @return Reference Region for these genomic coordinates
   */
  def fromGenomicRange(referenceName: String, start: Long, end: Long): ReferenceRegion = {
    ReferenceRegion(referenceName, start, end)
  }

  private def checkRead(record: Alignment) {
    require(record.getReadMapped,
      "Cannot build reference region for unmapped read %s.".format(record))
    require(record.getReferenceName != null &&
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
  def unstranded(record: Alignment): ReferenceRegion = {
    checkRead(record)
    ReferenceRegion(record.getReferenceName, record.getStart, record.getEnd)
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
  def stranded(record: Alignment): ReferenceRegion = {
    checkRead(record)

    val strand = Option(record.getReadNegativeStrand)
      .map(b => b: Boolean) match {
        case None => throw new IllegalArgumentException(
          "Alignment strand not set for %s".format(record))
        case Some(true)  => Strand.REVERSE
        case Some(false) => Strand.FORWARD
      }
    new ReferenceRegion(record.getReferenceName,
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
   * Generates a reference region from a sequence. Returns None if the sequence does not
   * have a name or a length.
   *
   * @param sequence Sequence from which to generate data.
   * @return Region corresponding to inclusive region of the specified sequence.
   */
  def apply(sequence: Sequence): Option[ReferenceRegion] = {
    if (sequence.getName != null &&
      sequence.getLength != null) {
      Some(ReferenceRegion(sequence.getName,
        0L,
        sequence.getLength))
    } else {
      None
    }
  }

  /**
   * Generates a reference region from a slice. Returns None if the slice does not
   * have a name, a start position, or an end position.
   *
   * @param slice Slice from which to generate data.
   * @return Region corresponding to inclusive region of the specified slice.
   */
  def apply(slice: Slice): Option[ReferenceRegion] = {
    if (slice.getName != null &&
      slice.getStart != null &&
      slice.getEnd != null) {
      Some(ReferenceRegion(slice.getName,
        slice.getStart,
        slice.getEnd))
    } else {
      None
    }
  }

  private def checkFeature(record: Feature) {
    require(record.getReferenceName != null &&
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
    new ReferenceRegion(feature.getReferenceName, feature.getStart, feature.getEnd)
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
    new ReferenceRegion(feature.getReferenceName,
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
    new ReferenceRegion(coverage.referenceName, coverage.start, coverage.end)
  }

  /**
   * Creates a predicate that filters records overlapping one or more regions.
   *
   * @param regions The regions to filter on.
   * @return Returns a predicate that can be pushed into Parquet files that
   *   keeps all records that overlap one or more region.
   */
  def createPredicate(regions: ReferenceRegion*): FilterPredicate = {
    require(regions.nonEmpty,
      "Cannot create a predicate from an empty set of regions.")
    regions.toIterable
      .map(_.toPredicate)
      .reduce(FilterApi.or)
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

  require(start >= 0 && end >= start && referenceName != null && strand != null,
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
   * @throws IllegalArgumentException Thrown if regions are not overlapping or adjacent.
   *
   * @param other Other region to merge with this region.
   * @return The merger of both unions.
   *
   * @see hull
   */
  def merge(other: ReferenceRegion): ReferenceRegion = {
    require(overlaps(other) || isAdjacent(other), "Cannot merge two regions that do not overlap or are not adjacent")
    merge(other, 1L)
  }

  /**
   * Merges two reference regions that are within a threshold of each other.
   *
   * @throws IllegalArgumentException Thrown if regions are not within the
   *         distance threshold.
   *
   * @param other Other region to merge with this region.
   * @return The merger of both unions.
   *
   * @see hull
   */
  def merge(other: ReferenceRegion, distanceThreshold: Long): ReferenceRegion = {
    require(isNearby(other, distanceThreshold), "Cannot merge two regions that do not meet the distance threshold")
    require(distanceThreshold >= 0, "Distance must be non-negative number")
    hull(other)
  }

  /**
   * Calculates the intersection of two reference regions given a minimum
   * overlap.
   *
   * @param other Region to intersect with.
   * @param minOverlap Minimum overlap between the two reference regions.
   * @return A smaller reference region
   */
  def intersection(other: ReferenceRegion, minOverlap: Long = 0L): ReferenceRegion = {
    require(overlapsBy(other).exists(_ >= minOverlap), "Other region does not meet minimum overlap provided")
    ReferenceRegion(referenceName, max(start, other.start), min(end, other.end), strand)
  }

  /**
   * Creates a region corresponding to the convex hull of two regions. Has no preconditions about the adjacency or
   * overlap of two regions. However, regions must be in the same reference space.
   *
   * @throws IllegalArgumentException Thrown if regions are in different reference spaces.
   *
   * @param other Other region to compute hull of with this region.
   * @return The convex hull of both unions.
   *
   * @see merge
   */
  def hull(other: ReferenceRegion): ReferenceRegion = {
    require(sameStrand(other), "Cannot compute convex hull of differently oriented regions.")
    require(sameReferenceName(other), "Cannot compute convex hull of regions on different references.")
    ReferenceRegion(referenceName, min(start, other.start), max(end, other.end), strand)
  }

  /**
   * Returns whether two regions are adjacent.
   *
   * Adjacent regions do not overlap, but have no separation between start/end.
   *
   * @param other Region to compare against.
   * @return True if regions are adjacent.
   */
  def isAdjacent(other: ReferenceRegion): Boolean = {
    distance(other).contains(1)
  }

  /**
   * Returns whether two regions are nearby.
   *
   * Two regions are near each other if the distance between the two is less
   * than the user provided distanceThreshold.
   *
   * @param other Region to compare against.
   * @param distanceThreshold The maximum distance of interest.
   * @param requireStranded Strandedness is or is not required, true by default.
   * @return True if regions are nearby.
   */
  def isNearby(other: ReferenceRegion,
               distanceThreshold: Long,
               requireStranded: Boolean = true): Boolean = {
    distance(other).exists(_ <= distanceThreshold) ||
      (!requireStranded && unstrandedDistance(other).exists(_ <= distanceThreshold))
  }

  /**
   * Returns the distance between this reference region and another region in
   * the reference space.
   *
   * @note Distance here is defined as the minimum distance between any point
   *       within this region, and any point within the other region we are measuring
   *       against. If the two sets overlap, the distance will be 0. If the sets abut,
   *       the distance will be 1. Else, the distance will be greater.
   *
   * @param other Region to compare against.
   * @return Returns an option containing the distance between two points. If
   *   the point is not in our reference space, we return an empty option.
   */
  def distance(other: ReferenceRegion): Option[Long] = {
    if (sameReferenceName(other) && sameStrand(other)) {
      if (overlaps(other)) {
        Some(0)
      } else {
        Some(max(start, other.start) - min(end, other.end) + 1)
      }
    } else {
      None
    }
  }

  /**
   * Returns the distance to another region, ignoring strand.
   *
   * @note Distance here is defined as the minimum distance between any point
   *       within this region, and any point within the other region we are measuring
   *       against. If the two sets overlap, the distance will be 0. If the sets abut,
   *       the distance will be 1. Else, the distance will be greater.
   *
   * @param other Region to compare against.
   * @return Returns an option containing the distance between two points. If
   *   the point is not in our reference space, we return an empty option.
   */
  def unstrandedDistance(other: ReferenceRegion): Option[Long] = {
    if (sameReferenceName(other)) {
      if (covers(other)) {
        Some(0)
      } else {
        Some(max(start, other.start) - min(end, other.end) + 1)
      }
    } else {
      None
    }
  }

  /**
   * Returns the number of bases overlapping another region.
   *
   * @param other Region to compare against.
   * @return Returns an option containing the number of positions of overlap
   *         between two points. If the two regions do not overlap, we return
   *         an empty option.
   */
  def overlapsBy(other: ReferenceRegion): Option[Long] = {
    if (overlaps(other)) {
      Some(min(end, other.end) - max(start, other.start))
    } else {
      None
    }
  }

  /**
   * Returns the number of bases covering another region.
   *
   * A region covers another region if it is overlapping, regardless of strand.
   *
   * @param other Region to compare against.
   * @return Returns an option containing the number of positions of coverage
   *         between two points. If the two regions do not cover each other,
   *         we return an empty option.
   */
  def coversBy(other: ReferenceRegion): Option[Long] = {
    if (covers(other)) {
      Some(min(end, other.end) - max(start, other.start))
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
    new ReferenceRegion(referenceName, max(0, start - byStart), end + byEnd, strand)
  }

  /**
   * Checks if another region is wholly within our region.
   *
   * @param other The region to compare against.
   * @return True if the region is wholly contained within our region.
   */
  def contains(other: ReferenceRegion): Boolean = {
    sameStrand(other) &&
      sameReferenceName(other) &&
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
    sameReferenceName(other) &&
      end > other.start && start < other.end
  }

  /**
   * Checks if our region overlaps or is within a threshold of another region,
   * independent of strand.
   *
   * @param other The region to compare against.
   * @param threshold The threshold within which the region must match.
   * @return True if any section of the two regions overlap.
   */
  def covers(other: ReferenceRegion, threshold: Long): Boolean = {
    isNearby(other, threshold, false)
  }

  /**
   * Checks if our region overlaps or is within a threshold of another region.
   *
   * @param other The region to compare against.
   * @return True if any section of the two regions overlap.
   */
  def overlaps(other: ReferenceRegion): Boolean = {
    sameStrand(other) &&
      covers(other)
  }

  /**
   * Checks if our region overlaps (wholly or partially) another region.
   *
   * @param other The region to compare against.
   * @param threshold The threshold within which the region must match.
   * @return True if any section of the two regions overlap.
   */
  def overlaps(other: ReferenceRegion, threshold: Long): Boolean = {
    isNearby(other, threshold)
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
   * Determines if two regions are on the same strand.
   *
   * @param other The other region.
   * @return True if the two are on the same strand, false otherwise
   */
  @inline final def sameStrand(other: ReferenceRegion): Boolean = {
    strand == other.strand
  }

  /**
   * Determines if two regions are on the same reference sequence.
   *
   * @param other The other region.
   * @return True if the two are on the same reference sequence, false otherwise.
   */
  @inline final def sameReferenceName(other: ReferenceRegion): Boolean = {
    referenceName == other.referenceName
  }

  /**
   * @return The length of this region in bases.
   */
  def length(): Long = {
    end - start
  }

  /**
   * Subtracts another region.
   *
   * Subtracting in this case removes the entire region and returns up to two
   * new regions.
   * @param other The region to subtract.
   * @param requireStranded Whether or not to require other be on same strand.
   * @return A list containing the regions resulting from the subtraction.
   */
  def subtract(other: ReferenceRegion, requireStranded: Boolean = false): Iterable[ReferenceRegion] = {
    val newRegionStrand =
      if (requireStranded) {
        require(overlaps(other), "Region $other is not overlapped by $this.")
        strand
      } else {
        require(covers(other), s"Region $other is not covered by $this.")
        Strand.INDEPENDENT
      }
    val first = if (other.start > start) {
      Iterable(ReferenceRegion(referenceName,
        start,
        other.start,
        newRegionStrand))
    } else {
      Iterable.empty[ReferenceRegion]
    }
    val second = if (end > other.end) {
      Iterable(ReferenceRegion(referenceName,
        other.end,
        end,
        newRegionStrand))
    } else {
      Iterable.empty[ReferenceRegion]
    }

    first ++ second
  }

  /**
   * Generates a predicate that can be used with Parquet files.
   *
   * @return A predicate that selects records that overlap a given genomic
   *   region.
   */
  def toPredicate: FilterPredicate = {
    FilterApi.and(
      FilterApi.and(
        FilterApi.gt[JLong, LongColumn](FilterApi.longColumn("end"), start),
        FilterApi.ltEq[JLong, LongColumn](FilterApi.longColumn("start"), end)),
      FilterApi.eq[Binary, BinaryColumn](
        FilterApi.binaryColumn("referenceName"), Binary.fromString(referenceName)))
  }

  override def hashCode: Int = {
    val nameHashCode = 37 + referenceName.hashCode
    val strandHashCode = strand.ordinal()

    ((nameHashCode * 41 + start.hashCode) * 41 + end.hashCode) * 41 + strandHashCode
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
