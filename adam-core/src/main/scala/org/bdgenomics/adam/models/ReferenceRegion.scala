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
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ Feature, AlignmentRecord, NucleotideContigFragment }
import scala.math.{ max, min }

object ReferenceRegionWithOrientation {

  /**
   * Builds an oriented reference region from the individual parameters
   *
   * @param referenceName The name of the sequence (chromosome) in the reference genome
   * @param start The 0-based residue-coordinate for the start of the region
   * @param end The 0-based residue-coordinate for the first residue <i>after</i> the start
   *            which is <i>not</i> in the region -- i.e. [start, end) define a 0-based
   *            half-open interval.
   * @param negativeStrand Boolean flag as to whether the region is on the forward or
   *                       reverse strand of the reference region.
   */
  def apply(referenceName: String,
            start: Long,
            end: Long,
            negativeStrand: Boolean): ReferenceRegionWithOrientation = {
    ReferenceRegionWithOrientation(ReferenceRegion(referenceName, start, end), negativeStrand)
  }
}

/**
 * Represents a contiguous region of the reference genome with strand information.
 *
 * @param region The genomic locus as a ReferenceRegion
 * @param negativeStrand Boolean flag as to whether the region is on the forward or
 *                       reverse strand of the reference region.
 */
case class ReferenceRegionWithOrientation(region: ReferenceRegion,
                                          negativeStrand: Boolean) {
  def width: Long = region.width

  def contains(other: ReferencePositionWithOrientation): Boolean = {
    negativeStrand == other.negativeStrand && region.contains(other.refPos)
  }

  def contains(other: ReferenceRegionWithOrientation): Boolean = {
    region.contains(other.region) && negativeStrand == other.negativeStrand
  }

  def overlaps(other: ReferenceRegionWithOrientation): Boolean = {
    region.overlaps(other.region) && negativeStrand == other.negativeStrand
  }

  /**
   * Creates a new ReferenceRegionWithOrientation, with either the start or
   * end field modified so that the overall region has a width equal to the
   * given target length.
   *
   * If the region is on the negative strand, then the start value is modified,
   * otherwise, if the region is on the positive strand, then the end value is
   * incremented.
   *
   * @param targetLength the desired width of the new region
   * @return The new region whose width==targetLength
   */
  def extend(targetLength: Long): ReferenceRegionWithOrientation = {
    require(targetLength >= 0)
    require(targetLength > width)

    val diff = targetLength - width

    if (negativeStrand)
      ReferenceRegionWithOrientation(
        ReferenceRegion(region.referenceName, region.start - diff, region.end), negativeStrand)
    else
      ReferenceRegionWithOrientation(
        ReferenceRegion(region.referenceName, region.start, region.end + diff), negativeStrand)
  }

  def toReferenceRegion: ReferenceRegion = region

  def referenceName: String = region.referenceName

  def start: Long = region.start

  def end: Long = region.end
}

object ReferenceRegionContext {
  implicit val referenceRegionOrdering = ReferenceRegionOrdering
  implicit val referenceRegionWithOrientationOrdering = ReferenceRegionWithOrientationOrdering
}

object ReferenceRegionWithOrientationOrdering extends Ordering[ReferenceRegionWithOrientation] {

  def compare(this_ : ReferenceRegionWithOrientation, that: ReferenceRegionWithOrientation): Int = {
    val regionCompare = ReferenceRegionOrdering.compare(this_.region, that.region)
    if (regionCompare != 0) {
      regionCompare
    } else {
      this_.negativeStrand.compare(that.negativeStrand)
    }
  }
}

object ReferenceRegion {

  /**
   * Generates a reference region from read data. Returns None if the read is not mapped;
   * else, returns the inclusive region from the start to the end of the read alignment.
   *
   * @param record Read to create region from.
   * @return Region corresponding to inclusive region of read alignment, if read is mapped.
   */
  def apply(record: AlignmentRecord): Option[ReferenceRegion] = {
    if (record.getReadMapped) {
      Some(ReferenceRegion(record.getContig.getContigName.toString, record.getStart, record.getEnd))
    } else {
      None
    }
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
    val contig = fragment.getContig
    if (contig != null && contig.getContigName != null &&
      fragment.getFragmentStartPosition != null) {
      val fragmentSequence = fragment.getFragmentSequence
      Some(ReferenceRegion(contig.getContigName,
        fragment.getFragmentStartPosition,
        fragment.getFragmentStartPosition + fragmentSequence.length))
    } else {
      None
    }
  }

  def apply(feature: Feature): ReferenceRegion = {
    new ReferenceRegion(feature.getContig.getContigName.toString, feature.getStart, feature.getEnd)
  }
}

object ReferenceRegionOrdering extends Ordering[ReferenceRegion] {

  override def compare(this_ : ReferenceRegion, that: ReferenceRegion): Int =
    if (this_.referenceName != that.referenceName)
      this_.referenceName.compareTo(that.referenceName)
    else if (this_.start != that.start)
      this_.start.compareTo(that.start)
    else
      this_.end.compareTo(that.end)
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
case class ReferenceRegion(referenceName: String, start: Long, end: Long) extends Interval {

  assert(start >= 0, "ReferenceRegion doesn't allow a negative start coordinate (here provided %d)".format(start))
  assert(end >= start, "ReferenceRegion requires end >= start (here provided %d,%d)".format(start, end))

  def width: Long = end - start

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
    distance(region).map(_ == 1).getOrElse(false)

  /**
   * Returns the distance between this reference region and a point in the reference space.
   *
   * @note Distance here is defined as the minimum distance between any point within this region, and
   * the point we are measuring against. If the point is within this region, its distance will be 0.
   * Else, the distance will be greater than or equal to 1.
   *
   * @param other Point to compare against.
   * @return Returns an option containing the distance between two points. If the point is not in
   * our reference space, we return an empty option (None).
   */
  def distance(other: ReferencePosition): Option[Long] =
    if (referenceName == other.referenceName)
      if (other.pos < start)
        Some(start - other.pos)
      else if (other.pos >= end)
        Some(other.pos - end + 1)
      else
        Some(0)
    else
      None

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
    if (referenceName == other.referenceName)
      if (overlaps(other))
        Some(0)
      else if (other.start >= end)
        Some(other.start - end + 1)
      else
        Some(start - other.end + 1)
    else
      None

  def contains(other: ReferencePosition): Boolean =
    referenceName == other.referenceName && start <= other.pos && end > other.pos

  def contains(other: ReferenceRegion): Boolean =
    referenceName == other.referenceName && start <= other.start && end >= other.end

  def overlaps(other: ReferenceRegion): Boolean =
    referenceName == other.referenceName && end > other.start && start < other.end

  def length(): Long = {
    end - start
  }
}

class ReferenceRegionSerializer extends Serializer[ReferenceRegion] {
  def write(kryo: Kryo, output: Output, obj: ReferenceRegion) = {
    output.writeString(obj.referenceName)
    output.writeLong(obj.start)
    output.writeLong(obj.end)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferenceRegion]): ReferenceRegion = {
    val referenceName = input.readString()
    val start = input.readLong()
    val end = input.readLong()
    new ReferenceRegion(referenceName, start, end)
  }
}
