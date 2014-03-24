/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.models

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import edu.berkeley.cs.amplab.adam.avro.{ADAMRecord, ADAMNucleotideContigFragment, Base}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import scala.math.{min, max}

object ReferenceRegion {
  
  /**
   * Generates a reference region from read data. Returns None if the read is not mapped;
   * else, returns the inclusive region from the start to the end of the read alignment.
   *
   * @param record Read to create region from.
   * @return Region corresponding to inclusive region of read alignment, if read is mapped.
   */
  def apply (record: ADAMRecord): Option[ReferenceRegion] = {
    if (record.getReadMapped) {
      Some(ReferenceRegion(record.getReferenceId, record.getStart, RichADAMRecord(record).end.get + 1))
    } else {
      None
    }
  }

  /**
   * Generates a region from a given position -- the region will have a length of 1.
   * @param pos The position to convert
   * @return A 1-wide region at the same location as pos
   */
  def apply(pos : ReferencePosition) : ReferenceRegion =
    ReferenceRegion(pos.refId, pos.pos, pos.pos+1)

  /**
   * Generates a reference region from assembly data. Returns None if the assembly does not
   * have an ID or a start position.
   *
   * @param fragment Assembly fragment from which to generate data.
   * @return Region corresponding to inclusive region of contig fragment.
   */
  def apply (fragment: ADAMNucleotideContigFragment): Option[ReferenceRegion] = {
    if (fragment.getContigId != null && fragment.getFragmentStartPosition != null) {
      val fragmentSequence = fragment.getFragmentSequence
      Some(ReferenceRegion(fragment.getContigId, 
                           fragment.getFragmentStartPosition,
                           fragment.getFragmentStartPosition + fragmentSequence.length))
    } else {
      None
    }
  }  
}

/**
 * Represents a contiguous region of the reference genome.
 *
 * @param refId The index of the sequence (chromosome) in the reference genome
 * @param start The 0-based residue-coordinate for the start of the region
 * @param end The 0-based residue-coordinate for the first residue <i>after</i> the start
 *            which is <i>not</i> in the region -- i.e. [start, end) define a 0-based
 *            half-open interval.
 */
case class ReferenceRegion(refId: Int, start: Long, end: Long) extends Ordered[ReferenceRegion] {

  assert(start >= 0)
  assert(end >= start)

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
    assert(refId == region.refId, "Cannot compute convex hull of regions on different references.")
    ReferenceRegion(refId, min(start, region.start), max(end, region.end))
  }

  /**
   * Returns whether two regions are adjacent. Adjacent regions do not overlap, but have no separation between start/end.
   *
   * @param region Region to compare against.
   * @return True if regions are adjacent.
   */
  def isAdjacent(region: ReferenceRegion): Boolean = distance(region) match {
    case Some(d) => d == 1
    case None => false
  }

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
    if (refId == other.refId)
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
    if (refId == other.refId)
      if (overlaps(other))
        Some(0)
      else if (other.start >= end)
        Some(other.start - end + 1)
      else
        Some(start - other.end + 1)
    else
      None

  def contains(other: ReferencePosition): Boolean =
    refId == other.refId && start <= other.pos && end > other.pos

  def contains(other: ReferenceRegion): Boolean =
    refId == other.refId && start <= other.start && end >= other.end

  def overlaps(other: ReferenceRegion): Boolean =
    refId == other.refId && end > other.start && start < other.end

  def compare(that: ReferenceRegion): Int =
    if (refId != that.refId)
      refId.compareTo(that.refId)
    else if (start != that.start)
      start.compareTo(that.start)
    else
      end.compareTo(that.end)

  def length(): Long = {
    end - start
  }
}

class ReferenceRegionSerializer extends Serializer[ReferenceRegion] {
  def write(kryo: Kryo, output: Output, obj: ReferenceRegion) = {
    output.writeInt(obj.refId)
    output.writeLong(obj.start)
    output.writeLong(obj.end)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferenceRegion]): ReferenceRegion = {
    val refId = input.readInt()
    val start = input.readLong()
    val end = input.readLong()
    new ReferenceRegion(refId, start, end)
  }
}
