/**
 * Copyright 2013 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.models

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import scala.math.{min, max}

/**
 * Represents a contiguous region of the reference genome.
 *
 * @param refId The index of the sequence (chromosome) in the reference genome
 * @param start The 0-based residue-coordinate for the start of the region
 * @param end The 0-based residue-coordinate for the first residue <i>after</i> the start
 *            which is <i>not</i> in the region -- i.e. [start, end) define a 0-based
 *            half-open interval.
 */
case class ReferenceRegion(refId : Int, start : Long, end : Long) extends Ordered[ReferenceRegion] {

  assert(start >= 0)
  assert(end >= start)

  def width : Long = end - start

  def union(region : ReferenceRegion) : ReferenceRegion = {
    assert(overlaps(region), "Cannot compute union of two non-overlapping regions")
    ReferenceRegion(refId, min(start, region.start), max(end, region.end))
  }

  def distance(other : ReferencePosition) : Option[Long] =
    if(refId == other.refId)
      if(other.pos < start)
        Some(start - other.pos)
      else if (other.pos >= end)
        Some(other.pos - end)
      else
        Some(0)
    else
      None

  def distance(other : ReferenceRegion) : Option[Long] =
    if(refId == other.refId)
      if(overlaps(other))
        Some(0)
      else if (other.start >= end)
        Some(other.start - end)
      else
        Some(start - other.end)
    else
      None

  def contains(other : ReferencePosition) : Boolean =
    refId == other.refId && start <= other.pos && end > other.pos

  def contains(other : ReferenceRegion) : Boolean =
    refId == other.refId && start <= other.start && end >= other.end

  def overlaps(other : ReferenceRegion) : Boolean =
    refId == other.refId && end > other.start && start < other.end

  def compare(that: ReferenceRegion): Int =
    if(refId != that.refId)
      refId.compareTo(that.refId)
    else if(start != that.start)
      start.compareTo(that.start)
    else
      end.compareTo(that.end)
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
