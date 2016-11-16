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

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.bdgenomics.formats.avro._

/**
 * A sort order that orders all positions lexicographically by contig and
 * numerically within a single contig.
 */
object PositionOrdering extends ReferenceOrdering[ReferencePosition] {
}

/**
 * A sort order that orders all given positions lexicographically by contig and
 * numerically within a single contig, and puts all non-provided positions at
 * the end. An extension of PositionOrdering to Optional data.
 *
 * @see PositionOrdering
 */
object OptionalPositionOrdering extends OptionalReferenceOrdering[ReferencePosition] {
  val baseOrdering = PositionOrdering
}

/**
 * Companion object for creating and sorting ReferencePositions.
 */
object ReferencePosition extends Serializable {

  implicit def orderingForPositions = PositionOrdering
  implicit def orderingForOptionalPositions = OptionalPositionOrdering

  /**
   * The UNMAPPED value is a convenience value, which can be used to indicate a position
   * which is not located anywhere along the reference sequences (see, e.g. its use in
   * GenomicPositionPartitioner).
   */
  val UNMAPPED = new ReferencePosition("", 0)

  /**
   * Generates a reference position from a read. This function generates the
   * position from the start mapping position of the read.
   *
   * @param record Read from which to generate a reference position.
   * @return Reference position of the start position.
   *
   * @see fivePrime
   */
  def apply(record: AlignmentRecord): ReferencePosition = {
    new ReferencePosition(record.getContigName, record.getStart)
  }

  /**
   * Generates a reference position from a called variant.
   *
   * @param variant Called variant from which to generate a
   * reference position.
   * @return The reference position of this variant.
   */
  def apply(variant: Variant): ReferencePosition = {
    new ReferencePosition(variant.getContigName, variant.getStart)
  }

  /**
   * Generates a reference position from a genotype.
   *
   * @param genotype Genotype from which to generate a reference position.
   * @return The reference position of this genotype.
   */
  def apply(genotype: Genotype): ReferencePosition = {
    val contigNameSet = Seq(Option(genotype.getContigName), Option(genotype.getVariant.getContigName))
      .flatten
      .toSet
    val startSet = Seq(Option(genotype.getStart), Option(genotype.getVariant.getStart))
      .flatten
      .toSet
    require(contigNameSet.nonEmpty, "Genotype has no contig name: %s".format(genotype))
    require(contigNameSet.size == 1, "Genotype has multiple contig names: %s, %s".format(
      contigNameSet, genotype))
    require(startSet.nonEmpty, "Genotype has no start: %s".format(genotype))
    require(startSet.size == 1, "Genotype has multiple starts: %s, %s".format(
      startSet, genotype))

    new ReferencePosition(contigNameSet.head, startSet.head)
  }

  /**
   * Convenience method for building a ReferencePosition.
   *
   * @param referenceName The name of the reference contig this locus exists on.
   * @param pos The position of this locus.
   */
  def apply(referenceName: String, pos: Long): ReferencePosition = {
    new ReferencePosition(referenceName, pos)
  }

  /**
   * Convenience method for building a ReferencePosition.
   *
   * @param referenceName The name of the reference contig this locus exists on.
   * @param pos The position of this locus.
   * @param orientation The strand that this locus is on.
   */
  def apply(referenceName: String, pos: Long, orientation: Strand): ReferencePosition = {
    new ReferencePosition(referenceName, pos, orientation)
  }
}

/**
 * A single genomic locus.
 *
 * @param referenceName The name of the reference contig this locus exists on.
 * @param pos The position of this locus.
 * @param orientation The strand that this locus is on.
 */
class ReferencePosition(
  override val referenceName: String,
  val pos: Long,
  override val orientation: Strand = Strand.INDEPENDENT)
    extends ReferenceRegion(referenceName, pos, pos + 1, orientation)

class ReferencePositionSerializer extends Serializer[ReferencePosition] {
  private val enumValues = Strand.values()

  def write(kryo: Kryo, output: Output, obj: ReferencePosition) = {
    output.writeString(obj.referenceName)
    output.writeLong(obj.pos)
    output.writeInt(obj.orientation.ordinal)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferencePosition]): ReferencePosition = {
    val refName = input.readString()
    val pos = input.readLong()
    val orientation = input.readInt()
    new ReferencePosition(refName, pos, enumValues(orientation))
  }
}
