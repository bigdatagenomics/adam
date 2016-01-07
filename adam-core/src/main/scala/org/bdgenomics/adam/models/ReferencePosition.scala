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

object PositionOrdering extends ReferenceOrdering[ReferencePosition] {
}
object OptionalPositionOrdering extends OptionalReferenceOrdering[ReferencePosition] {
  val baseOrdering = PositionOrdering
}

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
    new ReferencePosition(record.getContig.getContigName, record.getStart)
  }

  /**
   * Generates a reference position from a called variant.
   *
   * @param variant Called variant from which to generate a
   * reference position.
   * @return The reference position of this variant.
   */
  def apply(variant: Variant): ReferencePosition = {
    new ReferencePosition(variant.getContig.getContigName, variant.getStart)
  }

  /**
   * Generates a reference position from a genotype.
   *
   * @param genotype Genotype from which to generate a reference position.
   * @return The reference position of this genotype.
   */
  def apply(genotype: Genotype): ReferencePosition = {
    val variant = genotype.getVariant
    new ReferencePosition(variant.getContig.getContigName, variant.getStart)
  }

  def apply(referenceName: String, pos: Long): ReferencePosition = {
    new ReferencePosition(referenceName, pos)
  }

  def apply(referenceName: String, pos: Long, orientation: Strand): ReferencePosition = {
    new ReferencePosition(referenceName, pos, orientation)
  }
}

class ReferencePosition(
  override val referenceName: String,
  val pos: Long,
  override val orientation: Strand = Strand.Independent)
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
