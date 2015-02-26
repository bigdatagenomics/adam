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

import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd.ADAMContext._
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import scala.annotation.tailrec

object ReferencePositionWithOrientation {

  def apply(record: AlignmentRecord): Option[ReferencePositionWithOrientation] = {
    if (record.getReadMapped) {
      Some(new ReferencePositionWithOrientation(ReferencePosition(record).get, record.getReadNegativeStrand))
    } else {
      None
    }
  }

  def fivePrime(record: AlignmentRecord): Option[ReferencePositionWithOrientation] = {
    if (record.getReadMapped) {
      Some(new ReferencePositionWithOrientation(ReferencePosition.fivePrime(record).get, record.getReadNegativeStrand))
    } else {
      None
    }
  }
  /**
   * Given an index into a sequence (e.g., a transcript), and a sequence
   * of regions that map this sequence to a reference genome, returns a
   * reference position.
   *
   * @param idx Index into a sequence.
   * @param mapping Sequence of reference regions that maps the sequence to a
   * reference genome.
   * @return Returns a lifted over reference position.
   *
   * @throws AssertionError Throws an assertion error if the strandedness does
   * not match between the reference position and the mapping regions.
   * @throws IllegalArgumentException Throws an exception if the index given
   * is out of the range of the mappings given.
   */
  def liftOverToReference(idx: Long,
                          mapping: Seq[ReferenceRegionWithOrientation]): ReferencePositionWithOrientation = {
    val negativeStrand = mapping.head.negativeStrand

    @tailrec def liftOverHelper(regions: Iterator[ReferenceRegionWithOrientation],
                                idx: Long): ReferencePositionWithOrientation = {
      if (!regions.hasNext) {
        throw new IllegalArgumentException("Liftover is out of range")
      } else {
        val reg = regions.next()
        assert(reg.negativeStrand == negativeStrand,
          "Strand is inconsistent across set of regions.")

        // is our position in this region?
        if (reg.width > idx) {
          // get contig
          val chr = reg.referenceName

          // get our pos
          val pos = if (negativeStrand) {
            reg.end - idx - 1 // need -1 because of open end coordinate
          } else {
            reg.start + idx
          }

          // return
          ReferencePositionWithOrientation(ReferencePosition(chr, pos),
            negativeStrand)
        } else {
          // recurse
          liftOverHelper(regions, idx - reg.width)
        }
      }
    }

    // call out to helper
    if (negativeStrand) {
      liftOverHelper(mapping.reverseIterator, idx)
    } else {
      liftOverHelper(mapping.toIterator, idx)
    }
  }
}

case class ReferencePositionWithOrientation(refPos: ReferencePosition,
                                            negativeStrand: Boolean)
    extends Ordered[ReferencePositionWithOrientation] {

  /**
   * Given a sequence of regions that map from a reference genome to a sequence,
   * returns an index into a sequence.
   *
   * @param mapping Sequence of reference regions that maps the sequence to a
   * reference genome.
   * @return Returns an index into a sequence.
   *
   * @throws AssertionError Throws an assertion error if the strandedness does
   * not match between the reference position and the mapping regions.
   * @throws IllegalArgumentException Throws an exception if the index given
   * is out of the range of the mappings given.
   */
  def liftOverFromReference(mapping: Seq[ReferenceRegionWithOrientation]): Long = {
    val pos = refPos.pos

    @tailrec def liftOverHelper(regions: Iterator[ReferenceRegionWithOrientation],
                                idx: Long = 0L): Long = {
      if (!regions.hasNext) {
        throw new IllegalArgumentException("Position was not contained in mapping.")
      } else {
        // get region
        val reg = regions.next()
        assert(reg.negativeStrand == negativeStrand,
          "Strand is inconsistent across set of regions.")

        // are we in this region?
        if (reg.contains(this)) {
          // how far are we from the end of the region
          val into = if (negativeStrand) {
            reg.end - pos
          } else {
            pos - reg.start
          }

          // add to index
          idx + into
        } else {
          liftOverHelper(regions, idx + reg.width)
        }
      }
    }

    // call out to helper
    liftOverHelper(mapping.toIterator)
  }

  override def compare(that: ReferencePositionWithOrientation): Int = {
    val posCompare = refPos.compare(that.refPos)
    if (posCompare != 0) {
      posCompare
    } else {
      negativeStrand.compare(that.negativeStrand)
    }
  }
}

object ReferencePosition {

  /**
   * The UNMAPPED value is a convenience value, which can be used to indicate a position
   * which is not located anywhere along the reference sequences (see, e.g. its use in
   * GenomicPositionPartitioner).
   */
  val UNMAPPED = new ReferencePosition("", -1)

  /**
   * Checks to see if a read is mapped with a valid position.
   *
   * @param record Read to check for mapping.
   * @return True if read is mapped and has a valid position, else false.
   */
  def mappedPositionCheck(record: AlignmentRecord): Boolean = {
    val contig = Option(record.getContig)
    val start = Option(record.getStart)
    record.getReadMapped && (contig.isDefined && Option(contig.get.getContigName).isDefined) && start.isDefined
  }

  /**
   * Generates a reference position from a read. This function generates the
   * position from the start mapping position of the read.
   *
   * @param record Read from which to generate a reference position.
   * @return Reference position wrapped inside of an option. If the read is
   * mapped, the option will be stuffed, else it will be empty (None).
   *
   * @see fivePrime
   */
  def apply(record: AlignmentRecord): Option[ReferencePosition] = {
    if (mappedPositionCheck(record)) {
      Some(new ReferencePosition(record.getContig.getContigName.toString, record.getStart))
    } else {
      None
    }
  }

  /**
   * Generates a reference position from a called variant.
   *
   * @note An invariant of variants is that they have a defined position,
   * therefore we do not wrap them in an option.
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
   * @note An invariant of genotypes is that they have a defined position,
   * therefore we do not wrap them in an option.
   *
   * @param genotype Genotype from which to generate a reference position.
   * @return The reference position of this genotype.
   */
  def apply(genotype: Genotype): ReferencePosition = {
    val variant = genotype.getVariant
    new ReferencePosition(variant.getContig.getContigName, variant.getStart)
  }

  /**
   * Generates a reference position from the five prime end of a read. This
   * will be different from the start mapping position of a read if this
   * read is a reverse strand read.
   *
   * @param record Read from which to generate a reference position.
   * @return Reference position wrapped inside of an option. If the read is
   * mapped, the option will be stuffed, else it will be empty (None).
   *
   * @see apply(record: Read)
   */
  def fivePrime(record: AlignmentRecord): Option[ReferencePosition] = {
    if (mappedPositionCheck(record)) {
      Some(new ReferencePosition(record.getContig.getContigName, record.fivePrimePosition.get))
    } else {
      None
    }
  }
}

case class ReferencePosition(referenceName: String, pos: Long) extends Ordered[ReferencePosition] {

  override def compare(that: ReferencePosition): Int = {
    // Note: important to compare by reference first for coordinate ordering
    val refCompare = referenceName.compare(that.referenceName)
    if (refCompare != 0) {
      refCompare
    } else {
      pos.compare(that.pos)
    }
  }
}

class ReferencePositionWithOrientationSerializer extends Serializer[ReferencePositionWithOrientation] {
  val referencePositionSerializer = new ReferencePositionSerializer()

  def write(kryo: Kryo, output: Output, obj: ReferencePositionWithOrientation) = {
    output.writeBoolean(obj.negativeStrand)
    referencePositionSerializer.write(kryo, output, obj.refPos)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferencePositionWithOrientation]): ReferencePositionWithOrientation = {
    val negativeStrand = input.readBoolean()
    val referencePosition = referencePositionSerializer.read(kryo, input, classOf[ReferencePosition])
    new ReferencePositionWithOrientation(referencePosition, negativeStrand)
  }
}

class ReferencePositionSerializer extends Serializer[ReferencePosition] {
  def write(kryo: Kryo, output: Output, obj: ReferencePosition) = {
    output.writeString(obj.referenceName)
    output.writeLong(obj.pos)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferencePosition]): ReferencePosition = {
    val refName = input.readString()
    val pos = input.readLong()
    new ReferencePosition(refName, pos)
  }
}
