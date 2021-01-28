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
package org.bdgenomics.adam.ds.read

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferencePositionSerializer
}
import org.bdgenomics.adam.rich.RichAlignment
import org.bdgenomics.formats.avro.Alignment

/**
 * A singleton object for creating reference position pairs.
 */
private[read] object ReferencePositionPair {

  /**
   * Extracts the reference positions from a bucket of reads from a single fragment.
   *
   * @param singleReadBucket A bucket of reads from a single DNA fragment.
   * @return Returns the start position pair for the primary aligned first and
   *   second of pair reads.
   */
  def apply(singleReadBucket: SingleReadBucket): ReferencePositionPair = {
    val firstOfPair = (singleReadBucket.primaryMapped.filter(_.getReadInFragment == 0) ++
      singleReadBucket.unmapped.filter(_.getReadInFragment == 0)).toSeq
    val secondOfPair = (singleReadBucket.primaryMapped.filter(_.getReadInFragment == 1) ++
      singleReadBucket.unmapped.filter(_.getReadInFragment == 1)).toSeq

    def getPos(r: Alignment): ReferencePosition = {
      require(r.sequence != null, "Alignment sequence must not be null!")
      if (r.getReadMapped) {
        new RichAlignment(r).fivePrimeReferencePosition
      } else {
        ReferencePosition(r.getSequence, 0L)
      }
    }

    if (firstOfPair.size + secondOfPair.size > 0) {
      new ReferencePositionPair(
        firstOfPair.lift(0).map(getPos),
        secondOfPair.lift(0).map(getPos)
      )
    } else {
      new ReferencePositionPair(
        (singleReadBucket.primaryMapped ++
          singleReadBucket.unmapped).toSeq.headOption.map(getPos),
        None
      )
    }
  }
}

/**
 * The start positions for a fragment sequenced with a paired protocol.
 *
 * @param read1refPos The start position of the first-of-pair read, if aligned.
 * @param read2refPos The start position of the second-of-pair read, if aligned.
 */
private[adam] case class ReferencePositionPair(
    read1refPos: Option[ReferencePosition],
    read2refPos: Option[ReferencePosition]) {
}

class ReferencePositionPairSerializer extends Serializer[ReferencePositionPair] {
  val rps = new ReferencePositionSerializer()

  private def writeOptionalReferencePos(kryo: Kryo, output: Output, optRefPos: Option[ReferencePosition]) = {
    optRefPos match {
      case None =>
        output.writeBoolean(false)
      case Some(refPos) =>
        output.writeBoolean(true)
        rps.write(kryo, output, refPos)
    }
  }

  private def readOptionalReferencePos(kryo: Kryo, input: Input): Option[ReferencePosition] = {
    val exists = input.readBoolean()
    if (exists) {
      Some(rps.read(kryo, input, classOf[ReferencePosition]))
    } else {
      None
    }
  }

  def write(kryo: Kryo, output: Output, obj: ReferencePositionPair) = {
    writeOptionalReferencePos(kryo, output, obj.read1refPos)
    writeOptionalReferencePos(kryo, output, obj.read2refPos)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferencePositionPair]): ReferencePositionPair = {
    val read1ref = readOptionalReferencePos(kryo, input)
    val read2ref = readOptionalReferencePos(kryo, input)
    new ReferencePositionPair(read1ref, read2ref)
  }
}

