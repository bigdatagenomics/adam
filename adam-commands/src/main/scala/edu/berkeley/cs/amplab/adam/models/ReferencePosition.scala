/*
 * Copyright (c) 2013. Regents of the University of California
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

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import Ordering.Option

object ReferencePositionWithOrientation {

  def apply(record: ADAMRecord): Option[ReferencePositionWithOrientation] = {
    if (record.getReadMapped) {
      Some(new ReferencePositionWithOrientation(ReferencePosition(record), record.getReadNegativeStrand))
    } else {
      None
    }
  }

  def fivePrime(record: ADAMRecord): Option[ReferencePositionWithOrientation] = {
    if (record.getReadMapped) {
      Some(new ReferencePositionWithOrientation(ReferencePosition.fivePrime(record), record.getReadNegativeStrand))
    } else {
      None
    }
  }

}

case class ReferencePositionWithOrientation(refPos: Option[ReferencePosition], negativeStrand: Boolean)
  extends Ordered[ReferencePositionWithOrientation] {
  override def compare(that: ReferencePositionWithOrientation): Int = {
    val posCompare = refPos.compare(that.refPos)
    if (posCompare != 0) {
      posCompare
    }
    else {
      negativeStrand.compare(that.negativeStrand)
    }
  }
}

object ReferencePosition {

  def mappedPositionCheck(record: ADAMRecord): Boolean = {
    val referenceId = Some(record.getReferenceId)
    val start = Some(record.getStart)
    record.getReadMapped && referenceId.isDefined && start.isDefined
  }

  def apply(record: ADAMRecord): Option[ReferencePosition] = {
    if (mappedPositionCheck(record)) {
      Some(new ReferencePosition(record.getReferenceId, record.getStart))
    } else {
      None
    }
  }

  def fivePrime(record: ADAMRecord): Option[ReferencePosition] = {
    if (mappedPositionCheck(record)) {
      Some(new ReferencePosition(record.getReferenceId, record.fivePrimePosition.get))
    } else {
      None
    }
  }
}

case class ReferencePosition(refId: Int, pos: Long) extends Ordered[ReferencePosition] {

  def compare(that: ReferencePosition): Int = {
    // Note: important to compare by reference first for coordinate ordering
    val refCompare = refId.compare(that.refId)
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
    obj.refPos match {
      case None =>
        output.writeBoolean(false)
      case Some(refPos) =>
        output.writeBoolean(true)
        referencePositionSerializer.write(kryo, output, refPos)
    }
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferencePositionWithOrientation]): ReferencePositionWithOrientation = {
    val negativeStrand = input.readBoolean()
    val hasPos = input.readBoolean()
    hasPos match {
      case false =>
        new ReferencePositionWithOrientation(None, negativeStrand)
      case true =>
        val referencePosition = referencePositionSerializer.read(kryo, input, classOf[ReferencePosition])
        new ReferencePositionWithOrientation(Some(referencePosition), negativeStrand)
    }
  }
}

class ReferencePositionSerializer extends Serializer[ReferencePosition] {
  def write(kryo: Kryo, output: Output, obj: ReferencePosition) = {
    output.writeInt(obj.refId)
    output.writeLong(obj.pos)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReferencePosition]): ReferencePosition = {
    val refId = input.readInt()
    val pos = input.readLong()
    new ReferencePosition(refId, pos)
  }
}
