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

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

class MatedReferencePosition(val read1RefPos: ReferencePosition, val read2RefPos: Option[ReferencePosition])
  extends Ordered[MatedReferencePosition] with Serializable {
  def compare(that: MatedReferencePosition): Int = {
    val posCompare = this.read1RefPos.compare(that.read1RefPos)
    if (posCompare != 0) return posCompare
    (this.read2RefPos, that.read2RefPos) match {
      case (Some(thisPos), Some(thatPos)) => thisPos.compare(thatPos)
      case (None, Some(thatPos)) => -1
      case (Some(thisPos), None) => 1
      case (None, None) => 0
    }
  }
}

// Used by the KryoSerializer
class MatedReferencePositionSerializer extends Serializer[MatedReferencePosition] {
  val referencePositionSerializer = new ReferencePositionSerializer

  def write(kryo: Kryo, output: Output, matedReferencePosition: MatedReferencePosition) = {
    referencePositionSerializer.write(kryo, output, matedReferencePosition.read1RefPos)
    matedReferencePosition.read2RefPos match {
      case None =>
        output.writeBoolean(false)
      case Some(read2refPos) =>
        output.writeBoolean(true)
        referencePositionSerializer.write(kryo, output, read2refPos)
    }

  }

  def read(kryo: Kryo, input: Input, klass: Class[MatedReferencePosition]): MatedReferencePosition = {
    val read1pos = referencePositionSerializer.read(kryo, input, classOf[ReferencePosition])
    val read2pos = input.readBoolean() match {
      case true =>
        Some(referencePositionSerializer.read(kryo, input, classOf[ReferencePosition]))
      case false =>
        None
    }
    new MatedReferencePosition(read1pos, read2pos)
  }

}
