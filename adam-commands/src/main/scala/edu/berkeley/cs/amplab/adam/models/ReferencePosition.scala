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
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}

object ReferencePosition {

  def apply(record: ADAMRecord): ReferencePosition = {
    new ReferencePosition(record.getReferenceId, record.getStart)
  }

  def apply(refId: Int, pos: Long): ReferencePosition = {
    new ReferencePosition(refId, pos)
  }

}

class ReferencePosition(val refId: Int, val pos: Long) extends Ordered[ReferencePosition] with Serializable {

  def compare(that: ReferencePosition): Int = {
    val refCompare = refId.compare(that.refId)
    if (refCompare != 0) return refCompare
    pos.compare(that.pos)
  }

}

// Used by the KryoSerializer
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
