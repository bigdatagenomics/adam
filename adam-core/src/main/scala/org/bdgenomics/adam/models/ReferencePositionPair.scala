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
import Ordering.Option
import org.bdgenomics.utils.misc.Logging
import org.bdgenomics.adam.instrumentation.Timers.CreateReferencePositionPair
import org.bdgenomics.adam.models.ReferenceRegion._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord

object ReferencePositionPair extends Logging {
  def apply(singleReadBucket: SingleReadBucket): ReferencePositionPair = CreateReferencePositionPair.time {
    val firstOfPair = (singleReadBucket.primaryMapped.filter(_.getReadInFragment == 0) ++
      singleReadBucket.unmapped.filter(_.getReadInFragment == 0)).toSeq
    val secondOfPair = (singleReadBucket.primaryMapped.filter(_.getReadInFragment == 1) ++
      singleReadBucket.unmapped.filter(_.getReadInFragment == 1)).toSeq

    def getPos(r: AlignmentRecord): ReferencePosition = {
      if (r.getReadMapped) {
        new RichAlignmentRecord(r).fivePrimeReferencePosition
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

case class ReferencePositionPair(
  read1refPos: Option[ReferencePosition],
  read2refPos: Option[ReferencePosition])

class ReferencePositionPairSerializer extends Serializer[ReferencePositionPair] {
  val rps = new ReferencePositionSerializer()

  def writeOptionalReferencePos(kryo: Kryo, output: Output, optRefPos: Option[ReferencePosition]) = {
    optRefPos match {
      case None =>
        output.writeBoolean(false)
      case Some(refPos) =>
        output.writeBoolean(true)
        rps.write(kryo, output, refPos)
    }
  }

  def readOptionalReferencePos(kryo: Kryo, input: Input): Option[ReferencePosition] = {
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

