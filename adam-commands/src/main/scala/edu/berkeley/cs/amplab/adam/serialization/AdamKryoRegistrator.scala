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
package edu.berkeley.cs.amplab.adam.serialization

import org.apache.avro.specific.{SpecificDatumWriter, SpecificDatumReader, SpecificRecord}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, BinaryEncoder, EncoderFactory}
import spark.KryoRegistrator
import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotype, ADAMPileup, ADAMRecord}
import edu.berkeley.cs.amplab.adam.models.{MatedReferencePositionSerializer, MatedReferencePosition, ReferencePositionSerializer, ReferencePosition}

class AvroSerializer[T <: SpecificRecord : ClassManifest] extends Serializer[T] {
  val reader = new SpecificDatumReader[T](classManifest[T].erasure.asInstanceOf[Class[T]])
  val writer = new SpecificDatumWriter[T](classManifest[T].erasure.asInstanceOf[Class[T]])
  var encoder = null.asInstanceOf[BinaryEncoder]
  var decoder = null.asInstanceOf[BinaryDecoder]

  setAcceptsNull(false)

  def write(kryo: Kryo, output: Output, record: T) = {
    encoder = EncoderFactory.get().directBinaryEncoder(output, encoder)
    writer.write(record, encoder)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[T]): T = this.synchronized {
    decoder = DecoderFactory.get().directBinaryDecoder(input, decoder)
    reader.read(null.asInstanceOf[T], decoder)
  }
}

class AdamKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[ADAMRecord], new AvroSerializer[ADAMRecord]())
    kryo.register(classOf[ADAMPileup], new AvroSerializer[ADAMPileup]())
    kryo.register(classOf[ADAMGenotype], new AvroSerializer[ADAMGenotype]())
    kryo.register(classOf[ReferencePosition], new ReferencePositionSerializer)
    kryo.register(classOf[MatedReferencePosition], new MatedReferencePositionSerializer)
  }
}
