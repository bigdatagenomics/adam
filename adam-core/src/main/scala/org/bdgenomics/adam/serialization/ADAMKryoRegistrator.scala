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
package org.bdgenomics.adam.serialization

import org.apache.avro.specific.{ SpecificDatumWriter, SpecificDatumReader, SpecificRecord }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }
import org.apache.avro.io.{ BinaryDecoder, DecoderFactory, BinaryEncoder, EncoderFactory }
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.models._
import it.unimi.dsi.fastutil.io.{ FastByteArrayInputStream, FastByteArrayOutputStream }
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.algorithms.realignmenttarget._
import scala.collection.immutable.TreeSet
import scala.reflect.ClassTag

case class InputStreamWithDecoder(size: Int) {
  val buffer = new Array[Byte](size)
  val stream = new FastByteArrayInputStream(buffer)
  val decoder = DecoderFactory.get().directBinaryDecoder(stream, null.asInstanceOf[BinaryDecoder])
}

// NOTE: This class is not thread-safe; however, Spark guarantees that only a single thread will access it.
class AvroSerializer[T <: SpecificRecord: ClassTag] extends Serializer[T] {
  val reader = new SpecificDatumReader[T](scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]])
  val writer = new SpecificDatumWriter[T](scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]])
  var in = InputStreamWithDecoder(1024)
  val outstream = new FastByteArrayOutputStream()
  val encoder = EncoderFactory.get().directBinaryEncoder(outstream, null.asInstanceOf[BinaryEncoder])

  setAcceptsNull(false)

  def write(kryo: Kryo, kryoOut: Output, record: T) = {
    outstream.reset()
    writer.write(record, encoder)
    kryoOut.writeInt(outstream.array.length, true)
    kryoOut.write(outstream.array)
  }

  def read(kryo: Kryo, kryoIn: Input, klazz: Class[T]): T = this.synchronized {
    val len = kryoIn.readInt(true)
    if (len > in.size) {
      in = InputStreamWithDecoder(len + 1024)
    }
    in.stream.reset()
    // Read Kryo bytes into input buffer
    kryoIn.readBytes(in.buffer, 0, len)
    // Read the Avro object from the buffer
    reader.read(null.asInstanceOf[T], in.decoder)
  }
}

class ADAMKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[ADAMRecord], new AvroSerializer[ADAMRecord]())
    kryo.register(classOf[ADAMPileup], new AvroSerializer[ADAMPileup]())
    kryo.register(classOf[ADAMGenotype], new AvroSerializer[ADAMGenotype]())
    kryo.register(classOf[ADAMVariant], new AvroSerializer[ADAMVariant]())
    kryo.register(classOf[ADAMFlatGenotype], new AvroSerializer[ADAMFlatGenotype]())
    kryo.register(classOf[ADAMDatabaseVariantAnnotation], new AvroSerializer[ADAMDatabaseVariantAnnotation]())
    kryo.register(classOf[ADAMNucleotideContigFragment], new AvroSerializer[ADAMNucleotideContigFragment]())
    kryo.register(classOf[ADAMFeature], new AvroSerializer[ADAMFeature]())
    kryo.register(classOf[ReferencePositionWithOrientation], new ReferencePositionWithOrientationSerializer)
    kryo.register(classOf[ReferencePosition], new ReferencePositionSerializer)
    kryo.register(classOf[ReferencePositionPair], new ReferencePositionPairSerializer)
    kryo.register(classOf[SingleReadBucket], new SingleReadBucketSerializer)
    kryo.register(classOf[IndelRange], new IndelRangeSerializer())
    kryo.register(classOf[SNPRange], new SNPRangeSerializer)
    kryo.register(classOf[IndelRealignmentTarget])
    kryo.register(classOf[TreeSet[IndelRealignmentTarget]], new TreeSetSerializer)
  }
}
