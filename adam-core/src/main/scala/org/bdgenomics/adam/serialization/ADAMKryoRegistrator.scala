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

import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.avro.AvroSerializer
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.read.realignment._
import org.bdgenomics.formats.avro._

class ADAMKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[AlignmentRecord], AvroSerializer.SpecificRecordSerializer[AlignmentRecord])
    kryo.register(classOf[Pileup], AvroSerializer.SpecificRecordSerializer[Pileup])
    kryo.register(classOf[Genotype], AvroSerializer.SpecificRecordSerializer[Genotype])
    kryo.register(classOf[Variant], AvroSerializer.SpecificRecordSerializer[Variant])
    kryo.register(classOf[FlatGenotype], AvroSerializer.SpecificRecordSerializer[FlatGenotype])
    kryo.register(classOf[DatabaseVariantAnnotation], AvroSerializer.SpecificRecordSerializer[DatabaseVariantAnnotation])
    kryo.register(classOf[NucleotideContigFragment], AvroSerializer.SpecificRecordSerializer[NucleotideContigFragment])
    kryo.register(classOf[Feature], AvroSerializer.SpecificRecordSerializer[Feature])
    kryo.register(classOf[ReferencePositionWithOrientation], new ReferencePositionWithOrientationSerializer)
    kryo.register(classOf[ReferencePosition], new ReferencePositionSerializer)
    kryo.register(classOf[ReferencePositionPair], new ReferencePositionPairSerializer)
    kryo.register(classOf[SingleReadBucket], new SingleReadBucketSerializer)
    kryo.register(classOf[IndelRealignmentTarget])
    kryo.register(classOf[TargetSet], new TargetSetSerializer)
    kryo.register(classOf[ZippedTargetSet], new ZippedTargetSetSerializer)
  }
}
