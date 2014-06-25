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

import org.bdgenomics.formats.avro.ADAMRecord

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Output, Input }
import org.bdgenomics.adam.serialization.AvroSerializer
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

object SingleReadBucket extends Logging {
  def apply(rdd: RDD[ADAMRecord]): RDD[SingleReadBucket] = {
    rdd.groupBy(p => (p.getRecordGroupId, p.getReadName))
      .map(kv => {
        val ((recordGroup, readName), reads) = kv

        // split by mapping
        val (mapped, unmapped) = reads.partition(_.getReadMapped)
        val (primaryMapped, secondaryMapped) = mapped.partition(_.getPrimaryAlignment)

        // TODO: consider doing validation here (e.g. read says mate mapped but it doesn't exist)
        new SingleReadBucket(primaryMapped, secondaryMapped, unmapped)
      })
  }
}

case class SingleReadBucket(primaryMapped: Iterable[ADAMRecord] = Seq.empty,
                            secondaryMapped: Iterable[ADAMRecord] = Seq.empty,
                            unmapped: Iterable[ADAMRecord] = Seq.empty) {
  // Note: not a val in order to save serialization/memory cost
  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }
}

class SingleReadBucketSerializer extends Serializer[SingleReadBucket] {
  val recordSerializer = new AvroSerializer[ADAMRecord]()

  def writeArray(kryo: Kryo, output: Output, reads: Seq[ADAMRecord]): Unit = {
    output.writeInt(reads.size, true)
    for (read <- reads) {
      recordSerializer.write(kryo, output, read)
    }
  }

  def readArray(kryo: Kryo, input: Input): Seq[ADAMRecord] = {
    val numReads = input.readInt(true)
    (0 until numReads).foldLeft(List[ADAMRecord]()) {
      (a, b) => recordSerializer.read(kryo, input, classOf[ADAMRecord]) :: a
    }
  }

  def write(kryo: Kryo, output: Output, groupedReads: SingleReadBucket) = {
    writeArray(kryo, output, groupedReads.primaryMapped.toSeq)
    writeArray(kryo, output, groupedReads.secondaryMapped.toSeq)
    writeArray(kryo, output, groupedReads.unmapped.toSeq)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[SingleReadBucket]): SingleReadBucket = {
    val primaryReads = readArray(kryo, input)
    val secondaryReads = readArray(kryo, input)
    val unmappedReads = readArray(kryo, input)
    new SingleReadBucket(primaryReads, secondaryReads, unmappedReads)
  }
}

