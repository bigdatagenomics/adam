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
import org.bdgenomics.adam.serialization.AvroSerializer
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, Output }

/**
 * This class is similar to SingleReadBucket, except it breaks the reads down further.
 *
 * Rather than stopping at primary/secondary/unmapped, this will break it down further into whether they are paired
 * or unpaired, and then whether they are the first or second of the pair.
 *
 * This is useful as this will usually map a single read in any of the sequences.
 */
case class ReadBucket(unpairedPrimaryMappedReads: Iterable[ADAMRecord] = Seq.empty,
                      pairedFirstPrimaryMappedReads: Iterable[ADAMRecord] = Seq.empty,
                      pairedSecondPrimaryMappedReads: Iterable[ADAMRecord] = Seq.empty,
                      unpairedSecondaryMappedReads: Iterable[ADAMRecord] = Seq.empty,
                      pairedFirstSecondaryMappedReads: Iterable[ADAMRecord] = Seq.empty,
                      pairedSecondSecondaryMappedReads: Iterable[ADAMRecord] = Seq.empty,
                      unmappedReads: Iterable[ADAMRecord] = Seq.empty) {
  def allReads(): Iterable[ADAMRecord] =
    unpairedPrimaryMappedReads ++
      pairedFirstPrimaryMappedReads ++
      pairedSecondPrimaryMappedReads ++
      unpairedSecondaryMappedReads ++
      pairedFirstSecondaryMappedReads ++
      pairedSecondSecondaryMappedReads ++
      unmappedReads
}

class ReadBucketSerializer extends Serializer[ReadBucket] {
  val recordSerializer = new AvroSerializer[ADAMRecord]()

  def writeArray(kryo: Kryo, output: Output, reads: Iterable[ADAMRecord]): Unit = {
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

  def write(kryo: Kryo, output: Output, bucket: ReadBucket) = {
    writeArray(kryo, output, bucket.unpairedPrimaryMappedReads)
    writeArray(kryo, output, bucket.pairedFirstPrimaryMappedReads)
    writeArray(kryo, output, bucket.pairedSecondPrimaryMappedReads)
    writeArray(kryo, output, bucket.unpairedSecondaryMappedReads)
    writeArray(kryo, output, bucket.pairedFirstSecondaryMappedReads)
    writeArray(kryo, output, bucket.pairedSecondSecondaryMappedReads)
    writeArray(kryo, output, bucket.unmappedReads)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[ReadBucket]): ReadBucket = {
    val unpairedPrimaryReads = readArray(kryo, input)
    val pairedFirstPrimaryMappedReads = readArray(kryo, input)
    val pairedSecondPrimaryMappedReads = readArray(kryo, input)
    val unpairedSecondaryReads = readArray(kryo, input)
    val pairedFirstSecondaryMappedReads = readArray(kryo, input)
    val pairedSecondSecondaryMappedReads = readArray(kryo, input)
    val unmappedReads = readArray(kryo, input)
    new ReadBucket(
      unpairedPrimaryReads,
      pairedFirstPrimaryMappedReads,
      pairedSecondPrimaryMappedReads,
      unpairedSecondaryReads,
      pairedFirstSecondaryMappedReads,
      pairedSecondSecondaryMappedReads,
      unmappedReads)
  }
}

object ReadBucket {
  implicit def singleReadBucketToReadBucket(bucket: SingleReadBucket): ReadBucket = {
    val (pairedPrimary, unpairedPrimary) = bucket.primaryMapped.partition(_.getReadPaired)
    val (pairedFirstPrimary, pairedSecondPrimary) = pairedPrimary.partition(_.getFirstOfPair)
    val (pairedSecondary, unpairedSecondary) = bucket.secondaryMapped.partition(_.getReadPaired)
    val (pairedFirstSecondary, pairedSecondSecondary) = pairedSecondary.partition(_.getFirstOfPair)

    new ReadBucket(unpairedPrimary,
      pairedFirstPrimary,
      pairedSecondPrimary,
      unpairedSecondary,
      pairedFirstSecondary,
      pairedSecondSecondary,
      bucket.unmapped)
  }
}
