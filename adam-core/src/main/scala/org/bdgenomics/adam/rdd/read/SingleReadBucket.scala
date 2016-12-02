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
package org.bdgenomics.adam.rdd.read

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Output, Input }
import org.bdgenomics.utils.misc.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  Fragment
}
import scala.collection.JavaConversions._

/**
 * Companion object for building SingleReadBuckets.
 */
private[read] object SingleReadBucket extends Logging {

  /**
   * Builds an RDD of SingleReadBuckets from an RDD of AlignmentRecords.
   *
   * @param rdd The RDD of AlignmentRecords to build the RDD of single read
   *   buckets from.
   * @return Returns an RDD of SingleReadBuckets.
   */
  def apply(rdd: RDD[AlignmentRecord]): RDD[SingleReadBucket] = {
    rdd.groupBy(p => (p.getRecordGroupName, p.getReadName))
      .map(kv => {
        val (_, reads) = kv

        // split by mapping
        val (mapped, unmapped) = reads.partition(_.getReadMapped)
        val (primaryMapped, secondaryMapped) = mapped.partition(_.getPrimaryAlignment)

        // TODO: consider doing validation here
        // (e.g. read says mate mapped but it doesn't exist)
        new SingleReadBucket(primaryMapped, secondaryMapped, unmapped)
      })
  }
}

/**
 * A representation of all of the read alignments that came from a single sequenced
 * fragment.
 *
 * @param primaryMapped All read alignments that are primary alignments.
 * @param secondaryMapped All read alignments that are non-primary (i.e.,
 *   secondary or supplementary alignments).
 * @param unmapped All reads from the fragment that are unmapped.
 */
private[adam] case class SingleReadBucket(
    primaryMapped: Iterable[AlignmentRecord] = Iterable.empty,
    secondaryMapped: Iterable[AlignmentRecord] = Iterable.empty,
    unmapped: Iterable[AlignmentRecord] = Iterable.empty) {

  /**
   * @return The union of the primary, secondary, and unmapped buckets.
   */
  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }

  /**
   * Converts to an Avro Fragment record.
   *
   * @return Converts this bucket to a Fragment type, which does not have the
   *   various alignment buckets, but is otherwise equivalent.
   */
  def toFragment: Fragment = {
    // take union of all reads, as we will need this for building and
    // want to pay the cost exactly once
    val unionReads = allReads

    // start building fragment
    val builder = Fragment.newBuilder()
      .setReadName(unionReads.head.getReadName)
      .setAlignments(seqAsJavaList(allReads.toSeq))

    // is an insert size defined for this fragment?
    primaryMapped.headOption
      .foreach(r => {
        Option(r.getInferredInsertSize).foreach(is => {
          builder.setFragmentSize(is.toInt)
        })
      })

    // set record group name, if known
    Option(unionReads.head.getRecordGroupName)
      .foreach(n => builder.setRunId(n))

    builder.build()
  }
}

class SingleReadBucketSerializer extends Serializer[SingleReadBucket] {
  val recordSerializer = new AvroSerializer[AlignmentRecord]()

  def writeArray(kryo: Kryo, output: Output, reads: Seq[AlignmentRecord]): Unit = {
    output.writeInt(reads.size, true)
    for (read <- reads) {
      recordSerializer.write(kryo, output, read)
    }
  }

  def readArray(kryo: Kryo, input: Input): Seq[AlignmentRecord] = {
    val numReads = input.readInt(true)
    (0 until numReads).foldLeft(List[AlignmentRecord]()) {
      (a, b) => recordSerializer.read(kryo, input, classOf[AlignmentRecord]) :: a
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

