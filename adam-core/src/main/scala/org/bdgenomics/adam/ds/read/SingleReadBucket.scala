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
package org.bdgenomics.adam.ds.read

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Output, Input }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro.{
  Alignment,
  Fragment
}
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

private class FragmentIterator(
    reads: Iterator[Alignment]) extends Iterator[Iterable[Alignment]] with Serializable {

  private var readIter: BufferedIterator[Alignment] = reads.buffered

  def hasNext: Boolean = {
    readIter.hasNext
  }

  def next: Iterable[Alignment] = {

    // get the read name
    val readName = readIter.head.getReadName

    @tailrec def getReads(
      l: ListBuffer[Alignment]): Iterable[Alignment] = {

      if (!readIter.hasNext ||
        readIter.head.getReadName != readName) {
        l.toIterable
      } else {
        getReads(l += readIter.next)
      }
    }

    getReads(ListBuffer.empty)
  }
}

/**
 * Companion object for building SingleReadBuckets.
 */
private[read] object SingleReadBucket {

  private def fromGroupedReads(reads: Iterable[Alignment]): SingleReadBucket = {
    // split by mapping
    val (mapped, unmapped) = reads.partition(_.getReadMapped)
    val (primaryMapped, secondaryMapped) = mapped.partition(_.getPrimaryAlignment)

    // TODO: consider doing validation here
    // (e.g. read says mate mapped but it doesn't exist)
    new SingleReadBucket(primaryMapped, secondaryMapped, unmapped)
  }

  /**
   * Builds an RDD of SingleReadBuckets from a queryname sorted RDD of Alignments.
   *
   * @param rdd The RDD of Alignments to build the RDD of single read
   *   buckets from.
   * @return Returns an RDD of SingleReadBuckets.
   *
   * @note We do not validate that the input RDD is sorted by read name.
   */
  def fromQuerynameSorted(rdd: RDD[Alignment]): RDD[SingleReadBucket] = {
    rdd.mapPartitions(iter => new FragmentIterator(iter).map(fromGroupedReads))
  }

  /**
   * Builds an RDD of SingleReadBuckets from an RDD of Alignments.
   *
   * @param rdd The RDD of Alignments to build the RDD of single read
   *   buckets from.
   * @return Returns an RDD of SingleReadBuckets.
   */
  def apply(rdd: RDD[Alignment]): RDD[SingleReadBucket] = {
    rdd.groupBy(p => (p.getReadGroupId, p.getReadName))
      .map(kv => {
        val (_, reads) = kv

        fromGroupedReads(reads)
      })
  }

  def apply(fragment: Fragment): SingleReadBucket = {
    fromGroupedReads(fragment.getAlignments.toIterable)
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
    primaryMapped: Iterable[Alignment] = Iterable.empty,
    secondaryMapped: Iterable[Alignment] = Iterable.empty,
    unmapped: Iterable[Alignment] = Iterable.empty) {

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
      .setName(unionReads.head.getReadName)
      .setAlignments(seqAsJavaList(allReads.toSeq))

    // is an insert size defined for this fragment?
    primaryMapped.headOption
      .foreach(r => {
        Option(r.getInsertSize).foreach(is => {
          builder.setInsertSize(is.toInt)
        })
      })

    // set read group name, if known
    Option(unionReads.head.getReadGroupId)
      .foreach(n => builder.setReadGroupId(n))

    builder.build()
  }
}

class SingleReadBucketSerializer extends Serializer[SingleReadBucket] {
  val recordSerializer = new AvroSerializer[Alignment]()

  def writeArray(kryo: Kryo, output: Output, reads: Seq[Alignment]): Unit = {
    output.writeInt(reads.size, true)
    for (read <- reads) {
      recordSerializer.write(kryo, output, read)
    }
  }

  def readArray(kryo: Kryo, input: Input): Seq[Alignment] = {
    val numReads = input.readInt(true)
    (0 until numReads).foldLeft(List[Alignment]()) {
      (a, b) => recordSerializer.read(kryo, input, classOf[Alignment]) :: a
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

