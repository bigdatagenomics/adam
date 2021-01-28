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

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Alignment

/**
 * Helper object to reassemble paired reads from a queryname sorted file.
 *
 * When loading a queryname sorted file, the splits in the file may split the
 * reads in a pair across multiple splits. This code looks at the start of each
 * split and pulls the reads that are at the start of a split to the end of the
 * prior split. This ensures that all reads from a single pair are in the same
 * partition.
 *
 * This is necessary with Hadoop-BAM 7.8.0, which dropped the ability to ensure
 * that the splits for a queryname sorted file preserved said invariant.
 */
private[ds] object RepairPartitions extends Serializable {

  /**
   * Extracts the read pair at the start of a partition.
   *
   * @param idx The partition index. This function doesn't run for the 0th
   *   partition in an RDD, since we can't move those reads to a -1th partition.
   * @param iter The iterator to pull the reads from.
   * @return Returns an iterator with the partition index and reads from the
   *   read pair at the start of the partition. If this is the 0th partition
   *   or the iterator passed in was empty, then we return an empty iterator.
   */
  private[read] def getPairAtStart(
    idx: Int,
    iter: Iterator[Alignment]): Iterator[(Int, Seq[Alignment])] = {

    if (iter.hasNext && idx > 0) {
      val bufferedIterator = iter.buffered
      val firstReadName = bufferedIterator.head.getReadName

      // going toSeq here actually goes toStream, which causes odd, hard to debug
      // serialization issues. hence, we go toArray then toSeq.
      Iterator((idx, bufferedIterator.takeWhile(_.getReadName == firstReadName).toArray.toSeq))
    } else {
      Iterator.empty
    }
  }

  /**
   * Drops the read pair at the start of the partition.
   *
   * @param idx The index of the partition. If we are on the 0th partition, we
   *   do not drop the first read pair, since we do not move reads from the 0th
   *   partition.
   * @param iter The iterator to drop the reads from.
   * @return Returns an iterator where the reads that we want to drop have been
   *   omitted.
   */
  private[read] def dropPairAtStart(
    idx: Int,
    iter: Iterator[Alignment]): Iterator[Alignment] = {

    if (idx == 0) {
      iter
    } else if (iter.hasNext) {
      val bufferedIterator = iter.buffered
      val firstReadName = bufferedIterator.head.getReadName

      bufferedIterator.dropWhile(_.getReadName == firstReadName)
    } else {
      Iterator.empty
    }
  }

  /**
   * Drops reads from the partition and then appends the reads we want to the end.
   *
   * @param idx The index of this partition. We only drop reads from the last
   *   partition, and only add reads to the first partition.
   * @param iter The reads in this partition.
   * @param pairMap An array containing reads to be moved to each partition.
   * @return Returns the final reads that should be on this partition.
   */
  private[read] def addPairsAtEnd(
    idx: Int,
    iter: Iterator[Alignment],
    pairMap: Array[Seq[Alignment]]): Iterator[Alignment] = {

    // if we aren't in the last partition, then we should add reads
    val readsToAdd = if (idx == pairMap.length) {
      Iterator[Alignment]()
    } else {
      pairMap(idx).toIterator
    }

    dropPairAtStart(idx, iter) ++ readsToAdd
  }

  /**
   * Builds an array to send to the final stage (addPairsAtEnd).
   *
   * @param array An array of (partition from, read) pairs.
   * @param numOutputPartitions The number of partitions that we plan to output.
   * @return Returns an array where the indices is the "output partition index"
   *   ("partition from" - 1), and the reads that should move to that partition.
   */
  private[read] def unrollArray(
    array: Array[(Int, Seq[Alignment])],
    numOutputPartitions: Int): Array[Seq[Alignment]] = {

    assert(array.length < numOutputPartitions)

    // by filling instead of allocating, we ensure that we'll never have
    // an NPE on read
    val outputArray = Array.fill(
      numOutputPartitions) { Seq.empty[Alignment] }

    array.foreach(p => {
      val (idx, reads) = p

      outputArray(idx - 1) = reads
    })

    outputArray
  }

  /**
   * Reconstructs the read pairs across partitions by moving reads around.
   *
   * @param rdd The RDD to move reads around within.
   * @return An RDD where the read pairs that are split across partition
   *   boundaries have been moved back into the correct split.
   */
  def apply(rdd: RDD[Alignment]): RDD[Alignment] = {

    val pairsAtStarts = rdd.mapPartitionsWithIndex(getPairAtStart(_, _))
      .collect

    val readsToAdd = unrollArray(pairsAtStarts, rdd.partitions.length)
    val readsToAddBcast = rdd.context.broadcast(readsToAdd)

    rdd.mapPartitionsWithIndex(addPairsAtEnd(_, _, readsToAddBcast.value))
  }
}
