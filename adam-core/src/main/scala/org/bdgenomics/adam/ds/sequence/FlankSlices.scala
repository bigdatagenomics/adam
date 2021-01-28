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
package org.bdgenomics.adam.ds.sequence

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.ds.ReferencePartitioner
import org.bdgenomics.formats.avro.Slice

/**
 * Object that extends all of the slices in an RDD of slices with
 * sequence flanking said slice.
 */
private[sequence] object FlankSlices extends Serializable {

  /**
   * Adds flanking sequence to slices in an RDD.
   *
   * Assumes that after sorting, all slices are contiguous.
   *
   * @param rdd The RDD to flank.
   * @param sd The sequence dictionary describing all contigs in this sequence
   *   dictionary.
   * @param flankSize The size of flanking sequence to add to each slice.
   * @return Returns a new RDD where each slice has been extended with
   *   flanking sequence.
   */
  def apply(
    rdd: RDD[Slice],
    sd: SequenceDictionary,
    flankSize: Int): RDD[Slice] = {
    rdd.keyBy(slice => ReferenceRegion(slice).get)
      .repartitionAndSortWithinPartitions(ReferencePartitioner(sd))
      .mapPartitions(flank(_, flankSize))
  }

  def flank(
    iter: Iterator[(ReferenceRegion, Slice)],
    flankSize: Int): Iterator[Slice] = {
    // we need to have at least one element in the iterator
    if (iter.hasNext) {
      // now, we apply a window and flank adjacent segments
      var lastSlice = iter.next
      iter.map(f => {
        // grab temp copy; we will overwrite later
        val copyLastSlice = lastSlice

        // are the two slices adjacent? if so, we must add the flanking sequences
        if (copyLastSlice._1.isAdjacent(f._1)) {
          val lastSequence = copyLastSlice._2.getSequence
          val currSequence = f._2.getSequence

          // update fragments with flanking sequences
          copyLastSlice._2.setSequence(lastSequence + currSequence.take(flankSize))
          copyLastSlice._2.setDescription(Option(copyLastSlice._2.getDescription)
            .fold("rr")(_ + "rr"))
          f._2.setSequence(lastSequence.takeRight(flankSize) + currSequence)
          f._2.setDescription("f")

          // we must change the start position of the fragment we are prepending to
          f._2.setStart(f._2.getStart - flankSize.toLong)
          // and the end position of the fragment we are appending to
          copyLastSlice._2.setEnd(
            copyLastSlice._2.getStart + copyLastSlice._2.getSequence.length - 1L)
        }

        // overwrite last fragment
        lastSlice = f

        // emit updated last fragment
        copyLastSlice._2
      }) ++ Iterator(lastSlice._2)
    } else {
      Iterator()
    }
  }
}
