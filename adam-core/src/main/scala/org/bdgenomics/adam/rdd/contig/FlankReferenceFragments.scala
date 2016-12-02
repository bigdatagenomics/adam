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
package org.bdgenomics.adam.rdd.contig

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.rdd.ReferencePartitioner
import org.bdgenomics.formats.avro.NucleotideContigFragment

/**
 * Object that extends all of the fragments in an RDD of contig fragments
 * with the sequence flanking said fragment.
 */
private[contig] object FlankReferenceFragments extends Serializable {

  /**
   * Adds flanks to sequence fragments in an RDD.
   *
   * Assumes that after sorting, all fragments are contiguous.
   *
   * @param rdd The RDD to flank.
   * @param sd The sequence dictionary describing all contigs in this sequence
   *   dictionary.
   * @param flankSize The size of flanking sequence to add to each fragment.
   * @return Returns a new RDD where each fragment has been extended with
   *   flanking sequence.
   */
  def apply(
    rdd: RDD[NucleotideContigFragment],
    sd: SequenceDictionary,
    flankSize: Int): RDD[NucleotideContigFragment] = {
    rdd.keyBy(ctg => ReferenceRegion(ctg).get)
      .repartitionAndSortWithinPartitions(ReferencePartitioner(sd))
      .mapPartitions(flank(_, flankSize))
  }

  def flank(
    iter: Iterator[(ReferenceRegion, NucleotideContigFragment)],
    flankSize: Int): Iterator[NucleotideContigFragment] = {
    // we need to have at least one element in the iterator
    if (iter.hasNext) {
      // now, we apply a window and flank adjacent segments
      var lastFragment = iter.next
      iter.map(f => {
        // grab temp copy; we will overwrite later
        val copyLastFragment = lastFragment

        // are the two fragments adjacent? if so, we must add the flanking sequences
        if (copyLastFragment._1.isAdjacent(f._1)) {
          val lastSequence = copyLastFragment._2.getFragmentSequence
          val currSequence = f._2.getFragmentSequence

          // update fragments with flanking sequences
          copyLastFragment._2.setFragmentSequence(lastSequence + currSequence.take(flankSize))
          copyLastFragment._2.setDescription(Option(copyLastFragment._2.getDescription)
            .fold("rr")(_ + "rr"))
          f._2.setFragmentSequence(lastSequence.takeRight(flankSize) + currSequence)
          f._2.setDescription("f")

          // we must change the start position of the fragment we are prepending to
          f._2.setFragmentStartPosition(f._2.getFragmentStartPosition - flankSize.toLong)
          // and the end position of the fragment we are appending to
          copyLastFragment._2.setFragmentEndPosition(
            copyLastFragment._2.getFragmentStartPosition + copyLastFragment._2.getFragmentSequence.length - 1L)
        }

        // overwrite last fragment
        lastFragment = f

        // emit updated last fragment
        copyLastFragment._2
      }) ++ Iterator(lastFragment._2)
    } else {
      Iterator()
    }
  }
}
