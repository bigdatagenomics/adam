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
package org.bdgenomics.adam.converters

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro._
import scala.annotation.tailrec

/**
 * Singleton object for creating FragmentCollector instances.
 */
private object FragmentCollector extends Serializable {

  /**
   * Apply method to create a fragment collector that is keyed by the contig.
   *
   * @param fragment Fragment of a reference/assembled sequence to wrap.
   * @return Returns key value pair where the key is the contig metadata and the
   *   value is a Fragment Collector object.
   */
  def apply(fragment: NucleotideContigFragment): Option[(String, FragmentCollector)] = {
    ReferenceRegion(fragment).map(rr => {
      (fragment.getContigName,
        FragmentCollector(Seq((rr, fragment.getSequence))))
    })
  }
}

/**
 * A case class used for merging Fragments.
 *
 * @param fragments A seq where sequence fragments are keyed with the genomic
 *   interval they represent.
 */
private[adam] case class FragmentCollector(fragments: Seq[(ReferenceRegion, String)])

/**
 * Object used to convert sequence assemblies into unaligned reads.
 */
private[adam] object FragmentConverter extends Serializable {

  /**
   * Merges the sequences collected in two FragmentCollector instances.
   *
   * Merges sequences together by joining the sequences embedded in two
   * FragmentCollectors together, sorting, and then reducing all of the
   * sequences down. Checks to ensure that all sequences are adjacent
   * before reducing.
   *
   * @param f1 First collector to merge.
   * @param f2 Second collector to merge.
   * @return Returns the merger of two FragmentCollectors.
   */
  private def mergeFragments(
    f1: FragmentCollector,
    f2: FragmentCollector): FragmentCollector = {
    assert(!(f1.fragments.isEmpty || f2.fragments.isEmpty))

    // join fragments from each and sort
    val fragments = (f1.fragments ++ f2.fragments).sortBy(_._1)

    var fragmentList = List[(ReferenceRegion, String)]()

    @tailrec def fragmentCombiner(
      lastFragment: (ReferenceRegion, String),
      iter: Iterator[(ReferenceRegion, String)]) {
      if (!iter.hasNext) {
        // prepend fragment to list
        fragmentList = lastFragment :: fragmentList
      } else {
        // extract our next fragment, and our last fragment
        val (lastRegion, lastString) = lastFragment
        val (thisRegion, thisString) = iter.next

        // are our fragments adjacent?
        val newLastFragment = if (lastRegion.isAdjacent(thisRegion)) {
          // if they are, merge the fragments
          // we have sorted these fragments before we started, so the strand is already known
          (lastRegion.hull(thisRegion), lastString + thisString)
        } else {
          // if they aren't, prepend the last fragment to the list
          // and use the current fragment as the new last fragment
          fragmentList = lastFragment :: fragmentList
          (thisRegion, thisString)
        }

        // recurse
        fragmentCombiner(newLastFragment, iter)
      }
    }

    // convert to an iterator and peek at the first element and recurse
    val fragmentIter = fragments.toIterator
    fragmentCombiner(fragmentIter.next, fragmentIter)

    // create a new collector
    FragmentCollector(fragmentList.toSeq)
  }

  /**
   * Converts a reference assembly into one or more reads.
   *
   * Takes in a reduced key value pair containing merged FragmentCollectors.
   * The strings that are in this FragmentCollector are used to create reads.
   * The reference key is used to populate the metadata for the reference.
   *
   * @param kv (Reference metadata, FragmentCollector) key value pair.
   * @return Returns one alignment record per sequence in the collector.
   */
  private[converters] def convertFragment(kv: (String, FragmentCollector)): Seq[AlignmentRecord] = {
    // extract kv pair
    val (referenceName, fragment) = kv

    // extract the fragment string and region
    fragment.fragments.map(p => {
      val (fragmentRegion, fragmentString) = p

      // build record
      AlignmentRecord.newBuilder()
        .setReferenceName(referenceName)
        .setStart(fragmentRegion.start)
        .setEnd(fragmentRegion.end)
        .setSequence(fragmentString)
        .build()
    })
  }

  /**
   * Converts an RDD of NucleotideContigFragments into AlignmentRecords.
   *
   * Produces one alignment record per contiguous sequence contained in the
   * input RDD. Fragments are merged down to the longest contiguous chunks
   * possible.
   *
   * @param rdd RDD of assembled sequences.
   * @return Returns an RDD of reads that represent aligned contigs.
   */
  def convertRdd(rdd: RDD[NucleotideContigFragment]): RDD[AlignmentRecord] = {
    rdd.flatMap(FragmentCollector(_))
      .reduceByKey(mergeFragments)
      .flatMap(convertFragment)
  }
}
