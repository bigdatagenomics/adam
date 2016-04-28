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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro._
import scala.annotation.tailrec

private[converters] object FragmentCollector extends Serializable {
  def apply(fragment: NucleotideContigFragment): (Contig, FragmentCollector) = {
    (
      fragment.getContig,
      FragmentCollector(Seq((ReferenceRegion(fragment).get, fragment.getFragmentSequence)))
    )
  }
}

case class FragmentCollector(fragments: Seq[(ReferenceRegion, String)])

object FragmentConverter extends Serializable {

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
          // we have sorted these fragments before we started, so the orientation is already known
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

  private[converters] def convertFragment(kv: (Contig, FragmentCollector)): Seq[AlignmentRecord] = {
    // extract kv pair
    val (contig, fragment) = kv

    // extract the fragment string and region
    fragment.fragments.map(p => {
      val (fragmentRegion, fragmentString) = p

      // build record
      AlignmentRecord.newBuilder()
        .setContigName(contig.getContigName)
        .setStart(fragmentRegion.start)
        .setEnd(fragmentRegion.end)
        .setSequence(fragmentString)
        .build()
    })
  }

  def convertRdd(rdd: RDD[NucleotideContigFragment]): RDD[AlignmentRecord] = {
    rdd.map(FragmentCollector(_))
      .reduceByKey(mergeFragments)
      .flatMap(convertFragment)
  }
}
