/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget

import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.IndelRealignmentTarget._
import edu.berkeley.cs.amplab.adam.avro.{ADAMRecord,ADAMPileup}
import edu.berkeley.cs.amplab.adam.models.ADAMRod
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import org.apache.spark.{Logging, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import scala.collection.immutable.TreeSet

object RealignmentTargetFinder {

  /**
   * Generates realignment targets from a set of reads.
   *
   * @param rdd RDD of reads to use in generating realignment targets.
   * @return Sorted set of realignment targets.
   */
  def apply(rdd: RDD[RichADAMRecord], 
            maxIndelSize: Int = 500,
            maxTargetSize: Int = 3000): TreeSet[IndelRealignmentTarget] = {
    new RealignmentTargetFinder().findTargets(rdd, maxIndelSize, maxTargetSize).set
  }
}

class RealignmentTargetFinder extends Serializable with Logging {

  /**
   * Joins two sorted sets of targets together. Is tail call recursive.
   *
   * @note This function should not be called in a context where target set serialization is needed.
   * Instead, call joinTargets(TargetSet, TargetSet), which wraps this function.
   * 
   * @param first A sorted set of realignment targets. This set must be ordered ahead of the
   * second set.
   * @param second A sorted set of realignment targets.
   * @return A merged set of targets.
   */
  @tailrec protected final def joinTargets (
    first: TreeSet[IndelRealignmentTarget],
    second: TreeSet[IndelRealignmentTarget]): TreeSet[IndelRealignmentTarget] = {

    if (first.isEmpty && second.isEmpty) {
      TreeSet[IndelRealignmentTarget]()(TargetOrdering)
    } else if (second.isEmpty) {
      first
    } else if (first.isEmpty) {
      second
    } else {
      // if the two sets overlap, we must merge their head and tail elements,
      // else we can just blindly append
      if (!TargetOrdering.overlap(first.last, second.head)) {
        first.union(second)
      } else {
        // merge the tail of the first set and the head of the second set and retry the merge
        joinTargets(first - first.last + first.last.merge(second.head), second - second.head)
      }
    }
  }

  /**
   * Wrapper for joinTargets(TreeSet[IndelRealignmentTarget], TreeSet[IndelRealignmentTarget])
   * for contexts where serialization is needed.
   *
   * @param first A sorted set of realignment targets. This set must be ordered ahead of the
   * second set.
   * @param second A sorted set of realignment targets.
   * @return A merged set of targets. 
   */
  def joinTargets (first: TargetSet,
                   second: TargetSet): TargetSet = {
    new TargetSet(joinTargets (first.set, second.set))
  }

  /**
   * Finds indel targets over a set of reads.
   *
   * @param reads An RDD containing reads to generate indel realignment targets from.
   * @return An ordered set of indel realignment targets.
   */
  def findTargets (reads: RDD[RichADAMRecord],
                   maxIndelSize: Int = 500,
                   maxTargetSize: Int = 3000): TargetSet = {

    def createTargetSet(target: IndelRealignmentTarget) : TargetSet = {
      val tmp = new TreeSet()(TargetOrdering)
      new TargetSet(tmp + target)
    }

    /* for each rod, generate an indel realignment target. we then filter out all "empty" targets: these
     * are targets which do not show snp/indel evidence. we order these targets by reference position, and
     * merge targets who have overlapping positions
     */
    val targets = reads.flatMap(IndelRealignmentTarget(_, maxIndelSize))
      .filter(t => !t.isEmpty)

    val targetSet: TargetSet = TargetSet(targets.mapPartitions(iter => iter.toArray.sorted(TargetOrdering).toIterator)
      .map(createTargetSet)
      .fold(TargetSet())((t1: TargetSet, t2: TargetSet) => joinTargets(t1, t2))
      .set.filter(_.readRange.length <= maxTargetSize))

    targetSet
  }

}
