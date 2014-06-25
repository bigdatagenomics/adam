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
package org.bdgenomics.adam.algorithms.realignmenttarget

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.bdgenomics.formats.avro.{ ADAMRecord, ADAMPileup }
import scala.annotation.tailrec
import scala.collection.immutable.TreeSet
import org.bdgenomics.adam.rdd.ADAMContext._

object RealignmentTargetFinder {

  /**
   * Generates realignment targets from a set of reads.
   *
   * @param rdd RDD of reads to use in generating realignment targets.
   * @return Sorted set of realignment targets.
   */
  def apply(rdd: RDD[ADAMRecord]): TreeSet[IndelRealignmentTarget] = {
    new RealignmentTargetFinder().findTargets(rdd)
  }
}

class RealignmentTargetFinder extends Serializable with Logging {

  /**
   * Joins two sorted sets of targets together. Is tail call recursive.
   *
   * @param first A sorted set of realignment targets. This set must be ordered ahead of the second set.
   * @param second A sorted set of realignment targets.
   * @return A merged set of targets.
   */
  // TODO: it seems that the old way of merging allows for duplicate targets (say two copies of the same indel
  // that have been generated from two different reads that therefore have different read ranges)
  // That should be fixed now, see the change in merging.
  @tailrec protected final def joinTargets(
    first: TreeSet[IndelRealignmentTarget],
    second: TreeSet[IndelRealignmentTarget]): TreeSet[IndelRealignmentTarget] = {

    if (!first.isEmpty && second.isEmpty)
      first
    else if (first.isEmpty && !second.isEmpty)
      second
    else {
      // if the two sets overlap, we must merge their head and tail elements, else we can just blindly append
      if (!TargetOrdering.overlap(first.last, second.head)) {
        first.union(second)
      } else {
        // merge the tail of the first set and the head of the second set and retry the merge
        joinTargets(first - first.last + first.last.merge(second.head), second - second.head)
      }
    }
  }

  /**
   * Finds indel targets over a set of reads.
   *
   * @param reads An RDD containing reads to generate indel realignment targets from.
   * @return An ordered set of indel realignment targets.
   */
  def findTargets(reads: RDD[ADAMRecord]): TreeSet[IndelRealignmentTarget] = {

    // generate pileups from reads
    val rods: RDD[Iterable[ADAMPileup]] = reads.adamRecords2Pileup(true)
      .groupBy(_.getPosition).map(_._2)

    def createTreeSet(target: IndelRealignmentTarget): TreeSet[IndelRealignmentTarget] = {
      val tmp = new TreeSet()(TargetOrdering)
      tmp + target
    }

    /* for each rod, generate an indel realignment target. we then filter out all "empty" targets: these
     * are targets which do not show snp/indel evidence. we order these targets by reference position, and
     * merge targets who have overlapping positions
     */
    val targetSet = rods.map(IndelRealignmentTarget(_))
      .filter(!_.isEmpty)
      .keyBy(_.getSortKey())
      .sortByKey()
      .map(x => createTreeSet(x._2)).collect()
      .fold(new TreeSet()(TargetOrdering))(joinTargets)
    targetSet
  }

}
