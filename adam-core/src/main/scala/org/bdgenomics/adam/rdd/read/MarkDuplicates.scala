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

import org.bdgenomics.utils.misc.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferencePosition
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }

private[rdd] object MarkDuplicates extends Serializable with Logging {

  private def markReadsInBucket(bucket: SingleReadBucket, primaryAreDups: Boolean, secondaryAreDups: Boolean) {
    bucket.primaryMapped.foreach(read => {
      read.setDuplicateRead(primaryAreDups)
    })
    bucket.secondaryMapped.foreach(read => {
      read.setDuplicateRead(secondaryAreDups)
    })
    bucket.unmapped.foreach(read => {
      read.setDuplicateRead(false)
    })
  }

  // Calculates the sum of the phred scores that are greater than or equal to 15
  def score(record: AlignmentRecord): Int = {
    record.qualityScores.filter(15 <=).sum
  }

  private def scoreBucket(bucket: SingleReadBucket): Int = {
    bucket.primaryMapped.map(score).sum
  }

  private def markReads(reads: Iterable[(ReferencePositionPair, SingleReadBucket)], areDups: Boolean) {
    markReads(reads, primaryAreDups = areDups, secondaryAreDups = areDups, ignore = None)
  }

  private def markReads(reads: Iterable[(ReferencePositionPair, SingleReadBucket)], primaryAreDups: Boolean, secondaryAreDups: Boolean,
                        ignore: Option[(ReferencePositionPair, SingleReadBucket)] = None) = MarkReads.time {
    reads.foreach(read => {
      if (ignore.forall(_ != read))
        markReadsInBucket(read._2, primaryAreDups, secondaryAreDups)
    })
  }

  def apply(rdd: AlignmentRecordRDD): RDD[AlignmentRecord] = {
    markBuckets(rdd.groupReadsByFragment(), rdd.recordGroups)
      .flatMap(_.allReads)
  }

  def apply(rdd: FragmentRDD): RDD[Fragment] = {
    markBuckets(rdd.rdd.map(f => SingleReadBucket(f)), rdd.recordGroups)
      .map(_.toFragment)
  }

  private def checkRecordGroups(recordGroups: RecordGroupDictionary) {
    // do we have record groups where the library name is not set? if so, print a warning message
    // to the user, as all record groups without a library name will be treated as coming from
    // a single library
    val emptyRgs = recordGroups.recordGroups
      .filter(_.library.isEmpty)

    emptyRgs.foreach(rg => {
      log.warn("Library ID is empty for record group %s from sample %s.".format(rg.recordGroupName,
        rg.sample))
    })

    if (emptyRgs.nonEmpty) {
      log.warn("For duplicate marking, all reads whose library is unknown will be treated as coming from the same library.")
    }
  }

  private def markBuckets(rdd: RDD[SingleReadBucket],
                          recordGroups: RecordGroupDictionary): RDD[SingleReadBucket] = {
    checkRecordGroups(recordGroups)

    // Gets the library of a SingleReadBucket
    def getLibrary(singleReadBucket: SingleReadBucket): Option[String] = {
      val recordGroupName = singleReadBucket.allReads.head.getRecordGroupName
      recordGroups.recordGroupMap.get(recordGroupName).flatMap(v => {
        val (recordGroup, index) = v
        recordGroup.library
      })
    }

    // Group by library and left position
    def leftPositionAndLibrary(p: (ReferencePositionPair, SingleReadBucket),
                               rgd: RecordGroupDictionary): (Option[ReferencePosition], Option[String]) = {
      val (refPosPair, singleReadBucket) = p
      (refPosPair.read1refPos, getLibrary(singleReadBucket))
    }

    // Group by right position
    def rightPosition(p: (ReferencePositionPair, SingleReadBucket)): Option[ReferencePosition] = {
      p._1.read2refPos
    }

    rdd.keyBy(ReferencePositionPair(_))
      .groupBy(leftPositionAndLibrary(_, recordGroups))
      .flatMap(kv => PerformDuplicateMarking.time {

        val leftPos: Option[ReferencePosition] = kv._1._1
        val readsAtLeftPos: Iterable[(ReferencePositionPair, SingleReadBucket)] = kv._2

        leftPos match {

          // These are all unmapped reads. There is no way to determine if they are duplicates
          case None =>
            markReads(readsAtLeftPos, areDups = false)

          // These reads have their left position mapped
          case Some(leftPosWithOrientation) =>

            val readsByRightPos = readsAtLeftPos.groupBy(rightPosition)

            val groupCount = readsByRightPos.size

            readsByRightPos.foreach(e => {

              val rightPos = e._1
              val reads = e._2

              val groupIsFragments = rightPos.isEmpty

              // We have no pairs (only fragments) if the current group is a group of fragments
              // and there is only one group in total
              val onlyFragments = groupIsFragments && groupCount == 1

              // If there are only fragments then score the fragments. Otherwise, if there are not only
              // fragments (there are pairs as well) mark all fragments as duplicates.
              // If the group does not contain fragments (it contains pairs) then always score it.
              if (onlyFragments || !groupIsFragments) {
                // Find the highest-scoring read and mark it as not a duplicate. Mark all the other reads in this group as duplicates.
                val highestScoringRead = reads.max(ScoreOrdering)
                markReadsInBucket(highestScoringRead._2, primaryAreDups = false, secondaryAreDups = true)
                markReads(reads, primaryAreDups = true, secondaryAreDups = true, ignore = Some(highestScoringRead))
              } else {
                markReads(reads, areDups = true)
              }
            })
        }

        readsAtLeftPos.map(_._2)
      })
  }

  private object ScoreOrdering extends Ordering[(ReferencePositionPair, SingleReadBucket)] {
    override def compare(x: (ReferencePositionPair, SingleReadBucket), y: (ReferencePositionPair, SingleReadBucket)): Int = {
      // This is safe because scores are Ints
      scoreBucket(x._2) - scoreBucket(y._2)
    }
  }
}
