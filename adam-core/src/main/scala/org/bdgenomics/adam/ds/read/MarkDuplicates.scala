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

import grizzled.slf4j.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReadGroupDictionary, ReferencePosition }
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.fragment.FragmentDataset
import org.bdgenomics.formats.avro.{ Alignment, Fragment, Strand }

private[ds] object MarkDuplicates extends Serializable with Logging {

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
  def score(record: Alignment): Int = {
    record.qualityScoreValues.filter(15 <=).sum
  }

  private def scoreBucket(bucket: SingleReadBucket): Int = {
    bucket.primaryMapped.filter(r => !r.getSupplementaryAlignment).map(score).sum
  }

  private def markReads(reads: Iterable[(ReferencePositionPair, SingleReadBucket)], areDups: Boolean) {
    markReads(reads, primaryAreDups = areDups, secondaryAreDups = areDups, ignore = None)
  }

  private def markReads(reads: Iterable[(ReferencePositionPair, SingleReadBucket)], primaryAreDups: Boolean, secondaryAreDups: Boolean,
                        ignore: Option[(ReferencePositionPair, SingleReadBucket)] = None) = {
    reads.foreach(read => {
      if (ignore.forall(_ != read))
        markReadsInBucket(read._2, primaryAreDups, secondaryAreDups)
    })
  }

  def apply(rdd: AlignmentDataset): RDD[Alignment] = {

    markBuckets(rdd.groupReadsByFragment(), rdd.readGroups)
      .flatMap(_.allReads)
  }

  def apply(rdd: FragmentDataset): RDD[Fragment] = {

    markBuckets(rdd.rdd.map(f => SingleReadBucket(f)), rdd.readGroups)
      .map(_.toFragment)
  }

  private def checkReadGroups(readGroups: ReadGroupDictionary) {
    // do we have read groups where the library name is not set? if so, print a warning message
    // to the user, as all read groups without a library name will be treated as coming from
    // a single library
    val emptyRgs = readGroups.readGroups
      .filter(_.library.isEmpty)

    emptyRgs.foreach(rg => {
      warn("Library ID is empty for read group %s from sample %s.".format(rg.id,
        rg.sampleId))
    })

    if (emptyRgs.nonEmpty) {
      warn("For duplicate marking, all reads whose library is unknown will be treated as coming from the same library.")
    }
  }

  private def markBuckets(rdd: RDD[SingleReadBucket],
                          readGroups: ReadGroupDictionary): RDD[SingleReadBucket] = {
    checkReadGroups(readGroups)

    def positionForStrand(defaultPos: Option[ReferencePosition], otherPos: Option[ReferencePosition], strand: Strand) = {
      defaultPos.filter(_.strand == strand).orElse(otherPos.filter(_.strand == strand).orElse(defaultPos))
    }

    // Group by library and left position
    def leftPositionAndLibrary(p: (ReferencePositionPair, SingleReadBucket),
                               rgd: ReadGroupDictionary): (Option[ReferencePosition], String) = {
      val leftPosition = positionForStrand(p._1.read1refPos, p._1.read2refPos, Strand.FORWARD)
      if (p._2.allReads.head.getReadGroupId != null) {
        (leftPosition, rgd(p._2.allReads.head.getReadGroupId).library.orNull)
      } else {
        (leftPosition, null)
      }
    }

    // Group by right position
    def rightPosition(p: (ReferencePositionPair, SingleReadBucket)): Option[ReferencePosition] = {
      positionForStrand(p._1.read2refPos, p._1.read1refPos, Strand.REVERSE)
    }

    rdd.keyBy(ReferencePositionPair(_))
      .groupBy(leftPositionAndLibrary(_, readGroups))
      .flatMap(kv => {

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
