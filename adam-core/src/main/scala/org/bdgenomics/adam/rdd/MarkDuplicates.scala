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
package org.bdgenomics.adam.rdd

import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.{ SingleReadBucket, ReferencePositionPair, ReferencePositionWithOrientation }
import org.apache.spark.rdd.RDD

private[rdd] object MarkDuplicates extends Serializable {

  def markReads(buckets: Seq[SingleReadBucket], areDups: Boolean): Seq[SingleReadBucket] = {
    for (bucket <- buckets; read <- bucket.primaryMapped ++ bucket.secondaryMapped) {
      read.setDuplicateRead(areDups)
    }
    for (bucket <- buckets; read <- bucket.unmapped) {
      read.setDuplicateRead(false)
    }
    buckets
  }

  // Calculates the sum of the phred scores that are greater than or equal to 15
  def score(record: ADAMRecord): Int = {
    record.qualityScores.filter(15 <=).sum
  }

  def scoreAndMarkReads(buckets: Seq[SingleReadBucket]): Seq[SingleReadBucket] = {
    val scoredBuckets = buckets.map(p => (p.primaryMapped.map(score).sum, p))
    val sortedBuckets = scoredBuckets.sortBy(_._1)(Ordering[Int].reverse)

    for (((score, bucket), i) <- sortedBuckets.zipWithIndex) {
      for (read <- bucket.primaryMapped) {
        read.setDuplicateRead(i != 0)
      }
      for (read <- bucket.secondaryMapped) {
        read.setDuplicateRead(true)
      }
      for (read <- bucket.unmapped) {
        read.setDuplicateRead(false)
      }
    }
    buckets
  }

  def apply(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    // Group by library and left position
    def leftPositionAndLibrary(p: (ReferencePositionPair, SingleReadBucket)): (Option[ReferencePositionWithOrientation], CharSequence) = {
      (p._1.read1refPos, p._2.allReads.head.getRecordGroupLibrary)
    }

    // Group by right position
    def rightPosition(p: (ReferencePositionPair, SingleReadBucket)): Option[ReferencePositionWithOrientation] = {
      p._1.read2refPos
    }

    rdd.adamSingleReadBuckets().keyBy(ReferencePositionPair(_)).groupBy(leftPositionAndLibrary)
      .flatMap(kv => {
        val ((leftPos, library), readsByLeftPos) = kv

        val buckets = leftPos match {
          // These are all unmapped reads. There is no way to determine if they are duplicates
          case None =>
            markReads(readsByLeftPos.toSeq.unzip._2, areDups = false)

          // These reads have their left position mapped
          case Some(leftPosWithOrientation) =>
            // Group the reads by their right position
            val readsByRightPos = readsByLeftPos.groupBy(rightPosition)
            // Find any reads with no right position
            val fragments = readsByRightPos.get(None)
            // Check if we have any pairs (reads with a right position)
            val hasPairs = readsByRightPos.keys.exists(_.isDefined)

            if (hasPairs) {
              // Since we have pairs, mark all fragments as duplicates
              val processedFrags = if (fragments.isDefined) {
                markReads(fragments.get.toSeq.unzip._2, areDups = true)
              } else {
                Seq.empty
              }

              val processedPairs = for (
                buckets <- (readsByRightPos - None).values;
                processedPair <- scoreAndMarkReads(buckets.toSeq.unzip._2)
              ) yield processedPair

              processedPairs ++ processedFrags

            } else if (fragments.isDefined) {
              // No pairs. Score the fragments.
              scoreAndMarkReads(fragments.get.toSeq.unzip._2)
            } else {
              Seq.empty
            }
        }

        buckets.flatMap(_.allReads)
      })
  }
}

