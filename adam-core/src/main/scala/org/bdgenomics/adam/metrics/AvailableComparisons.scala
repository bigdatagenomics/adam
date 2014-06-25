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
package org.bdgenomics.adam.metrics

import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.projections.FieldValue
import org.bdgenomics.adam.projections.ADAMRecordField._
import org.bdgenomics.adam.rich.RichADAMRecord._
import org.bdgenomics.adam.util.Util._;
import scala.collection.Map
import org.bdgenomics.adam.models.ReadBucket

object DefaultComparisons {

  val comparisons: Seq[BucketComparisons[Any]] = Seq[BucketComparisons[Any]](
    OverMatched,
    DupeMismatch,
    MappedPosition,
    MapQualityScores,
    BaseQualityScores)

  private val map =
    comparisons.foldLeft(
      Map[String, BucketComparisons[Any]]())(
        (a: Map[String, BucketComparisons[Any]], b: BucketComparisons[Any]) => a + ((b.name, b)))

  def findComparison(k: String): BucketComparisons[Any] =
    map.getOrElse(k, throw new ArrayIndexOutOfBoundsException(
      String.format("Could not find comparison %s", k)))
}

object OverMatched extends BooleanComparisons with Serializable {
  val name = "overmatched"
  val description = "Checks that all buckets have exactly 0 or 1 records"

  def matches(records1: Iterable[ADAMRecord], records2: Iterable[ADAMRecord]): Boolean =
    records1.size == records2.size && (records1.size == 0 || records1.size == 1)

  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Seq[Boolean] =
    Seq(matches(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads) &&
      matches(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads) &&
      matches(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads) &&
      matches(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads) &&
      matches(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads))

  def schemas: Seq[FieldValue] = Seq()
}

object DupeMismatch extends PointComparisons with Serializable {
  val name = "dupemismatch"
  val description = "Counts the number of common reads marked as duplicates"

  def points(records1: Iterable[ADAMRecord], records2: Iterable[ADAMRecord]): Option[(Int, Int)] = {
    if (records1.size == records2.size) {
      records1.size match {
        case 0 => None
        case 1 => Some((if (records1.head.getDuplicateRead) 1 else 0, if (records2.head.getDuplicateRead) 1 else 0))
        case _ => None
      }
    } else None
  }

  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Seq[(Int, Int)] =
    Seq(
      points(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads),
      points(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads),
      points(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads),
      points(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads),
      points(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads)).flatten

  def schemas: Seq[FieldValue] = Seq(duplicateRead)
}

object MappedPosition extends LongComparisons with Serializable {
  val name = "positions"
  val description = "Counts how many reads align to the same genomic location"

  def distance(records1: Iterable[ADAMRecord], records2: Iterable[ADAMRecord]): Long = {
    if (records1.size == records2.size) records1.size match {
      case 0 => 0
      case 1 => {
        val r1 = records1.head
        val r2 = records2.head
        if (isSameContig(r1.getContig, r2.getContig)) {
          val start1 = r1.getStart
          val start2 = r2.getStart
          if (start1 > start2) start1 - start2 else start2 - start1
        } else -1
      }
      case _ => -1
    }
    else -1
  }

  /**
   * The records have been matched by their names, but the rest may be mismatched.
   */
  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Seq[Long] =
    Seq(distance(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads) +
      distance(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads) +
      distance(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads) +
      distance(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads) +
      distance(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads))

  def schemas: Seq[FieldValue] = Seq(
    start,
    firstOfPair)
}

object MapQualityScores extends PointComparisons with Serializable {
  val name = "mapqs"
  val description = "Creates scatter plot of mapping quality scores across identical reads"

  def points(records1: Iterable[ADAMRecord], records2: Iterable[ADAMRecord]): Option[(Int, Int)] = {
    if (records1.size == records2.size) {
      records1.size match {
        case 0 => None
        case 1 => Some((records1.head.getMapq.toInt, records2.head.getMapq.toInt))
        case _ => None
      }
    } else None
  }

  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Seq[(Int, Int)] =
    Seq(
      points(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads),
      points(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads),
      points(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads),
      points(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads),
      points(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads)).flatten

  def schemas: Seq[FieldValue] = Seq(mapq)
}

object BaseQualityScores extends PointComparisons with Serializable {
  val name = "baseqs"
  val description = "Creates scatter plots of base quality scores across identical positions in the same reads"

  def points(records1: Iterable[ADAMRecord], records2: Iterable[ADAMRecord]): Seq[(Int, Int)] = {
    if (records1.size == records2.size) {
      records1.size match {
        case 0 => Seq()
        case 1 => {
          val record1 = records1.head
          val record2 = records2.head
          record1.qualityScores
            .zip(record2.qualityScores)
            .map(b => (b._1.toInt, b._2.toInt))
        }
        case _ => Seq()
      }
    } else Seq()
  }

  def matchedByName(bucket1: ReadBucket, bucket2: ReadBucket): Seq[(Int, Int)] =
    points(bucket1.unpairedPrimaryMappedReads, bucket2.unpairedPrimaryMappedReads) ++
      points(bucket1.pairedFirstPrimaryMappedReads, bucket2.pairedFirstPrimaryMappedReads) ++
      points(bucket1.pairedSecondPrimaryMappedReads, bucket2.pairedSecondPrimaryMappedReads) ++
      points(bucket1.pairedFirstSecondaryMappedReads, bucket2.pairedFirstSecondaryMappedReads) ++
      points(bucket1.pairedSecondSecondaryMappedReads, bucket2.pairedSecondSecondaryMappedReads)

  def schemas: Seq[FieldValue] = Seq(qual)
}
