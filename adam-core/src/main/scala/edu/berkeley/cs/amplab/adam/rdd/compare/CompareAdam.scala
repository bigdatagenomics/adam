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
package edu.berkeley.cs.amplab.adam.rdd.compare

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.adam.projections.Projection
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField._

import scala.collection._
import scala.Some
import edu.berkeley.cs.amplab.adam.models.SingleReadBucket

object CompareAdam extends Serializable {

  def compareADAM(sc: SparkContext, input1Path: String, input2Path: String,
                  predicateFactory: (Map[Int, Int]) => (SingleReadBucket, SingleReadBucket) => Boolean)
  : (ComparisonResult, ComparisonResult) = {

    val projection = Projection(
      referenceId,
      mateReferenceId,
      readMapped,
      mateMapped,
      readPaired,
      firstOfPair,
      primaryAlignment,
      referenceName,
      mateReference,
      start,
      mateAlignmentStart,
      cigar,
      readNegativeStrand,
      mateNegativeStrand,
      readName)

    val reads1: RDD[ADAMRecord] = sc.adamLoad(input1Path, projection = Some(projection))
    val reads2: RDD[ADAMRecord] = sc.adamLoad(input2Path, projection = Some(projection))

    val dict1 = sc.adamDictionaryLoad[ADAMRecord](input1Path)
    val dict2 = sc.adamDictionaryLoad[ADAMRecord](input2Path)

    val map12: Map[Int, Int] = dict1.mapTo(dict2)

    val predicate = predicateFactory(map12)

    val named1 = reads1.adamSingleReadBuckets().keyBy(_.allReads.head.getReadName)
    val named2 = reads2.adamSingleReadBuckets().keyBy(_.allReads.head.getReadName)

    val countMatch = named1.join(named2).filter {
      case (name, (bucket1, bucket2)) => predicate(bucket1, bucket2)
    }.count()

    // Used to filter the results of an outer-join
    def noPair[U](x: U): Boolean = {
      x match {
        case (name, (read, None)) => true
        case _ => false
      }
    }

    val numUnique1 = named1.leftOuterJoin(named2).filter(noPair).count()
    val numUnique2 = named2.leftOuterJoin(named1).filter(noPair).count()

    (ComparisonResult(named1.count(), numUnique1, countMatch),
      ComparisonResult(named2.count(), numUnique2, countMatch))
  }

  def samePairLocation(map: Map[Int, Int], read1: ADAMRecord, read2: ADAMRecord): Boolean =
    sameLocation(map, read1, read2) && sameMateLocation(map, read1, read2)

  def sameLocation(map: Map[Int, Int], read1: ADAMRecord, read2: ADAMRecord): Boolean = {
    assert(map.contains(read1.getReferenceId),
      "referenceId %d of read %s not in map %s".format(read1.getReferenceId.toInt, read1.getReadName, map))

    map(read1.getReferenceId) == read2.getReferenceId.toInt &&
      read1.getStart == read2.getStart && read1.getReadNegativeStrand == read2.getReadNegativeStrand
  }

  def sameMateLocation(map: Map[Int, Int], read1: ADAMRecord, read2: ADAMRecord): Boolean = {

    assert(map.contains(read1.getMateReferenceId),
      "mateReferenceId %d of read %s not in map %s".format(read1.getMateReferenceId.toInt, read1.getReadName, map))

    map(read1.getMateReferenceId) == read2.getMateReferenceId.toInt &&
      read1.getMateAlignmentStart == read2.getMateAlignmentStart &&
      read1.getMateNegativeStrand == read2.getMateNegativeStrand
  }

  def readLocationsMatchPredicate(map: Map[Int, Int])(bucket1: SingleReadBucket, bucket2: SingleReadBucket): Boolean = {

    val (paired1, single1) = bucket1.primaryMapped.partition(_.getReadPaired)
    val (paired2, single2) = bucket2.primaryMapped.partition(_.getReadPaired)

    if (single1.size != single2.size) return false
    if (single1.size == 1 && !sameLocation(map, single1.head, single2.head)) return false

    // TODO: if there are multiple primary hits for single-ended reads?

    val (firstPairs1, secondPairs1) = paired1.partition(_.getFirstOfPair)
    val (firstPairs2, secondPairs2) = paired2.partition(_.getFirstOfPair)

    if (firstPairs1.size != firstPairs2.size) return false
    if (firstPairs1.size == 1 && !samePairLocation(map, firstPairs1.head, firstPairs2.head)) return false

    // TODO: if there are multiple primary hits for paired-end reads?

    true
  }

}

case class ComparisonResult(total: Long, unique: Long, matching: Long) {}