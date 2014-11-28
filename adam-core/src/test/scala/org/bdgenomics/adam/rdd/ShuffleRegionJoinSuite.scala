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

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class ShuffleRegionJoinSuite extends ADAMFunSuite {
  val partitionSize = 3
  var seqDict: SequenceDictionary = _

  before {
    seqDict = SequenceDictionary(
      SequenceRecord("chr1", 5, url = "test://chrom1"),
      SequenceRecord("chr2", 5, url = "tes=t://chrom2"))
  }

  sparkTest("Overlapping reference regions") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(5L)
      .setReferenceURL("test://chrom1")
      .build

    val built = AlignmentRecord.newBuilder()
      .setContig(contig)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = built
    val record2 = AlignmentRecord.newBuilder(built).setStart(3L).setEnd(4L).build()
    val baseRecord = AlignmentRecord.newBuilder(built).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord)).keyBy(ReferenceRegion(_).get)
    val recordsRdd = sc.parallelize(Seq(record1, record2)).keyBy(ReferenceRegion(_).get)

    assert(ShuffleRegionJoin.partitionAndJoin(
      baseRdd,
      recordsRdd,
      seqDict,
      partitionSize)
      .aggregate(true)(
        ShuffleRegionJoinSuite.merge,
        ShuffleRegionJoinSuite.and))

    assert(ShuffleRegionJoin.partitionAndJoin(
      baseRdd,
      recordsRdd,
      seqDict,
      partitionSize).count() === 2)
  }

  sparkTest("Multiple reference regions do not throw exception") {
    val contig1 = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(5L)
      .setReferenceURL("test://chrom1")
      .build

    val contig2 = Contig.newBuilder
      .setContigName("chr2")
      .setContigLength(5L)
      .setReferenceURL("test://chrom2")
      .build

    val builtRef1 = AlignmentRecord.newBuilder()
      .setContig(contig1)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()
    val builtRef2 = AlignmentRecord.newBuilder()
      .setContig(contig2)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = builtRef1
    val record2 = AlignmentRecord.newBuilder(builtRef1).setStart(3L).setEnd(4L).build()
    val record3 = builtRef2
    val baseRecord1 = AlignmentRecord.newBuilder(builtRef1).setCigar("4M").setEnd(5L).build()
    val baseRecord2 = AlignmentRecord.newBuilder(builtRef2).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2)).keyBy(ReferenceRegion(_).get)
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3)).keyBy(ReferenceRegion(_).get)

    assert(ShuffleRegionJoin.partitionAndJoin(
      baseRdd,
      recordsRdd,
      seqDict,
      partitionSize)
      .aggregate(true)(
        ShuffleRegionJoinSuite.merge,
        ShuffleRegionJoinSuite.and))

    assert(ShuffleRegionJoin.partitionAndJoin(
      baseRdd,
      recordsRdd,
      seqDict,
      partitionSize).count() === 3)
  }

  sparkTest("RegionJoin2 contains the same results as cartesianRegionJoin") {
    val contig1 = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(5L)
      .setReferenceURL("test://chrom1")
      .build

    val contig2 = Contig.newBuilder
      .setContigName("chr2")
      .setContigLength(5L)
      .setReferenceURL("test://chrom2")
      .build

    val builtRef1 = AlignmentRecord.newBuilder()
      .setContig(contig1)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()
    val builtRef2 = AlignmentRecord.newBuilder()
      .setContig(contig2)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = builtRef1
    val record2 = AlignmentRecord.newBuilder(builtRef1).setStart(3L).setEnd(4L).build()
    val record3 = builtRef2
    val baseRecord1 = AlignmentRecord.newBuilder(builtRef1).setCigar("4M").setEnd(5L).build()
    val baseRecord2 = AlignmentRecord.newBuilder(builtRef2).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2)).keyBy(ReferenceRegion(_).get)
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3)).keyBy(ReferenceRegion(_).get)

    assert(BroadcastRegionJoin.cartesianFilter(
      baseRdd,
      recordsRdd)
      .leftOuterJoin(
        ShuffleRegionJoin.partitionAndJoin(
          baseRdd,
          recordsRdd,
          seqDict,
          partitionSize))
      .filter({
        case ((_: AlignmentRecord, (cartesian: AlignmentRecord, region: Option[AlignmentRecord]))) =>
          region match {
            case None         => false
            case Some(record) => cartesian == record
          }
      })
      .count() === 3)
  }
}

object ShuffleRegionJoinSuite {
  def getReferenceRegion(record: AlignmentRecord): ReferenceRegion =
    ReferenceRegion(record).get

  def merge(prev: Boolean, next: (AlignmentRecord, AlignmentRecord)): Boolean =
    prev && getReferenceRegion(next._1).overlaps(getReferenceRegion(next._2))

  def count[T](prev: Int, next: (T, T)): Int =
    prev + 1

  def sum(value1: Int, value2: Int): Int =
    value1 + value2

  def and(value1: Boolean, value2: Boolean): Boolean =
    value1 && value2
}
