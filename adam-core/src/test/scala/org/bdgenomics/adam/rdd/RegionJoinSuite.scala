/*
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.models.{ SequenceRecord, SequenceDictionary, ReferenceRegion, ReferenceMapping }
import org.bdgenomics.adam.rich.ReferenceMappingContext._
import org.apache.spark.SparkContext._

class RegionJoinSuite extends SparkFunSuite {
  var seqDict: SequenceDictionary = _

  before {
    seqDict = SequenceDictionary(
      SequenceRecord(1, "1", 5, "test://chrom1"),
      SequenceRecord(2, "2", 5, "test://chrom2"))
  }

  test("alternating returns an alternating seq of items") {
    import NonoverlappingRegions._

    assert(alternating(Seq(), true) === Seq())
    assert(alternating(Seq(1), true) === Seq(1))
    assert(alternating(Seq(1, 2), true) === Seq(1))
    assert(alternating(Seq(1, 2, 3), true) === Seq(1, 3))
    assert(alternating(Seq(1, 2, 3, 4), true) === Seq(1, 3))
    assert(alternating(Seq(1, 2, 3, 4, 5), true) === Seq(1, 3, 5))

    assert(alternating(Seq(), false) === Seq())
    assert(alternating(Seq(1), false) === Seq())
    assert(alternating(Seq(1, 2), false) === Seq(2))
    assert(alternating(Seq(1, 2, 3), false) === Seq(2))
    assert(alternating(Seq(1, 2, 3, 4), false) === Seq(2, 4))
    assert(alternating(Seq(1, 2, 3, 4, 5), false) === Seq(2, 4))
    assert(alternating(Seq(1, 2, 3, 4, 5, 6), false) === Seq(2, 4, 6))
  }

  test("Single region returns itself") {
    val region = new ReferenceRegion(1, 1, 2)
    val regions = new NonoverlappingRegions(seqDict, Seq(region))
    val result = regions.findOverlappingRegions(region)
    assert(result.size === 1)
    assert(result.head === region)
  }

  test("Two adjacent regions will be merged") {
    val regions = new NonoverlappingRegions(seqDict, Seq(
      ReferenceRegion(1, 10, 20),
      ReferenceRegion(1, 20, 30)))

    assert(regions.endpoints === Array(10L, 30L))
  }

  test("Nonoverlapping regions will all be returned") {
    val region1 = new ReferenceRegion(1, 1, 2)
    val region2 = new ReferenceRegion(1, 3, 5)
    val testRegion3 = new ReferenceRegion(1, 1, 4)
    val testRegion1 = new ReferenceRegion(1, 4, 5)
    val regions = new NonoverlappingRegions(seqDict, Seq(region1, region2))

    // this should be 2, not 3, because binaryRegionSearch is (now) no longer returning
    // ReferenceRegions in which no original RR's were placed (i.e. the 'gaps').
    assert(regions.findOverlappingRegions(testRegion3).size === 2)

    assert(regions.findOverlappingRegions(testRegion1).size === 1)
  }

  test("Many overlapping regions will all be merged") {
    val region1 = new ReferenceRegion(1, 1, 3)
    val region2 = new ReferenceRegion(1, 2, 4)
    val region3 = new ReferenceRegion(1, 3, 5)
    val testRegion = new ReferenceRegion(1, 1, 4)
    val regions = new NonoverlappingRegions(seqDict, Seq(region1, region2, region3))
    assert(regions.findOverlappingRegions(testRegion).size === 1)
  }

  test("ADAMRecords return proper references") {
    val built = ADAMRecord.newBuilder()
      .setReferenceId(1)
      .setReferenceName("1")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom1")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .build()

    val record1 = built
    val record2 = ADAMRecord.newBuilder(built).setStart(3).build()
    val baseRecord = ADAMRecord.newBuilder(built).setCigar("4M").build()

    val baseMapping = new NonoverlappingRegions(seqDict, Seq(ADAMRecordReferenceMapping.getReferenceRegion(baseRecord)))
    val regions1 = baseMapping.findOverlappingRegions(ADAMRecordReferenceMapping.getReferenceRegion(record1))
    val regions2 = baseMapping.findOverlappingRegions(ADAMRecordReferenceMapping.getReferenceRegion(record2))
    assert(regions1.size === 1)
    assert(regions2.size === 1)
    assert(regions1.head === regions2.head)
  }

  sparkTest("Ensure same reference regions get passed together") {
    val builder = ADAMRecord.newBuilder()
      .setReferenceId(1)
      .setReferenceName("1")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom1")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")

    val record1 = builder.build()
    val record2 = builder.build()

    val rdd1 = sc.parallelize(Seq(record1))
    val rdd2 = sc.parallelize(Seq(record2))

    assert(RegionJoinSuite.getReferenceRegion(record1) ===
      RegionJoinSuite.getReferenceRegion(record2))

    assert(RegionJoin.partitionAndJoin[ADAMRecord, ADAMRecord](
      sc,
      seqDict,
      rdd1,
      rdd2).aggregate(true)(
        RegionJoinSuite.merge,
        RegionJoinSuite.and))

    assert(RegionJoin.partitionAndJoin[ADAMRecord, ADAMRecord](
      sc,
      seqDict,
      rdd1,
      rdd2)
      .aggregate(0)(
        RegionJoinSuite.count,
        RegionJoinSuite.sum) === 1)
  }

  sparkTest("Overlapping reference regions") {
    val built = ADAMRecord.newBuilder()
      .setReferenceId(1)
      .setReferenceName("1")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom1")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .build()

    val record1 = built
    val record2 = ADAMRecord.newBuilder(built).setStart(3).build()
    val baseRecord = ADAMRecord.newBuilder(built).setCigar("4M").build()

    val baseRdd = sc.parallelize(Seq(baseRecord))
    val recordsRdd = sc.parallelize(Seq(record1, record2))

    assert(RegionJoin.partitionAndJoin[ADAMRecord, ADAMRecord](
      sc,
      seqDict,
      baseRdd,
      recordsRdd)
      .aggregate(true)(
        RegionJoinSuite.merge,
        RegionJoinSuite.and))

    assert(RegionJoin.partitionAndJoin[ADAMRecord, ADAMRecord](
      sc,
      seqDict,
      baseRdd,
      recordsRdd).count() === 2)
  }

  sparkTest("Multiple reference regions do not throw exception") {
    val builtRef1 = ADAMRecord.newBuilder()
      .setReferenceId(1)
      .setReferenceName("1")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom1")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .build()
    val builtRef2 = ADAMRecord.newBuilder()
      .setReferenceId(2)
      .setReferenceName("2")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom2")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .build()

    val record1 = builtRef1
    val record2 = ADAMRecord.newBuilder(builtRef1).setStart(3).build()
    val record3 = builtRef2
    val baseRecord1 = ADAMRecord.newBuilder(builtRef1).setCigar("4M").build()
    val baseRecord2 = ADAMRecord.newBuilder(builtRef2).setCigar("4M").build()

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2))
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3))

    assert(RegionJoin.partitionAndJoin[ADAMRecord, ADAMRecord](
      sc,
      seqDict,
      baseRdd,
      recordsRdd)
      .aggregate(true)(
        RegionJoinSuite.merge,
        RegionJoinSuite.and))

    assert(RegionJoin.partitionAndJoin[ADAMRecord, ADAMRecord](
      sc,
      seqDict,
      baseRdd,
      recordsRdd).count() === 3)
  }

  sparkTest("regionJoin contains the same results as cartesianRegionJoin") {
    val builtRef1 = ADAMRecord.newBuilder()
      .setReferenceId(1)
      .setReferenceName("1")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom1")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .build()
    val builtRef2 = ADAMRecord.newBuilder()
      .setReferenceId(2)
      .setReferenceName("2")
      .setReferenceLength(5)
      .setReferenceUrl("test://chrom2")
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .build()

    val record1 = builtRef1
    val record2 = ADAMRecord.newBuilder(builtRef1).setStart(3).build()
    val record3 = builtRef2
    val baseRecord1 = ADAMRecord.newBuilder(builtRef1).setCigar("4M").build()
    val baseRecord2 = ADAMRecord.newBuilder(builtRef2).setCigar("4M").build()

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2))
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3))

    assert(RegionJoin.cartesianFilter(
      baseRdd,
      recordsRdd)
      .leftOuterJoin(
        RegionJoin.partitionAndJoin(
          sc,
          seqDict,
          baseRdd,
          recordsRdd))
      .filter({
        case ((_: ADAMRecord, (cartesian: ADAMRecord, region: Option[ADAMRecord]))) =>
          region match {
            case None => false
            case Some(record) => cartesian == record
          }
      })
      .count() === 3)
  }
}

object RegionJoinSuite {
  def getReferenceRegion[T](record: T)(implicit mapping: ReferenceMapping[T]): ReferenceRegion =
    mapping.getReferenceRegion(record)

  def merge(prev: Boolean, next: (ADAMRecord, ADAMRecord)): Boolean =
    prev && getReferenceRegion(next._1).overlaps(getReferenceRegion(next._2))

  def count[T](prev: Int, next: (T, T)): Int =
    prev + 1

  def sum(value1: Int, value2: Int): Int =
    value1 + value2

  def and(value1: Boolean, value2: Boolean): Boolean =
    value1 && value2
}
