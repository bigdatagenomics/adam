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

import org.apache.spark.SparkContext._
import org.bdgenomics.adam.models.{ ReferenceMapping, ReferenceRegion }
import org.bdgenomics.adam.rich.ReferenceMappingContext._
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class RegionJoinSuite extends SparkFunSuite {

  test("alternating returns an alternating seq of items") {
    import org.bdgenomics.adam.rdd.NonoverlappingRegions._

    assert(alternating(Seq(), includeFirst = true) === Seq())
    assert(alternating(Seq(1), includeFirst = true) === Seq(1))
    assert(alternating(Seq(1, 2), includeFirst = true) === Seq(1))
    assert(alternating(Seq(1, 2, 3), includeFirst = true) === Seq(1, 3))
    assert(alternating(Seq(1, 2, 3, 4), includeFirst = true) === Seq(1, 3))
    assert(alternating(Seq(1, 2, 3, 4, 5), includeFirst = true) === Seq(1, 3, 5))

    assert(alternating(Seq(), includeFirst = false) === Seq())
    assert(alternating(Seq(1), includeFirst = false) === Seq())
    assert(alternating(Seq(1, 2), includeFirst = false) === Seq(2))
    assert(alternating(Seq(1, 2, 3), includeFirst = false) === Seq(2))
    assert(alternating(Seq(1, 2, 3, 4), includeFirst = false) === Seq(2, 4))
    assert(alternating(Seq(1, 2, 3, 4, 5), includeFirst = false) === Seq(2, 4))
    assert(alternating(Seq(1, 2, 3, 4, 5, 6), includeFirst = false) === Seq(2, 4, 6))
  }

  test("Single region returns itself") {
    val region = new ReferenceRegion("chr1", 1, 2)
    val regions = new NonoverlappingRegions(Seq(region))
    val result = regions.findOverlappingRegions(region)
    assert(result.size === 1)
    assert(result.head === region)
  }

  test("Two adjacent regions will be merged") {
    val regions = new NonoverlappingRegions(Seq(
      ReferenceRegion("chr1", 10, 20),
      ReferenceRegion("chr1", 20, 30)))

    assert(regions.endpoints === Array(10L, 30L))
  }

  test("Nonoverlapping regions will all be returned") {
    val region1 = new ReferenceRegion("chr1", 1, 2)
    val region2 = new ReferenceRegion("chr1", 3, 5)
    val testRegion3 = new ReferenceRegion("chr1", 1, 4)
    val testRegion1 = new ReferenceRegion("chr1", 4, 5)
    val regions = new NonoverlappingRegions(Seq(region1, region2))

    // this should be 2, not 3, because binaryRegionSearch is (now) no longer returning
    // ReferenceRegions in which no original RR's were placed (i.e. the 'gaps').
    assert(regions.findOverlappingRegions(testRegion3).size === 2)

    assert(regions.findOverlappingRegions(testRegion1).size === 1)
  }

  test("Many overlapping regions will all be merged") {
    val region1 = new ReferenceRegion("chr1", 1, 3)
    val region2 = new ReferenceRegion("chr1", 2, 4)
    val region3 = new ReferenceRegion("chr1", 3, 5)
    val testRegion = new ReferenceRegion("chr1", 1, 4)
    val regions = new NonoverlappingRegions(Seq(region1, region2, region3))
    assert(regions.findOverlappingRegions(testRegion).size === 1)
  }

  test("ADAMRecords return proper references") {
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

    val baseMapping = new NonoverlappingRegions(Seq(AlignmentRecordReferenceMapping.getReferenceRegion(baseRecord)))
    val regions1 = baseMapping.findOverlappingRegions(AlignmentRecordReferenceMapping.getReferenceRegion(record1))
    val regions2 = baseMapping.findOverlappingRegions(AlignmentRecordReferenceMapping.getReferenceRegion(record2))
    assert(regions1.size === 1)
    assert(regions2.size === 1)
    assert(regions1.head === regions2.head)
  }

  sparkTest("Ensure same reference regions get passed together") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(5L)
      .setReferenceURL("test://chrom1")
      .build

    val builder = AlignmentRecord.newBuilder()
      .setContig(contig)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)

    val record1 = builder.build()
    val record2 = builder.build()

    val rdd1 = sc.parallelize(Seq(record1))
    val rdd2 = sc.parallelize(Seq(record2))

    assert(RegionJoinSuite.getReferenceRegion(record1) ===
      RegionJoinSuite.getReferenceRegion(record2))

    assert(RegionJoin.partitionAndJoin[AlignmentRecord, AlignmentRecord](
      sc,
      rdd1,
      rdd2).aggregate(true)(
        RegionJoinSuite.merge,
        RegionJoinSuite.and))

    assert(RegionJoin.partitionAndJoin[AlignmentRecord, AlignmentRecord](
      sc,
      rdd1,
      rdd2)
      .aggregate(0)(
        RegionJoinSuite.count,
        RegionJoinSuite.sum) === 1)
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

    val baseRdd = sc.parallelize(Seq(baseRecord))
    val recordsRdd = sc.parallelize(Seq(record1, record2))

    assert(RegionJoin.partitionAndJoin[AlignmentRecord, AlignmentRecord](
      sc,
      baseRdd,
      recordsRdd)
      .aggregate(true)(
        RegionJoinSuite.merge,
        RegionJoinSuite.and))

    assert(RegionJoin.partitionAndJoin[AlignmentRecord, AlignmentRecord](
      sc,
      baseRdd,
      recordsRdd).count() === 2)
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
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = builtRef1
    val record2 = AlignmentRecord.newBuilder(builtRef1).setStart(3L).setEnd(4L).build()
    val record3 = builtRef2
    val baseRecord1 = AlignmentRecord.newBuilder(builtRef1).setCigar("4M").setEnd(5L).build()
    val baseRecord2 = AlignmentRecord.newBuilder(builtRef2).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2))
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3))

    assert(RegionJoin.partitionAndJoin[AlignmentRecord, AlignmentRecord](
      sc,
      baseRdd,
      recordsRdd)
      .aggregate(true)(
        RegionJoinSuite.merge,
        RegionJoinSuite.and))

    assert(RegionJoin.partitionAndJoin[AlignmentRecord, AlignmentRecord](
      sc,
      baseRdd,
      recordsRdd).count() === 3)
  }

  sparkTest("regionJoin contains the same results as cartesianRegionJoin") {
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

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2))
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3))

    assert(RegionJoin.cartesianFilter(
      baseRdd,
      recordsRdd)
      .leftOuterJoin(
        RegionJoin.partitionAndJoin(
          sc,
          baseRdd,
          recordsRdd))
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

object RegionJoinSuite {
  def getReferenceRegion[T](record: T)(implicit mapping: ReferenceMapping[T]): ReferenceRegion =
    mapping.getReferenceRegion(record)

  def merge(prev: Boolean, next: (AlignmentRecord, AlignmentRecord)): Boolean =
    prev && getReferenceRegion(next._1).overlaps(getReferenceRegion(next._2))

  def count[T](prev: Int, next: (T, T)): Int =
    prev + 1

  def sum(value1: Int, value2: Int): Int =
    value1 + value2

  def and(value1: Boolean, value2: Boolean): Boolean =
    value1 && value2
}
