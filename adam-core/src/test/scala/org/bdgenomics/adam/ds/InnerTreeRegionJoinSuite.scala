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
package org.bdgenomics.adam.ds

import org.apache.spark.SparkContext._
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.ds.read.AlignmentArray
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ Alignment, Reference }
import org.bdgenomics.utils.interval.array.IntervalArray

class InnerTreeRegionJoinSuite extends ADAMFunSuite {

  sparkTest("Ensure same reference regions get passed together") {
    val reference = Reference.newBuilder
      .setName("chr1")
      .setLength(5L)
      .setSourceUri("test://chrom1")
      .build

    val builder = Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)

    val record1 = builder.build()
    val record2 = builder.build()

    val rdd1 = sc.parallelize(Seq(record1)).keyBy(ReferenceRegion.unstranded(_))
    val rdd2 = sc.parallelize(Seq(record2)).keyBy(ReferenceRegion.unstranded(_))

    assert(InnerTreeRegionJoinSuite.getReferenceRegion(record1) ===
      InnerTreeRegionJoinSuite.getReferenceRegion(record2))

    val tree = IntervalArray[ReferenceRegion, Alignment](rdd1,
      AlignmentArray.apply(_, _))

    assert(InnerTreeRegionJoin[Alignment, Alignment]().broadcastAndJoin(
      tree,
      rdd2).aggregate(true)(
        InnerTreeRegionJoinSuite.merge,
        InnerTreeRegionJoinSuite.and))

    assert(InnerTreeRegionJoin[Alignment, Alignment]().broadcastAndJoin(
      tree,
      rdd2)
      .aggregate(0)(
        InnerTreeRegionJoinSuite.count,
        InnerTreeRegionJoinSuite.sum) === 1)
  }

  sparkTest("Overlapping reference regions") {
    val reference = Reference.newBuilder
      .setName("chr1")
      .setLength(5L)
      .setSourceUri("test://chrom1")
      .build

    val built = Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = built
    val record2 = Alignment.newBuilder(built).setStart(3L).setEnd(4L).build()
    val baseRecord = Alignment.newBuilder(built).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord)).keyBy(ReferenceRegion.unstranded(_))
    val recordsRdd = sc.parallelize(Seq(record1, record2)).keyBy(ReferenceRegion.unstranded(_))

    val tree = IntervalArray[ReferenceRegion, Alignment](baseRdd,
      AlignmentArray.apply(_, _))

    assert(InnerTreeRegionJoin[Alignment, Alignment]().broadcastAndJoin(
      tree,
      recordsRdd)
      .aggregate(true)(
        InnerTreeRegionJoinSuite.merge,
        InnerTreeRegionJoinSuite.and))

    assert(InnerTreeRegionJoin[Alignment, Alignment]().broadcastAndJoin(
      tree,
      recordsRdd).count() === 2)
  }

  sparkTest("Multiple reference regions do not throw exception") {
    val reference1 = Reference.newBuilder
      .setName("chr1")
      .setLength(5L)
      .setSourceUri("test://chrom1")
      .build

    val reference2 = Reference.newBuilder
      .setName("chr2")
      .setLength(5L)
      .setSourceUri("test://chrom2")
      .build

    val builtRef1 = Alignment.newBuilder()
      .setReferenceName(reference1.getName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()
    val builtRef2 = Alignment.newBuilder()
      .setReferenceName(reference2.getName)
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = builtRef1
    val record2 = Alignment.newBuilder(builtRef1).setStart(3L).setEnd(4L).build()
    val record3 = builtRef2
    val baseRecord1 = Alignment.newBuilder(builtRef1).setCigar("4M").setEnd(5L).build()
    val baseRecord2 = Alignment.newBuilder(builtRef2).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2)).keyBy(ReferenceRegion.unstranded(_))
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3)).keyBy(ReferenceRegion.unstranded(_))

    val tree = IntervalArray[ReferenceRegion, Alignment](baseRdd,
      AlignmentArray.apply(_, _))

    assert(InnerTreeRegionJoin[Alignment, Alignment]().broadcastAndJoin(
      tree,
      recordsRdd)
      .aggregate(true)(
        InnerTreeRegionJoinSuite.merge,
        InnerTreeRegionJoinSuite.and))

    assert(InnerTreeRegionJoin[Alignment, Alignment]().broadcastAndJoin(
      tree,
      recordsRdd).count() === 3)
  }
}

object InnerTreeRegionJoinSuite {
  def getReferenceRegion(record: Alignment): ReferenceRegion =
    ReferenceRegion.unstranded(record)

  def merge(prev: Boolean, next: (Alignment, Alignment)): Boolean =
    prev && getReferenceRegion(next._1).overlaps(getReferenceRegion(next._2))

  def count[T](prev: Int, next: (T, T)): Int =
    prev + 1

  def sum(value1: Int, value2: Int): Int =
    value1 + value2

  def and(value1: Boolean, value2: Boolean): Boolean =
    value1 && value2
}
