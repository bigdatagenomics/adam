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

import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Reference }

class InnerShuffleRegionJoinSuite extends ADAMFunSuite {
  val partitionSize = 3
  var seqDict: SequenceDictionary = _

  before {
    seqDict = SequenceDictionary(
      SequenceRecord("chr1", 5, url = "test://chrom1"),
      SequenceRecord("chr2", 5, url = "tes=t://chrom2"))
  }

  sparkTest("Overlapping reference regions") {
    val reference = Reference.newBuilder
      .setName("chr1")
      .setLength(5L)
      .setSourceUri("test://chrom1")
      .build

    val built = AlignmentRecord.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = built
    val record2 = AlignmentRecord.newBuilder(built).setStart(3L).setEnd(4L).build()
    val baseRecord = AlignmentRecord.newBuilder(built).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord), 1).keyBy(ReferenceRegion.unstranded(_))
    val recordsRdd = sc.parallelize(Seq(record1, record2), 1).keyBy(ReferenceRegion.unstranded(_))

    assert(
      InnerShuffleRegionJoin[AlignmentRecord, AlignmentRecord](baseRdd, recordsRdd)
        .compute()
        .aggregate(true)(
          InnerShuffleRegionJoinSuite.merge,
          InnerShuffleRegionJoinSuite.and
        )
    )

    assert(
      InnerShuffleRegionJoin[AlignmentRecord, AlignmentRecord](baseRdd, recordsRdd)
        .compute()
        .count() === 2
    )
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

    val builtRef1 = AlignmentRecord.newBuilder()
      .setReferenceName(reference1.getName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()
    val builtRef2 = AlignmentRecord.newBuilder()
      .setReferenceName(reference2.getName)
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

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2), 1).keyBy(ReferenceRegion.unstranded(_))
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3), 1).keyBy(ReferenceRegion.unstranded(_))

    assert(
      InnerShuffleRegionJoin[AlignmentRecord, AlignmentRecord](baseRdd, recordsRdd)
        .compute()
        .aggregate(true)(
          InnerShuffleRegionJoinSuite.merge,
          InnerShuffleRegionJoinSuite.and
        )
    )

    assert({
      InnerShuffleRegionJoin[AlignmentRecord, AlignmentRecord](baseRdd, recordsRdd)
        .compute()
        .count() === 3
    }
    )
  }
}

object InnerShuffleRegionJoinSuite {
  def getReferenceRegion(record: AlignmentRecord): ReferenceRegion =
    ReferenceRegion.unstranded(record)

  def merge(prev: Boolean, next: (AlignmentRecord, AlignmentRecord)): Boolean =
    prev && getReferenceRegion(next._1).overlaps(getReferenceRegion(next._2))

  def count[T](prev: Int, next: (T, T)): Int =
    prev + 1

  def sum(value1: Int, value2: Int): Int =
    value1 + value2

  def and(value1: Boolean, value2: Boolean): Boolean =
    value1 && value2
}
