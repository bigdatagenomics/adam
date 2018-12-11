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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Reference }

class LeftOuterShuffleRegionJoinAndGroupByLeftSuite(partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]]) extends ADAMFunSuite {

  val partitionSize = 3
  var seqDict: SequenceDictionary = _

  before {
    seqDict = SequenceDictionary(
      SequenceRecord("chr1", 15, url = "test://chrom1"),
      SequenceRecord("chr2", 15, url = "test://chrom2"))
  }

  def runJoin(leftRdd: RDD[(ReferenceRegion, AlignmentRecord)],
              rightRdd: RDD[(ReferenceRegion, AlignmentRecord)]): RDD[(AlignmentRecord, Iterable[AlignmentRecord])] = {
    LeftOuterShuffleRegionJoinAndGroupByLeft[AlignmentRecord, AlignmentRecord](rightRdd, leftRdd)
      .compute()
  }

  sparkTest("Ensure same reference regions get passed together") {
    val reference = Reference.newBuilder
      .setName("chr1")
      .setLength(15L)
      .setSourceUri("test://chrom1")
      .build

    val builder = AlignmentRecord.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)

    val record1 = builder.setReadName("1").build()
    val record2 = builder.setReadName("2").build()
    val record3 = builder.setReadName("3")
      .setStart(3L)
      .setEnd(5L)
      .build()
    val record4 = builder.setReadName("4")
      .setStart(10L)
      .setEnd(11L)
      .build()

    val rdd1 = sc.parallelize(Seq(record1, record3)).keyBy(ReferenceRegion.unstranded(_))
    val rdd2 = sc.parallelize(Seq(record2, record4)).keyBy(ReferenceRegion.unstranded(_))

    val jrdd = runJoin(rdd1, rdd2).cache()

    assert(jrdd.count === 2)
    assert(jrdd.filter(_._2.nonEmpty).count === 1)
    assert(jrdd.filter(_._2.nonEmpty).first._1.getReadName() === "1")
    assert(jrdd.filter(_._2.nonEmpty).first._2.head.getReadName() === "2")
    assert(jrdd.filter(_._2.isEmpty).count === 1)
    assert(jrdd.filter(_._2.isEmpty).first._2.head.getReadName() === "4")
  }

  sparkTest("Overlapping reference regions") {
    val reference = Reference.newBuilder
      .setName("chr1")
      .setLength(15L)
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
    val record3 = AlignmentRecord.newBuilder(built).setStart(6L).setEnd(7L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord)).keyBy(ReferenceRegion.unstranded(_))
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3)).keyBy(ReferenceRegion.unstranded(_))

    val jrdd = runJoin(baseRdd, recordsRdd).cache

    assert(jrdd.count() === 3)
    assert(jrdd.filter(_._2.isEmpty).count == 2)
    assert(jrdd.filter(_._2.nonEmpty).count == 1)
  }
}
