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
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Reference }

trait OuterRegionJoinSuite extends ADAMFunSuite {

  def runJoin(leftRdd: RDD[(ReferenceRegion, AlignmentRecord)],
              rightRdd: RDD[(ReferenceRegion, AlignmentRecord)]): RDD[(Option[AlignmentRecord], AlignmentRecord)]

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
    assert(jrdd.filter(_._1.isDefined).count === 1)
    assert(jrdd.filter(_._1.isDefined).first._1.get.getReadName() === "1")
    assert(jrdd.filter(_._1.isDefined).first._2.getReadName() === "2")
    assert(jrdd.filter(_._1.isEmpty).count === 1)
    assert(jrdd.filter(_._1.isEmpty).first._2.getReadName() === "4")
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
    assert(jrdd.filter(_._1.isDefined).count == 2)
    assert(jrdd.filter(_._1.isEmpty).count == 1)
  }
}
