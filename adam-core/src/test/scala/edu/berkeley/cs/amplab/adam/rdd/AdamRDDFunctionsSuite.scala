/**
 * Copyright 2013 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.rdd

import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.{ADAMRecord, ADAMPileup, Base}
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import scala.util.Random

class AdamRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("can convert pileups to rods, bases at different pos, same reference") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setReferenceId(0)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 2)
    assert(rods.filter(_.position.pos == 0L).count === 1)
    assert(rods.filter(_.position.pos == 0L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos, different reference") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setReferenceId(0)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setReferenceId(1)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 2)
    assert(rods.filter(_.position.refId == 0).count === 1)
    assert(rods.filter(_.position.refId == 0).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.refId == 1).count === 1)
    assert(rods.filter(_.position.refId == 1).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos, split by different sample") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.A)
      .setRecordGroupSample("0")
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.C)
      .setRecordGroupSample("1")
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
    assert(rods.filter(_.isSingleSample).count === 0)

    val split = rods.adamSplitRodsBySamples()

    assert(split.count === 2)
    assert(split.filter(_.position.pos == 1L).count === 2)
    assert(split.filter(_.isSingleSample).count === 2)
  }

  sparkTest("can convert pileups to rods, bases at same pos, split by same sample") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.A)
      .setRecordGroupSample("1")
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.C)
      .setRecordGroupSample("1")
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
    assert(rods.filter(_.isSingleSample).count === 1)

    val split = rods.adamSplitRodsBySamples()

    assert(split.count === 1)
    assert(split.filter(_.isSingleSample).count === 1)
  }


  sparkTest("check coverage, bases at different pos") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setReferenceId(0)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val coverage = pileups.adamPileupsToRods(1)
      .adamRodCoverage()

    // floating point, so apply tolerance
    assert(coverage > 0.99 && coverage < 1.01)
  }

  sparkTest("check coverage, bases at same pos") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val coverage = pileups.adamPileupsToRods(1)
      .adamRodCoverage()

    // floating point, so apply tolerance
    assert(coverage > 1.99 && coverage < 2.01)
  }

  sparkTest("sorting reads") {
    val random = new Random("sorting".hashCode)
    val numReadsToCreate = 1000
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = ADAMRecord.newBuilder().setReadMapped(mapped)
      if (mapped) {
        builder.setReferenceId(random.nextInt(numReadsToCreate / 10)).setStart(random.nextInt(1000000))
      }
      builder.build()
    }
    val rdd = sc.parallelize(reads)
    val sortedReads = rdd.adamSortReadsByReferencePosition().collect().zipWithIndex
    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)
    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))
    // Make sure that we appropriately sorted the reads
    val expectedSortedReads = mapped.sortWith(
      (a, b) => a._1.getReferenceId < b._1.getReferenceId && a._1.getStart < b._1.getStart)
    assert(expectedSortedReads === mapped)
  }

  sparkTest("convert an RDD of reads into rods") {
    val r0 = ADAMRecord.newBuilder
      .setStart(1L)
      .setReferenceId(0)
      .setSequence("ACG")
      .setMapq(30)
      .setCigar("3M")
      .setMismatchingPositions("3")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setQual("!#$")
      .build()
    val r1 = ADAMRecord.newBuilder
      .setStart(2L)
      .setReferenceId(0)
      .setSequence("CG")
      .setMapq(40)
      .setCigar("2M")
      .setMismatchingPositions("2")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setQual("%&")
      .build()
    val r2 = ADAMRecord.newBuilder
      .setStart(3L)
      .setReferenceId(0)
      .setSequence("G")
      .setMapq(50)
      .setCigar("1M")
      .setMismatchingPositions("1")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setQual("%")
      .build()
    
    val reads = sc.parallelize(List(r0, r1, r2))

    val rods = reads.adamRecords2Rods()
    
    assert(rods.count === 3)
    assert(rods.collect.forall(_.position.refId == 0))
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).first.pileups.length === 1)
    assert(rods.filter(_.position.pos == 1L).first.pileups.forall(_.getReadBase == Base.A))
    assert(rods.filter(_.position.pos == 2L).count === 1)
    assert(rods.filter(_.position.pos == 2L).first.pileups.length === 2)
    assert(rods.filter(_.position.pos == 2L).first.pileups.forall(_.getReadBase == Base.C))
    assert(rods.filter(_.position.pos == 3L).count === 1)
    assert(rods.filter(_.position.pos == 3L).first.pileups.length === 3)
    assert(rods.filter(_.position.pos == 3L).first.pileups.forall(_.getReadBase == Base.G))
  }

  sparkTest("convert an RDD of reads into rods, different references") {
    val r0 = ADAMRecord.newBuilder
      .setStart(1L)
      .setReferenceId(0)
      .setSequence("AC")
      .setMapq(30)
      .setCigar("2M")
      .setMismatchingPositions("2")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setQual("!#$")
      .build()
    val r1 = ADAMRecord.newBuilder
      .setStart(2L)
      .setReferenceId(0)
      .setSequence("C")
      .setMapq(40)
      .setCigar("1M")
      .setMismatchingPositions("1")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setQual("%&")
      .build()
    val r2 = ADAMRecord.newBuilder
      .setStart(2L)
      .setReferenceId(1)
      .setSequence("G")
      .setMapq(50)
      .setCigar("1M")
      .setMismatchingPositions("1")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setQual("%")
      .build()
    
    val reads = sc.parallelize(List(r0, r1, r2))

    val rods = reads.adamRecords2Rods()
    
    assert(rods.count === 3)
    assert(rods.filter(_.position.refId == 0).count === 2)
    assert(rods.filter(_.position.refId == 1).count === 1)
    assert(rods.filter(_.position.pos == 1L).filter(_.position.refId == 0).count === 1)
    assert(rods.filter(_.position.pos == 1L).filter(_.position.refId == 0).first.pileups.length === 1)
    assert(rods.filter(_.position.pos == 1L).filter(_.position.refId == 0).first.pileups.forall(_.getReadBase == Base.A))
    assert(rods.filter(_.position.pos == 2L).filter(_.position.refId == 0).count === 1)
    assert(rods.filter(_.position.pos == 2L).filter(_.position.refId == 0).first.pileups.length === 2)
    assert(rods.filter(_.position.pos == 2L).filter(_.position.refId == 0).first.pileups.forall(_.getReadBase == Base.C))
    assert(rods.filter(_.position.pos == 2L).filter(_.position.refId == 1).count === 1)
    assert(rods.filter(_.position.pos == 2L).filter(_.position.refId == 1).first.pileups.length === 1)
    assert(rods.filter(_.position.pos == 2L).filter(_.position.refId == 1).first.pileups.forall(_.getReadBase == Base.G))
  }

}
