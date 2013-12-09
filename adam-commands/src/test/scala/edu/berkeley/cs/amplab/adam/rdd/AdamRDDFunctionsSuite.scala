/**
 * Copyright 2013 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.rdd

import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, Base}
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.models.ADAMRod

class AdamRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("can convert pileups to rods, bases at different pos") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 2)
    assert(rods.filter(_.position == 0L).count === 1)
    assert(rods.filter(_.position == 0L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position == 1L).count === 1)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position == 1L).count === 1)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos, split by different sample") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.A)
      .setRecordGroupSample("0")
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.C)
      .setRecordGroupSample("1")
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position == 1L).count === 1)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
    assert(rods.filter(_.isSingleSample).count === 0)

    val split = rods.adamSplitRodsBySamples()

    assert(split.count === 2)
    assert(split.filter(_.position == 1L).count === 2)
    assert(split.filter(_.isSingleSample).count === 2)
  }

  sparkTest("can convert pileups to rods, bases at same pos, split by same sample") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.A)
      .setRecordGroupSample("1")
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.C)
      .setRecordGroupSample("1")
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position == 1L).count === 1)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
    assert(rods.filter(_.isSingleSample).count === 1)

    val split = rods.adamSplitRodsBySamples()

    assert(split.count === 1)
    assert(split.filter(_.isSingleSample).count === 1)
  }


  sparkTest("check coverage, bases at different pos") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
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
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val coverage = pileups.adamPileupsToRods(1)
      .adamRodCoverage()

    // floating point, so apply tolerance
    assert(coverage > 1.99 && coverage < 2.01)
  }

}
