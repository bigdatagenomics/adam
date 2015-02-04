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

import PairingRDD._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.ADAMFunSuite

class PairingRDDSuite extends ADAMFunSuite {

  sparkTest("sliding on an empty RDD returns an empty RDD") {
    val rdd: RDD[Int] = sc.emptyRDD
    assert(rdd.sliding(2).collect() === Array())
  }

  sparkTest("sliding on an RDD where count() < width returns an empty RDD") {
    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 3))
    assert(rdd.sliding(4).collect() === Array())
  }

  sparkTest("sliding on an RDD where count() == width returns an RDD with one element.") {
    val rdd: RDD[Int] = sc.parallelize(Seq(1, 2, 3))
    assert(rdd.sliding(3).count() === 1)
  }

  sparkTest("sliding on a small RDD works correctly") {
    val seq: Seq[String] = Seq("1_one", "2_two", "3_three", "4_four", "5_five", "6_six", "7_seven")
    val rdd: RDD[String] = sc.parallelize(seq)
    val windows: RDD[Seq[String]] = rdd.sliding(3).sortBy(k => k(0))

    assert(windows.collect() === seq.sliding(3).toArray)
  }

  sparkTest("sliding works correctly on a partitioned RDD") {
    val seq: Seq[Int] = 0 until 1000
    val rdd: RDD[Int] = sc.parallelize(seq).repartition(7)
    val windows: RDD[Seq[Int]] = rdd.sliding(5).sortBy(k => k(0))

    assert(windows.collect() === seq.sliding(5).toArray)
  }

  sparkTest("pairing a simple sequence works") {
    val seq: Seq[Int] = Seq(5, 9, 23, 1, 2)
    val seqPaired = seq.sorted.sliding(2).map(s => (s(0), s(1))).toArray

    val rdd: RDD[Int] = sc.parallelize(seq)
    val rddPaired = rdd.pair().collect().toArray

    assert(seqPaired === rddPaired)
  }

  sparkTest("pairing an empty sequence returns an empty sequence") {
    val rdd: RDD[Int] = sc.emptyRDD
    assert(rdd.pair().collect() === Array())
  }

  sparkTest("pairing a sorted sequence works") {
    val seq: Seq[Int] = Seq(5, 9, 23, 1, 2)
    val seqPaired = seq.sorted.sliding(2).map(s => (s(0), s(1))).toArray

    val rdd: RDD[Int] = sc.parallelize(seq)
    val rddPaired = rdd.pair().collect().toArray

    assert(seqPaired === rddPaired)
  }

  sparkTest("pairWithEnds on an empty sequence returns an empty sequence") {
    val rdd: RDD[Int] = sc.emptyRDD
    assert(rdd.pairWithEnds().collect() === Array())
  }

  sparkTest("pairWithEnds gives us the right number and set of values") {
    val seq: Seq[Int] = Seq(5, 9, 23, 1, 2)
    val rdd: RDD[Int] = sc.parallelize(seq)
    val rddPaired = rdd.pairWithEnds().collect().toArray

    assert(rddPaired === Seq(
      None -> Some(1),
      Some(1) -> Some(2),
      Some(2) -> Some(5),
      Some(5) -> Some(9),
      Some(9) -> Some(23),
      Some(23) -> None))
  }
}
