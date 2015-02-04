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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.util.ADAMFunSuite

import scala.reflect.ClassTag

class CoverageSuite extends ADAMFunSuite {

  private def region(start: Long, end: Long) = ReferenceRegion("seq", start, end)

  /*
  Tests for local coverage calculations
   */

  test("regionToWindows") {
    val c = new Coverage(100L)
    val ref = region(10, 50)
    val w0 = region(0, 100)
    assert(c.regionToWindows(ref) === Seq((w0, ref)))

    val w1 = region(100, 200)
    val r2 = region(50, 150)
    assert(c.regionToWindows(r2) == Seq((w0, region(50, 100)), (w1, region(100, 150))))
  }

  test("calculate empty coverage") {
    val c = new Coverage(100L)
    assert(c.calculateCoverageRegions(Seq()).toList === Seq())
  }

  test("calculate coverage of one region") {
    val c = new Coverage(100L)
    assert(c.calculateCoverageRegions(Seq(region(10, 50))).toList ===
      Seq(region(10, 50)))
  }

  test("calculate coverage of two regions") {
    val c = new Coverage(100L)
    assert(c.calculateCoverageRegions(Seq(region(10, 50), region(20, 60))).toList ===
      Seq(region(10, 60)))
  }

  test("calculate coverage of three regions") {
    val c = new Coverage(100L)
    assert(c.calculateCoverageRegions(Seq(region(10, 100), region(10, 20), region(50, 80))).toList ===
      Seq(region(10, 100)))
  }

  test("calculate coverage of two adjacent regions") {
    val c = new Coverage(100L)
    assert(c.calculateCoverageRegions(Seq(region(10, 99), region(99, 200))).toList ===
      Seq(region(10, 200)))
  }

  test("calculate coverage of two nearby regions") {
    val c = new Coverage(100L)
    assert(c.calculateCoverageRegions(Seq(region(10, 100), region(101, 200))).toList ===
      Seq(region(10, 100), region(101, 200)))
  }

  test("calculate coverage of three out-of-order regions") {
    val c = new Coverage(100L)
    assert(c.calculateCoverageRegions(Seq(region(100, 200), region(10, 50), region(150, 201))).toList ===
      Seq(region(10, 50), region(100, 201)))
  }

  test("calculate coverage of two regions which join at a window boundary") {
    val c = new Coverage(100L)
    assert(c.calculateCoverageRegions(Seq(region(0, 100), region(100, 200))).toList ===
      Seq(region(0, 200)))
  }

  def rdd[T](values: Seq[T])(implicit sc: SparkContext, kt: ClassTag[T]): RDD[T] =
    if (values.isEmpty)
      sc.emptyRDD
    else
      sc.parallelize(values)

  implicit def seqToRDD[T](values: Seq[T])(implicit sc: SparkContext, kt: ClassTag[T]): RDD[T] = rdd(values)

  /*
  Tests for coverage calculation inside RDDs
   */

  sparkTest("find empty coverage") {
    implicit val sparkContext = sc
    val c = new Coverage(100L)
    assert(c.findCoverageRegions(Seq()).collect() === Array())
  }

  sparkTest("find coverage of one region") {
    implicit val sparkContext = sc
    val c = new Coverage(100L)
    assert(c.findCoverageRegions(Seq(region(10, 50))).collect() === Array(region(10, 50)))
  }

  sparkTest("find coverage of two regions") {
    implicit val sparkContext = sc
    val c = new Coverage(100L)
    assert(c.findCoverageRegions(Seq(region(10, 50), region(20, 60))).collect() ===
      Array(region(10, 60)))
  }

  sparkTest("find coverage of three regions") {
    implicit val sparkContext = sc
    val c = new Coverage(100L)
    assert(c.findCoverageRegions(Seq(region(10, 100), region(10, 20), region(50, 80))).collect() ===
      Array(region(10, 100)))
  }

  sparkTest("find coverage of two adjacent regions") {
    implicit val sparkContext = sc
    val c = new Coverage(100L)
    assert(c.findCoverageRegions(Seq(region(10, 99), region(99, 200))).collect() ===
      Array(region(10, 200)))
  }

  sparkTest("find coverage of two nearby regions") {
    implicit val sparkContext = sc
    val c = new Coverage(100L)
    assert(c.findCoverageRegions(Seq(region(10, 100), region(101, 200))).collect() ===
      Array(region(10, 100), region(101, 200)))
  }

  sparkTest("find coverage of three out-of-order regions") {
    implicit val sparkContext = sc
    val c = new Coverage(100L)
    assert(c.findCoverageRegions(Seq(region(100, 200), region(10, 50), region(150, 201))).collect() ===
      Array(region(10, 50), region(100, 201)))
  }

  sparkTest("find coverage of two regions which join at a window boundary") {
    implicit val sparkContext = sc
    val c = new Coverage(100L)
    assert(c.findCoverageRegions(Seq(region(0, 100), region(100, 200))).collect() ===
      Array(region(0, 200)))
  }

}
