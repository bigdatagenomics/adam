/*
 * Copyright (c) 2014. Regents of the University of California
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

package org.bdgenomics.adam.rdd.RegionMultijoin

import org.bdgenomics.adam.util.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rich.ReferenceMappingContext._

class YetAnotherRegionJoinTest extends SparkFunSuite {
  sparkTest("joining non overlap regions results into no entries") {

    val rdd1: RDD[ReferenceRegion] = sc.parallelize(Seq(
      ReferenceRegion("chr1", 100L, 200L),
      ReferenceRegion("chr1", 400L, 600L),
      ReferenceRegion("chr1", 700L, 800L),
      ReferenceRegion("chr1", 900L, 1000L)))

    val rdd2: RDD[ReferenceRegion] = sc.parallelize(Seq(
      ReferenceRegion("chr2", 100L, 200L),
      ReferenceRegion("chr2", 400L, 600L),
      ReferenceRegion("chr1", 1100L, 1200L),
      ReferenceRegion("chr1", 1400L, 1600L)))

    assert(YetAnotherRegionJoin.overlapJoin(sc, rdd1, rdd2).count == 0)
  }

  sparkTest("test join with non-perfect overlapping regions") {
    val rdd1: RDD[ReferenceRegion] = sc.parallelize(Seq(
      ReferenceRegion("chr1", 100L, 200L),
      ReferenceRegion("chr1", 400L, 600L),
      ReferenceRegion("chr1", 700L, 800L),
      ReferenceRegion("chr1", 900L, 1000L)), 2)

    val rdd2: RDD[ReferenceRegion] = sc.parallelize(Seq(
      ReferenceRegion("chr1", 150L, 250L),
      ReferenceRegion("chr1", 300L, 500L),
      ReferenceRegion("chr1", 1100L, 1200L),
      ReferenceRegion("chr1", 1400L, 1600L)), 2)

    val j = YetAnotherRegionJoin.overlapJoin(sc, rdd1, rdd2).collect

    assert(j.size === 2)
    assert(j.forall(p => p._2.size == 1))
    assert(j.filter(p => p._1.start == 100L).size === 1)
    assert(j.filter(p => p._1.start == 100L).head._2.size === 1)
    assert(j.filter(p => p._1.start == 100L).head._2.head.start === 150L)
    assert(j.filter(p => p._1.start == 400L).size === 1)
    assert(j.filter(p => p._1.start == 400L).head._2.size === 1)
    assert(j.filter(p => p._1.start == 400L).head._2.head.start === 300L)
  }

  sparkTest("basic multi-join") {
    val rdd1: RDD[ReferenceRegion] = sc.parallelize(Seq(ReferenceRegion("chr1", 100L, 199L),
      ReferenceRegion("chr1", 200L, 299L),
      ReferenceRegion("chr1", 400L, 600L),
      ReferenceRegion("chr1", 10000L, 20000L)))

    val rdd2: RDD[ReferenceRegion] = sc.parallelize(Seq(ReferenceRegion("chr1", 150L, 250L),
      ReferenceRegion("chr1", 300L, 500L),
      ReferenceRegion("chr1", 500L, 700L),
      ReferenceRegion("chr2", 100L, 200L)))

    val j = YetAnotherRegionJoin.overlapJoin(sc, rdd1, rdd2).collect

    assert(j.size === 3)
    assert(j.filter(p => p._1.start == 100L).size === 1)
    assert(j.filter(p => p._1.start == 200L).size === 1)
    assert(j.filter(p => p._1.start <= 200L).forall(p => p._2.size == 1))
    assert(j.filter(p => p._1.start <= 200L).forall(p => p._2.head == ReferenceRegion("chr1", 150L, 250L)))
    assert(j.filter(p => p._1.start == 400L).size === 1)
    assert(j.filter(p => p._1.start == 400L).head._2.size === 2)
    assert(j.filter(p => p._1.start == 400L).head._2.filter(_ == ReferenceRegion("chr1", 300L, 500L)).size === 1)
    assert(j.filter(p => p._1.start == 400L).head._2.filter(_ == ReferenceRegion("chr1", 500L, 700L)).size === 1)
  }

}
