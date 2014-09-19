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
package org.bdgenomics.adam.rdd.regionMultiJoin

import org.scalatest.FunSuite
import org.bdgenomics.adam.models.ReferenceRegion

class IntervalForestSuite extends FunSuite {
  test("Two nested symmetric intervals should produce only one tree node") {
    val regions = List((ReferenceRegion("chr1", 10, 20), 1), (ReferenceRegion("chr1", 12, 18), 2))
    val forest = new IntervalForest[Int](regions)

    assert(forest.getTreeNodeCount("chr1") == 1)

  }

  test("Three independent intervals that produce 3 tree nodes") {
    val regions = List((ReferenceRegion("chr1", 10, 20), 1), (ReferenceRegion("chr1", 30, 40), 2), (ReferenceRegion("chr1", 50, 60), 3))
    val forest = new IntervalForest[Int](regions)

    assert(forest.getTreeHeight("chr1") == 1)
    assert(forest.getTreeNodeCount("chr1") == 3)
  }

  test("Queries against a collection of intervals1") {
    val regions = List(
      (ReferenceRegion("chr1", 10, 100), 1),
      (ReferenceRegion("chr1", 80, 95), 2),
      (ReferenceRegion("chr1", 20, 30), 3),
      (ReferenceRegion("chr1", 15, 18), 4))

    val forest = new IntervalForest[Int](regions)
    val successful_q = ReferenceRegion("chr1", 25, 40)
    val unsuccessful_q = ReferenceRegion("chr1", 104, 2014)

    val successful_ans = forest.getAllOverlappings(successful_q)
    val unsuccessful_ans = forest.getAllOverlappings(unsuccessful_q)

    assert(successful_ans.length == 2)
    assert(unsuccessful_ans.length == 0)

  }

  test("Queries against a collection of intervals across mutliple chromosomes") {
    val regions = List(
      (ReferenceRegion("chr1", 100L, 199L), 1),
      (ReferenceRegion("chr1", 200L, 299L), 2),
      (ReferenceRegion("chr1", 400L, 600L), 3),
      (ReferenceRegion("chr2", 1000L, 2000L), 4),
      (ReferenceRegion("chr13", 100L, 199L), 5),
      (ReferenceRegion("chr13", 200L, 299L), 6),
      (ReferenceRegion("chr13", 400L, 600L), 7))

    val forest = new IntervalForest[Int](regions)

    val s1 = forest.getAllOverlappings(ReferenceRegion("chr1", 150L, 250L))
    val s2 = forest.getAllOverlappings(ReferenceRegion("chr1", 300L, 500L))
    val s3 = forest.getAllOverlappings(ReferenceRegion("chr1", 500L, 700L))
    val s4 = forest.getAllOverlappings(ReferenceRegion("chr2", 1400L, 1600L))
    val t1 = forest.getAllOverlappings(ReferenceRegion("chr13", 150L, 250L))
    val t2 = forest.getAllOverlappings(ReferenceRegion("chr13", 300L, 500L))
    val t3 = forest.getAllOverlappings(ReferenceRegion("chr13", 500L, 700L))
    val noGo1 = forest.getAllOverlappings(ReferenceRegion("chr1", 1400L, 1600L))
    val noGo2 = forest.getAllOverlappings(ReferenceRegion("chr14", 300L, 500L))
    val noGo3 = forest.getAllOverlappings(ReferenceRegion("chr22", 500L, 700L))

    assert(s1.length == 2)
    assert(s2.length == 1)
    assert(s3.length == 1)
    assert(s4.length == 1)

    assert(t1.length == 2)
    assert(t2.length == 1)
    assert(t3.length == 1)

    assert(noGo1.length == 0)
    assert(noGo2.length == 0)
    assert(noGo3.length == 0)

  }

}
