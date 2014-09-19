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

import org.scalatest.FunSuite
import org.bdgenomics.adam.models.ReferenceRegion

class IntervalTreeTest extends FunSuite {
  test("Two nested symmetric intervals should produce only one tree node") {
    val regions = List((ReferenceRegion("chr1", 10, 20), 1), (ReferenceRegion("chr1", 12, 18), 2))
    val tree = new IntervalTree[Int]("chr1", regions)

    assert(tree.root.inclusiveIntervals.length == 2)
    assert(tree.root.leftChild == null)
    assert(tree.root.rightChild == null)
  }

  test("Three independent intervals that produce 3 tree nodes") {
    val regions = List((ReferenceRegion("chr1", 10, 20), 1), (ReferenceRegion("chr1", 30, 40), 2), (ReferenceRegion("chr1", 50, 60), 3))
    val tree = new IntervalTree[Int]("chr1", regions)

    assert(tree.root.centerPoint == 35)
    assert(tree.root.inclusiveIntervals.head._1.start == 30)

    assert(tree.root.inclusiveIntervals.length == 1)
    assert(tree.root.leftChild.inclusiveIntervals.length == 1)
    assert(tree.root.rightChild.rightChild == null)
    assert(tree.root.rightChild.leftChild == null)
    assert(tree.root.leftChild.rightChild == null)
    assert(tree.root.rightChild.leftChild == null)
  }

  test("Queries against a collection of intervals") {
    val regions = List(
      (ReferenceRegion("chr1", 10, 100), 1),
      (ReferenceRegion("chr1", 80, 95), 2),
      (ReferenceRegion("chr1", 20, 30), 3),
      (ReferenceRegion("chr1", 15, 18), 4))

    val tree = new IntervalTree[Int]("chr1", regions)
    val successful_q = ReferenceRegion("chr1", 25, 40)
    val unsuccessful_q = ReferenceRegion("chr1", 104, 2014)

    val successful_ans = tree.getAllOverlappings(successful_q)
    val unsuccessful_ans = tree.getAllOverlappings(unsuccessful_q)

    assert(successful_ans.length == 2)
    assert(unsuccessful_ans.length == 0)

  }

}
