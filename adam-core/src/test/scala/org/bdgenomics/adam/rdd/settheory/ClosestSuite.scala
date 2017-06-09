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
package org.bdgenomics.adam.rdd.settheory

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._
import scala.collection.mutable.ListBuffer

class ClosestSuite extends ADAMFunSuite {
  sparkTest("testing closest") {
    val leftFile = sc.loadBed(resourceUrl("intersect_with_overlap_00.bed").getFile)
      .sortLexicographically(storePartitionMap = true)
    val rightFile = sc.loadBed(resourceUrl("intersect_with_overlap_01.bed").getFile)

    val x = ShuffleClosestRegion(leftFile.flattenRddByRegions(),
      rightFile.flattenRddByRegions(), leftFile.optPartitionMap)
      .compute()
    val result = x.map(f =>
      (ReferenceRegion(f._1.getContigName, f._1.getStart, f._1.getEnd),
        f._2.map(g => ReferenceRegion(g.getContigName, g.getStart, g.getEnd))))
      .collect

    val correctOutput = Array((ReferenceRegion("chr1", 28735, 29810), ListBuffer(ReferenceRegion("chr1", 135000, 135444))),
      (ReferenceRegion("chr1", 135124, 135563), ListBuffer(ReferenceRegion("chr1", 135000, 135444), ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135333, 135777))),
      (ReferenceRegion("chr1", 135453, 139441), ListBuffer(ReferenceRegion("chr1", 135124, 135563), ReferenceRegion("chr1", 135333, 135777))),
      (ReferenceRegion("chr1", 327790, 328229), ListBuffer(ReferenceRegion("chr1", 135333, 135777))),
      (ReferenceRegion("chr1", 437151, 438164), ListBuffer(ReferenceRegion("chr1", 135333, 135777))),
      (ReferenceRegion("chr1", 449273, 450544), ListBuffer(ReferenceRegion("chr1", 135333, 135777))),
      (ReferenceRegion("chr1", 533219, 534114), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 544738, 546649), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 713984, 714547), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 762416, 763445), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 788863, 789211), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 801975, 802338), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 805198, 805628), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 839694, 840619), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 844299, 845883), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 854765, 854973), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 858970, 861632), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 869332, 871872), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 875730, 878363), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 886356, 886602), ListBuffer(ReferenceRegion("chr1", 886356, 886602))),
      (ReferenceRegion("chr1", 894313, 902654), ListBuffer(ReferenceRegion("chr1", 894313, 902654))))

    assert(result === correctOutput)
  }
}
