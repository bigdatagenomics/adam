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
import org.bdgenomics.adam.models.{ ReferenceRegionContext, ReferenceRegion }
import ADAMContext._
import ReferenceRegionContext._
import RegionRDDFunctions._
import org.bdgenomics.utils.misc.SparkFunSuite

class RegionRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("union of two simple overlapping regions returns a single union region") {
    val r1: ReferenceRegion = ReferenceRegion("chr1", 1000, 2000)
    val r2: ReferenceRegion = ReferenceRegion("chr1", 1500, 3000)

    val rdd1 = sc.parallelize(Seq(r1))
    val rdd2 = sc.parallelize(Seq(r2))

    val unioned = rdd1.spatialUnion(rdd2)

    assert(unioned.collect() === Array(ReferenceRegion("chr1", 1000, 3000)))
  }

  sparkTest("joinByOverlap on an overlapping pair returns one pair") {
    val r1: ReferenceRegion = ReferenceRegion("chr1", 1000, 2000)
    val r2: ReferenceRegion = ReferenceRegion("chr1", 1500, 3000)

    val rdd1 = sc.parallelize(Seq(r1))
    val rdd2 = sc.parallelize(Seq(r2))

    val joined = rdd1.joinByOverlap(rdd2)

    assert(joined.collect() === Array((r1, r2)))
  }

  sparkTest("joinByRange on a nearby pair returns one pair") {
    val r1: ReferenceRegion = ReferenceRegion("chr1", 1000, 2000)
    val r2: ReferenceRegion = ReferenceRegion("chr1", 3000, 4000)

    val rdd1 = sc.parallelize(Seq(r1))
    val rdd2 = sc.parallelize(Seq(r2))

    val joined = rdd1.joinWithinRange(rdd2, 1001L)
    assert(joined.collect() === Array((r1, r2)))

    val joined2 = rdd1.joinWithinRange(rdd2, 500L)
    assert(joined2.collect().isEmpty)
  }
}
