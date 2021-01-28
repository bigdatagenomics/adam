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
package org.bdgenomics.adam.ds

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.ds.variant.VariantArray
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ Alignment, Variant }
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.reflect.ClassTag

class TreeRegionJoinSuite extends ADAMFunSuite {

  sparkTest("run a join between data on a single contig") {

    val rightRdd = sc.parallelize(Seq(
      (ReferenceRegion("chr1", 10L, 20L), 0),
      (ReferenceRegion("chr1", 15L, 25L), 1),
      (ReferenceRegion("chr1", 30L, 50L), 2),
      (ReferenceRegion("chr1", 60L, 70L), 3),
      (ReferenceRegion("chr1", 90L, 100L), 4)))
      .map(kv => {
        val (k, v) = kv
        // i have made many poor life decisions
        (k, Variant.newBuilder
          .setStart(v.toLong)
          .build)
      })

    val tree = IntervalArray[ReferenceRegion, Variant](rightRdd,
      VariantArray.apply(_, _))

    val leftRdd = sc.parallelize(Seq(
      (ReferenceRegion("chr1", 12L, 22L), 0),
      (ReferenceRegion("chr1", 20L, 35L), 1),
      (ReferenceRegion("chr1", 40L, 55L), 2),
      (ReferenceRegion("chr1", 75L, 85L), 3),
      (ReferenceRegion("chr1", 95L, 105L), 4)))
      .map(kv => {
        val (k, v) = kv
        // and this is but another one of them
        (k, Alignment.newBuilder
          .setStart(v.toLong)
          .build)
      })

    val joinData = InnerTreeRegionJoin().runJoinAndGroupByRightWithTree(tree,
      leftRdd)
      .map(kv => {
        val (k, v) = kv
        (k.map(_.getStart.toInt), v.getStart.toInt)
      }).collect

    assert(joinData.size === 5)

    val joinMap = joinData.filter(_._1.nonEmpty)
      .map(_.swap)
      .toMap
      .mapValues(_.toSet)

    assert(joinMap.size === 4)
    assert(joinMap(0).size === 2)
    assert(joinMap(0)(0))
    assert(joinMap(0)(1))
    assert(joinMap(1).size === 2)
    assert(joinMap(1)(1))
    assert(joinMap(1)(2))
    assert(joinMap(2).size === 1)
    assert(joinMap(2)(2))
    assert(joinMap(4).size === 1)
    assert(joinMap(4)(4))
  }
}
