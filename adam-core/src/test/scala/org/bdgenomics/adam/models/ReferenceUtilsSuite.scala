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
package org.bdgenomics.adam.models

import org.bdgenomics.adam.models.ReferenceUtils._
import org.bdgenomics.adam.util.ADAMFunSuite

class ReferenceUtilsSuite extends ADAMFunSuite {

  test("unionReferenceSet: empty") {
    val regions = Seq()

    assert(unionReferenceSet(regions) === regions)
  }

  test("unionReferenceSet: one region") {
    val regions = Seq(ReferenceRegion("1", 3, 7))

    assert(unionReferenceSet(regions) === regions)
  }

  test("unionReferenceSet: multiple regions on one contig, all overlap") {
    val regions = Seq(ReferenceRegion("1", 3, 7),
      ReferenceRegion("1", 1, 5),
      ReferenceRegion("1", 6, 10))

    assert(unionReferenceSet(regions) === Seq(ReferenceRegion("1", 1, 10)))
  }

  test("unionReferenceSet: multiple regions on one contig, some overlap") {
    val regions = Seq(ReferenceRegion("1", 3, 7),
      ReferenceRegion("1", 1, 5),
      ReferenceRegion("1", 8, 10))

    assert(unionReferenceSet(regions) === Seq(ReferenceRegion("1", 8, 10), ReferenceRegion("1", 1, 7)))
  }

  test("unionReferenceSet: multiple regions on multiple contigs") {
    val regions = Seq(ReferenceRegion("1", 3, 7),
      ReferenceRegion("1", 1, 5),
      ReferenceRegion("2", 4, 8))

    assert(unionReferenceSet(regions) === Seq(ReferenceRegion("2", 4, 8), ReferenceRegion("1", 1, 7)))
  }

}
