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

import org.bdgenomics.formats.avro.{ Alignment, Reference }
import org.scalatest.FunSuite

class NonoverlappingRegionsSuite extends FunSuite {

  test("alternating returns an alternating seq of items") {
    assert(NonoverlappingRegions.alternating(Seq(),
      includeFirst = true) === Seq())
    assert(NonoverlappingRegions.alternating(Seq(1),
      includeFirst = true) === Seq(1))
    assert(NonoverlappingRegions.alternating(Seq(1, 2),
      includeFirst = true) === Seq(1))
    assert(NonoverlappingRegions.alternating(Seq(1, 2, 3),
      includeFirst = true) === Seq(1, 3))
    assert(NonoverlappingRegions.alternating(Seq(1, 2, 3, 4),
      includeFirst = true) === Seq(1, 3))
    assert(NonoverlappingRegions.alternating(Seq(1, 2, 3, 4, 5),
      includeFirst = true) === Seq(1, 3, 5))

    assert(NonoverlappingRegions.alternating(Seq(),
      includeFirst = false) === Seq())
    assert(NonoverlappingRegions.alternating(Seq(1),
      includeFirst = false) === Seq())
    assert(NonoverlappingRegions.alternating(Seq(1, 2),
      includeFirst = false) === Seq(2))
    assert(NonoverlappingRegions.alternating(Seq(1, 2, 3),
      includeFirst = false) === Seq(2))
    assert(NonoverlappingRegions.alternating(Seq(1, 2, 3, 4),
      includeFirst = false) === Seq(2, 4))
    assert(NonoverlappingRegions.alternating(Seq(1, 2, 3, 4, 5),
      includeFirst = false) === Seq(2, 4))
    assert(NonoverlappingRegions.alternating(Seq(1, 2, 3, 4, 5, 6),
      includeFirst = false) === Seq(2, 4, 6))
  }

  test("Single region returns itself") {
    val region = new ReferenceRegion("chr1", 1, 2)
    val regions = new NonoverlappingRegions(Seq(region))
    val result = regions.findOverlappingRegions(region)
    assert(result.size === 1)
    assert(result.head === region)
  }

  test("Two adjacent regions will be merged") {
    val regions = new NonoverlappingRegions(Seq(
      ReferenceRegion("chr1", 10, 20),
      ReferenceRegion("chr1", 20, 30)))

    assert(regions.endpoints === Array(10L, 30L))
  }

  test("Nonoverlapping regions will all be returned") {
    val region1 = new ReferenceRegion("chr1", 1, 2)
    val region2 = new ReferenceRegion("chr1", 3, 5)
    val testRegion3 = new ReferenceRegion("chr1", 1, 4)
    val testRegion1 = new ReferenceRegion("chr1", 4, 5)
    val regions = new NonoverlappingRegions(Seq(region1, region2))

    // this should be 2, not 3, because binaryRegionSearch is (now) no longer returning
    // ReferenceRegions in which no original RR's were placed (i.e. the 'gaps').
    assert(regions.findOverlappingRegions(testRegion3).size === 2)

    assert(regions.findOverlappingRegions(testRegion1).size === 1)
  }

  test("Many overlapping regions will all be merged") {
    val region1 = new ReferenceRegion("chr1", 1, 3)
    val region2 = new ReferenceRegion("chr1", 2, 4)
    val region3 = new ReferenceRegion("chr1", 3, 5)
    val testRegion = new ReferenceRegion("chr1", 1, 4)
    val regions = new NonoverlappingRegions(Seq(region1, region2, region3))
    assert(regions.findOverlappingRegions(testRegion).size === 1)
  }

  test("ADAMRecords return proper references") {
    val reference = Reference.newBuilder
      .setName("chr1")
      .setLength(5L)
      .setSourceUri("test://chrom1")
      .build

    val built = Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = built
    val record2 = Alignment.newBuilder(built).setStart(3L).setEnd(4L).build()
    val baseRecord = Alignment.newBuilder(built).setCigar("4M").setEnd(5L).build()

    val baseMapping = new NonoverlappingRegions(Seq(ReferenceRegion.unstranded(baseRecord)))
    val regions1 = baseMapping.findOverlappingRegions(ReferenceRegion.unstranded(record1))
    val regions2 = baseMapping.findOverlappingRegions(ReferenceRegion.unstranded(record2))
    assert(regions1.size === 1)
    assert(regions2.size === 1)
    assert(regions1.head === regions2.head)
  }
}
