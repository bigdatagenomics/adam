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

import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
import org.scalatest._

class ReferenceRegionSuite extends FunSuite {

  test("contains(: ReferenceRegion)") {
    assert(region("chr0", 10, 100).contains(region("chr0", 50, 70)))
    assert(region("chr0", 10, 100).contains(region("chr0", 10, 100)))
    assert(!region("chr0", 10, 100).contains(region("chr1", 50, 70)))
    assert(region("chr0", 10, 100).contains(region("chr0", 50, 100)))
    assert(!region("chr0", 10, 100).contains(region("chr0", 50, 101)))
  }

  test("contains(: ReferencePosition)") {
    assert(region("chr0", 10, 100).contains(point("chr0", 50)))
    assert(region("chr0", 10, 100).contains(point("chr0", 10)))
    assert(region("chr0", 10, 100).contains(point("chr0", 99)))
    assert(!region("chr0", 10, 100).contains(point("chr0", 100)))
    assert(!region("chr0", 10, 100).contains(point("chr1", 50)))
  }

  test("merge") {
    intercept[AssertionError] {
      region("chr0", 10, 100).merge(region("chr1", 10, 100))
    }

    val r1 = region("chr0", 10, 100)
    val r2 = region("chr0", 0, 15)
    val r3 = region("chr0", 50, 150)

    val r12 = region("chr0", 0, 100)
    val r13 = region("chr0", 10, 150)

    assert(r1.merge(r1) === r1)
    assert(r1.merge(r2) === r12)
    assert(r1.merge(r3) === r13)
  }

  test("overlaps") {

    // contained
    assert(region("chr0", 10, 100).overlaps(region("chr0", 20, 50)))

    // right side
    assert(region("chr0", 10, 100).overlaps(region("chr0", 50, 250)))

    // left side
    assert(region("chr0", 10, 100).overlaps(region("chr0", 5, 15)))

    // left edge
    assert(region("chr0", 10, 100).overlaps(region("chr0", 5, 11)))
    assert(!region("chr0", 10, 100).overlaps(region("chr0", 5, 10)))

    // right edge
    assert(region("chr0", 10, 100).overlaps(region("chr0", 99, 200)))
    assert(!region("chr0", 10, 100).overlaps(region("chr0", 100, 200)))

    // different sequences
    assert(!region("chr0", 10, 100).overlaps(region("chr1", 50, 200)))
  }

  test("distance(: ReferenceRegion)") {

    // distance on the right
    assert(region("chr0", 10, 100).distance(region("chr0", 200, 300)) === Some(101))

    // distance on the left
    assert(region("chr0", 100, 200).distance(region("chr0", 10, 50)) === Some(51))

    // different sequences
    assert(region("chr0", 100, 200).distance(region("chr1", 10, 50)) === None)

    // touches on the right
    assert(region("chr0", 10, 100).distance(region("chr0", 100, 200)) === Some(1))

    // overlaps
    assert(region("chr0", 10, 100).distance(region("chr0", 50, 150)) === Some(0))

    // touches on the left
    assert(region("chr0", 10, 100).distance(region("chr0", 0, 10)) === Some(1))
  }

  test("distance(: ReferencePosition)") {

    // middle
    assert(region("chr0", 10, 100).distance(point("chr0", 50)) === Some(0))

    // left edge
    assert(region("chr0", 10, 100).distance(point("chr0", 10)) === Some(0))

    // right edge
    assert(region("chr0", 10, 100).distance(point("chr0", 100)) === Some(1))

    // right
    assert(region("chr0", 10, 100).distance(point("chr0", 150)) === Some(51))

    // left
    assert(region("chr0", 100, 200).distance(point("chr0", 50)) === Some(50))

    // different sequences
    assert(region("chr0", 100, 200).distance(point("chr1", 50)) === None)

  }

  test("create region from unmapped read fails") {
    val read = AlignmentRecord.newBuilder()
      .setReadMapped(false)
      .build()

    assert(ReferenceRegion(read).isEmpty)
  }

  test("create region from mapped read contains read start and end") {
    val read = AlignmentRecord.newBuilder()
      .setReadMapped(true)
      .setSequence("AAAAA")
      .setStart(1L)
      .setCigar("5M")
      .setEnd(6L)
      .setContig(Contig.newBuilder
        .setContigName("chr1")
        .setContigLength(10)
        .build)
      .build()

    assert(ReferenceRegion(read).isDefined)
    assert(ReferenceRegion(read).get.contains(point("chr1", 1L)))
    assert(ReferenceRegion(read).get.contains(point("chr1", 5L)))

    assert(!ReferenceRegion(read).get.contains(point("chr1", 6L)))
  }

  test("validate that adjacent regions can be merged") {
    val r1 = region("chr1", 0L, 6L)
    val r2 = region("chr1", 6L, 10L)

    assert(r1.distance(r2).get === 1)
    assert(r1.isAdjacent(r2))
    assert(r1.merge(r2) == region("chr1", 0L, 10L))
  }

  test("validate that non-adjacent regions cannot be merged") {
    val r1 = region("chr1", 0L, 5L)
    val r2 = region("chr1", 7L, 10L)

    assert(!r1.isAdjacent(r2))

    intercept[AssertionError] {
      r1.merge(r2)
    }
  }

  test("compute convex hull of two sets") {
    val r1 = region("chr1", 0L, 5L)
    val r2 = region("chr1", 7L, 10L)

    assert(!r1.isAdjacent(r2))

    val hull1 = r1.hull(r2)
    val hull2 = r2.hull(r1)

    assert(hull1 === hull2)
    assert(hull1.overlaps(r1))
    assert(hull1.overlaps(r2))
    assert(hull1.start == 0L)
    assert(hull1.end == 10L)
  }

  test("region name is sanitized when creating region from read") {
    val contig = Contig.newBuilder()
      .setContigName("chrM")
      .build()

    val read = AlignmentRecord.newBuilder()
      .setStart(5L)
      .setSequence("ACGT")
      .setContig(contig)
      .setReadMapped(true)
      .setCigar("5M")
      .setEnd(10L)
      .setMismatchingPositions("5")
      .build()

    val region = ReferenceRegion(read)

    assert(region.isDefined)

    val r = region.get

    assert(r.referenceName === "chrM")
    assert(r.start === 5L)
    assert(r.end === 10L)
  }

  test("intersection fails on non-overlapping regions") {
    intercept[AssertionError] {
      ReferenceRegion("chr1", 1L, 10L).intersection(ReferenceRegion("chr1", 11L, 20L))
    }
    intercept[AssertionError] {
      ReferenceRegion("chr1", 1L, 10L).intersection(ReferenceRegion("chr2", 1L, 10L))
    }
  }

  test("compute intersection") {
    val overlapRegion = ReferenceRegion("chr1", 1L, 10L).intersection(ReferenceRegion("chr1", 5L, 15L))
    assert(overlapRegion.referenceName === "chr1")
    assert(overlapRegion.start === 5L)
    assert(overlapRegion.end === 10L)
  }

  def region(refName: String, start: Long, end: Long): ReferenceRegion =
    ReferenceRegion(refName, start, end)

  def point(refName: String, pos: Long): ReferencePosition =
    ReferencePosition(refName, pos)

  test("build oriented reference region from non-oriented") {
    val rrf = ReferenceRegionWithOrientation(ReferenceRegion("chr1", 10L, 20L), negativeStrand = false)
    assert(rrf.referenceName === "chr1")
    assert(rrf.start === 10L)
    assert(rrf.end === 20L)
    assert(!rrf.negativeStrand)

    val rrr = ReferenceRegionWithOrientation(ReferenceRegion("chr1", 10L, 20L), negativeStrand = true)
    assert(rrr.referenceName === "chr1")
    assert(rrr.start === 10L)
    assert(rrr.end === 20L)
    assert(rrr.negativeStrand)
  }

  test("comparison tests for oriented reference region") {
    assert(ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = false)
      .contains(ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = false)))
    assert(ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = true)
      .contains(ReferenceRegionWithOrientation("chr1", 15L, 17L, negativeStrand = true)))

    val rrf = ReferenceRegionWithOrientation(ReferenceRegion("chr1", 10L, 20L), negativeStrand = false)
    val rrr = ReferenceRegionWithOrientation(ReferenceRegion("chr1", 10L, 20L), negativeStrand = true)
    assert(!rrf.contains(rrr))

    assert(!ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = false)
      .contains(ReferenceRegionWithOrientation("chr2", 10L, 20L, negativeStrand = false)))
    assert(!ReferenceRegionWithOrientation("chr1", 20L, 50L, negativeStrand = true)
      .contains(ReferenceRegionWithOrientation("chr1", 50L, 100L, negativeStrand = true)))
  }

  test("comparison tests for oriented reference region vs position") {
    assert(ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = false)
      .contains(ReferencePositionWithOrientation(Some(ReferencePosition("chr1", 10L)), negativeStrand = false)))
    assert(ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = true)
      .contains(ReferencePositionWithOrientation(Some(ReferencePosition("chr1", 17L)), negativeStrand = true)))

    assert(!ReferenceRegionWithOrientation(ReferenceRegion("chr1", 10L, 20L), negativeStrand = false)
      .contains(ReferencePositionWithOrientation(Some(ReferencePosition("chr1", 17L)), negativeStrand = true)))
    assert(!ReferenceRegionWithOrientation(ReferenceRegion("chr1", 10L, 20L), negativeStrand = true)
      .contains(ReferencePositionWithOrientation(Some(ReferencePosition("chr1", 10L)), negativeStrand = false)))

    assert(!ReferenceRegionWithOrientation(ReferenceRegion("chr1", 10L, 20L), negativeStrand = false)
      .contains(ReferencePositionWithOrientation(None.asInstanceOf[Option[ReferencePosition]], negativeStrand = true)))
    assert(!ReferenceRegionWithOrientation(ReferenceRegion("chr1", 10L, 20L), negativeStrand = true)
      .contains(ReferencePositionWithOrientation(None.asInstanceOf[Option[ReferencePosition]], negativeStrand = false)))

    assert(!ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = false)
      .contains(ReferencePositionWithOrientation(Some(ReferencePosition("chr2", 10L)), negativeStrand = false)))
    assert(!ReferenceRegionWithOrientation("chr1", 20L, 50L, negativeStrand = true)
      .contains(ReferencePositionWithOrientation(Some(ReferencePosition("chr1", 100L)), negativeStrand = true)))
  }

  test("overlap tests for oriented reference region") {
    assert(ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = false)
      .overlaps(ReferenceRegionWithOrientation("chr1", 15L, 25L, negativeStrand = false)))
    assert(ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = true)
      .overlaps(ReferenceRegionWithOrientation("chr1", 5L, 15L, negativeStrand = true)))

    val rrf = ReferenceRegionWithOrientation(ReferenceRegion("chr1", 12L, 22L), negativeStrand = false)
    val rrr = ReferenceRegionWithOrientation(ReferenceRegion("chr1", 8L, 8L), negativeStrand = true)
    assert(!rrf.overlaps(rrr))

    assert(!ReferenceRegionWithOrientation("chr1", 10L, 20L, negativeStrand = false)
      .overlaps(ReferenceRegionWithOrientation("chr2", 10L, 20L, negativeStrand = false)))
    assert(!ReferenceRegionWithOrientation("chr1", 20L, 50L, negativeStrand = true)
      .overlaps(ReferenceRegionWithOrientation("chr1", 51L, 100L, negativeStrand = true)))
  }

  test("check the width of a reference region") {
    assert(ReferenceRegion("chr1", 100, 201).width === 100)
    assert(ReferenceRegionWithOrientation("chr2", 200, 401, negativeStrand = false).width === 200)
    assert(ReferenceRegionWithOrientation("chr3", 399, 1000, negativeStrand = true).width === 600)
  }
}
