/**
 * Copyright 2013 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.models

import org.scalatest._

class ReferenceRegionSuite extends FunSuite {

  test("contains(: ReferenceRegion)") {
    assert( region(0, 10, 100).contains(region(0, 50, 70)) )
    assert( region(0, 10, 100).contains(region(0, 10, 100)) )
    assert( !region(0, 10, 100).contains(region(1, 50, 70)))
    assert( region(0, 10, 100).contains(region(0, 50, 100)) )
    assert( !region(0, 10, 100).contains(region(0, 50, 101)) )
  }

  test("contains(: ReferencePosition)") {
    assert( region(0, 10, 100).contains(point(0, 50)))
    assert( region(0, 10, 100).contains(point(0, 10)))
    assert( region(0, 10, 100).contains(point(0, 99)))
    assert( !region(0, 10, 100).contains(point(0, 100)))
    assert( !region(0, 10, 100).contains(point(1, 50)))
  }

  test("union") {
    intercept[AssertionError] {
      region(0, 10, 100).union(region(1, 10, 100))
    }

    val r1 = region(0, 10, 100)
    val r2 = region(0, 0, 15)
    val r3 = region(0, 50, 150)

    val r12 = region(0, 0, 100)
    val r13 = region(0, 10, 150)

    assert(r1.union(r1) === r1)
    assert(r1.union(r2) === r12)
    assert(r1.union(r3) === r13)
  }

  test("overlaps") {

    // contained
    assert( region(0, 10, 100).overlaps(region(0, 20, 50)))

    // right side
    assert( region(0, 10, 100).overlaps(region(0, 50, 250)))

    // left side
    assert( region(0, 10, 100).overlaps(region(0, 5, 15)) )

    // left edge
    assert( region(0, 10, 100).overlaps(region(0, 5, 11)) )
    assert( !region(0, 10, 100).overlaps(region(0, 5, 10)) )

    // right edge
    assert( region(0, 10, 100).overlaps(region(0, 99, 200)) )
    assert( !region(0, 10, 100).overlaps(region(0, 100, 200)))

    // different sequences
    assert( !region(0, 10, 100).overlaps(region(1, 50, 200)) )
  }

  test("distance(: ReferenceRegion)") {

    // distance on the right
    assert( region(0, 10, 100).distance(region(0, 200, 300)) === Some(100))

    // distance on the left
    assert( region(0, 100, 200).distance(region(0, 10, 50)) === Some(50))

    // different sequences
    assert( region(0, 100, 200).distance(region(1, 10, 50)) === None)

    // touches on the right
    assert( region(0, 10, 100).distance(region(0, 100, 200)) === Some(0))

    // overlaps
    assert( region(0, 10, 100).distance(region(0, 50, 150)) === Some(0))

    // touches on the left
    assert( region(0, 10, 100).distance(region(0, 0, 10)) === Some(0))
  }

  test("distance(: ReferencePosition)") {

    // middle
    assert( region(0, 10, 100).distance(point(0, 50)) === Some(0))

    // left edge
    assert( region(0, 10, 100).distance(point(0, 10)) === Some(0))

    // right edge
    assert( region(0, 10, 100).distance(point(0, 100)) === Some(0))

    // right
    assert( region(0, 10, 100).distance(point(0, 150)) === Some(50))

    // left
    assert( region(0, 100, 200).distance(point(0, 50)) === Some(50))

    // different sequences
    assert( region(0, 100, 200).distance(point(1, 50)) === None)

  }

  def region(refId : Int, start : Long, end : Long) : ReferenceRegion =
    ReferenceRegion(refId, start, end)

  def point(refId : Int, pos : Long) : ReferencePosition =
    ReferencePosition(refId, pos)
}
