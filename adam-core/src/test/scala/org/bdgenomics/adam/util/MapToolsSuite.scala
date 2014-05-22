/**
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.util

import org.scalatest._

class MapToolsSuite extends FunSuite {

  test("add two nonzero integer maps") {

    val m1 = Map(55 -> 2, 23 -> 1)
    val m2 = Map(55 -> 1, 17 -> 10)

    val sum = MapTools.add(m1, m2)

    assert(sum.size === 3)
    assert(sum(55) === 3)
    assert(sum(23) === 1)
    assert(sum(17) === 10)
  }

  test("add two nonzero float maps") {
    val m1 = Map(55 -> 2.0f, 23 -> 1.1f)
    val m2 = Map(55 -> 1.8f, 17 -> 10.2f)

    val sum = MapTools.add(m1, m2)

    assert(sum.size === 3)
    assert(sum(55) === 2.0f + 1.8f)
    assert(sum(23) === 1.1f)
    assert(sum(17) === 10.2f)
  }

  test("adding an empty map is the identity") {
    val m1 = Map(55 -> 2, 23 -> 1)
    val m2 = Map[Int, Int]()

    val sum = MapTools.add(m1, m2)

    assert(sum === m1)
  }

}
