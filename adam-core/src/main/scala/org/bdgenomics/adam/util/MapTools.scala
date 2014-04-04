/*
 * Copyright 2014 Genome Bridge LLC
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
package org.bdgenomics.adam.util

object MapTools {

  /**
   * Takes two "count maps" (i.e. Map values that contain numeric counts as the values of the keys),
   * and does a point-wise add.  For example,
   *
   *   add(Map("A"->1, "B"->1), Map("B"->1, "C"->1))
   *
   * should be equal to
   *
   *   Map("A"->1, "B"->2, "C"->1)
   *
   * The operation should be commutative, although it contains a "filter" step on the second addend
   * (in the current implementation), so if the two maps are of wildly different sizes, you might
   * want to make the larger map the first argument.
   *
   * @param map1 Left addend
   * @param map2 Right addend
   * @param ops
   * @tparam KeyType
   * @tparam NumberType
   * @return
   */
  def add[KeyType, NumberType](map1: Map[KeyType, NumberType],
    map2: Map[KeyType, NumberType])(implicit ops: Numeric[NumberType]): Map[KeyType, NumberType] = {

    (map1.keys ++ map2.keys.filter(!map1.contains(_))).map {
      (key: KeyType) =>
        (key, ops.plus(map1.getOrElse(key, ops.zero), map2.getOrElse(key, ops.zero)))
    }.toMap
  }

}
