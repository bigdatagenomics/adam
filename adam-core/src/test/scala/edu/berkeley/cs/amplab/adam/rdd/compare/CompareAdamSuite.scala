/**
 * Copyright 2013 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.rdd.compare

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite

class CompareAdamSuite extends SparkFunSuite {

  sparkTest("Test that reads12.sam and reads21.sam are the same") {
    val reads12 = ClassLoader.getSystemClassLoader.getResource("reads12.sam").getFile
    val reads21 = ClassLoader.getSystemClassLoader.getResource("reads21.sam").getFile

    val (comp1, comp2) =
      CompareAdam.compareADAM(sc, reads12, reads21, CompareAdam.readLocationsMatchPredicate)

    assert(comp1.total === 200)
    assert(comp2.total === 200)
    assert(comp1.unique === 0)
    assert(comp2.unique === 0)
    assert(comp1.matching === 200)
  }
}
