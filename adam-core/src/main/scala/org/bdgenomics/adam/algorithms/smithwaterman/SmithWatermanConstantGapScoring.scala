/*
 * Copyright (c) 2013. Regents of the University of California
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

package org.bdgenomics.adam.algorithms.smithwaterman

object SmithWatermanConstantGapScoring {

  protected def constantGapFn(wMatch: Double, wDelete: Double, wInsert: Double, wMismatch: Double)(x: Int, y: Int, i: Char, j: Char): Double = {
    if (x == y) {
      wMatch
    } else if (x == '_') {
      wDelete
    } else if (y == '_') {
      wInsert
    } else {
      wMismatch
    }
  }

}

abstract class SmithWatermanConstantGapScoring(xSequence: String,
                                               ySequence: String,
                                               wMatch: Double,
                                               wMismatch: Double,
                                               wInsert: Double,
                                               wDelete: Double)
    extends SmithWatermanGapScoringFromFn(xSequence, ySequence, SmithWatermanConstantGapScoring.constantGapFn(wMatch, wMismatch, wInsert, wDelete)) {
}
