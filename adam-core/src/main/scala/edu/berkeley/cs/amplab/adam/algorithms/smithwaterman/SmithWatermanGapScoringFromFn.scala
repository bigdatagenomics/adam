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

package edu.berkeley.cs.amplab.adam.algorithms.smithwaterman

abstract class SmithWatermanGapScoringFromFn(xSequence: String,
  ySequence: String,
  scoreFn: (Int, Int, Char, Char) => Double)
  extends SmithWaterman(xSequence, ySequence) {

  def buildScoringMatrix(): Array[Array[Double]] = {

    val y = ySequence.length + 1
    val x = xSequence.length + 1

    var matrix = new Array[Array[Double]](x)
    for (i <- 0 until x) {
      matrix(i) = new Array[Double](y)
    }

    // set row/col 0 to 0
    for (i <- 0 until x) {
      matrix(i)(0) = 0.0
    }
    for (j <- 0 until y) {
      matrix(0)(j) = 0.0
    }

    // score matrix
    for (i <- 1 until x) {
      for (j <- i until y) {
        val m = matrix(i - 1)(j - 1) + scoreFn(i, j, xSequence(i), ySequence(j))
        val d = matrix(i - 1)(j) + scoreFn(i, j, xSequence(i), '_')
        val in = matrix(i)(j - 1) + scoreFn(i, j, '_', ySequence(j))
        val update = (d max in) max (m max 0)

        matrix(i)(j) = update

        // check if new max and update
        if (update > max) {
          maxX = i
          maxY = j
          max = update
        }
      }
    }

    matrix
  }

}

