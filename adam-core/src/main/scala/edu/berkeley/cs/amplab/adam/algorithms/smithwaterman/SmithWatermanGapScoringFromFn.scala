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

abstract class SmithWatermanGapScoringFromFn (xSequence: String,
				     ySequence: String,
				     scoreFn: (Int, Int, Char, Char) => Double)
    extends SmithWaterman (xSequence, ySequence) {
  
  def buildScoringMatrix (): (Array[Array[Double]], Array[Array[Char]]) = {

    val y = ySequence.length
    val x = xSequence.length

    var scoreMatrix = new Array[Array[Double]](x + 1)
    var moveMatrix = new Array[Array[Char]](x + 1)
    for (i <- 0 to x) {
      scoreMatrix (i) = new Array[Double](y + 1)
      moveMatrix (i) = new Array[Char](y + 1)
    }

    // set row/col 0 to 0
    for (i <- 0 to x) {
      scoreMatrix (i)(0) = 0.0
      moveMatrix (i)(0) = 'T'
    }
    for (j <- 0 to y) {
      scoreMatrix (0)(j) = 0.0
      moveMatrix (0)(j) = 'T'
    }

    // score matrix
    for (i <- 1 to x) {
      for (j <- 1 to y) {
	val m = scoreMatrix(i - 1)(j - 1) + scoreFn(i, j, xSequence(i - 1), ySequence(j - 1))
	val d = scoreMatrix(i - 1)(j) + scoreFn(i, j, xSequence(i - 1), '_')
	val in = scoreMatrix(i)(j - 1) + scoreFn(i, j, '_', ySequence(j - 1))	

	val (scoreUpdate, moveUpdate) = if (m >= d && m >= in && m > 0.0) {
          (m, 'B')
        } else if (d >= in && d > 0.0) {
          (d, 'J')
        } else if (in > 0.0) {
          (in, 'I')
        } else {
          (0.0, 'T')
        }

	scoreMatrix(i)(j) = scoreUpdate
        moveMatrix(i)(j) = moveUpdate
      }
    }

    (scoreMatrix, moveMatrix)
  }

}
    

