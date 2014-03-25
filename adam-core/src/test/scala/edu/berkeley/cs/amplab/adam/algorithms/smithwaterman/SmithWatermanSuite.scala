/*
 * Copyright (c) 2014. Regents of the University of California
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

import org.scalatest.FunSuite
import scala.math.abs

class SmithWatermanSuite extends FunSuite {

  val epsilon = 1e-6
  def fpEquals(a: Double, b: Double): Boolean = {
    abs(a - b) < epsilon
  }

  test("gather max position from simple scoring matrix") {
    val c0 = Array(0.0, 0.0, 0.0, 0.0, 0.0)
    val c1 = Array(0.0, 1.0, 1.0, 1.0, 1.0)
    val c2 = Array(0.0, 1.0, 2.0, 2.0, 2.0)
    val c3 = Array(0.0, 1.0, 2.0, 3.0, 3.0)
    val c4 = Array(0.0, 1.0, 2.0, 3.0, 4.0)
    val matrix = Array(c0, c1, c2, c3, c4)
    
    val sw = new SmithWatermanConstantGapScoring("AAAA", "AAAA", 1.0, 0.0, -1.0, -1.0)

    val max = sw.maxCoordinates(matrix)
    assert(max._1 === 4)
    assert(max._2 === 4)
  }

  test("gather max position from irregular scoring matrix") {
    //             _    A    C    G    T
    val c0 = Array(0.0, 0.0, 0.0, 0.0, 0.0) // _
    val c1 = Array(0.0, 1.0, 1.0, 1.0, 1.0) // A
    val c2 = Array(0.0, 0.0, 2.0, 1.0, 1.0) // C
    val c3 = Array(0.0, 1.0, 1.0, 2.0, 1.0) // A
    val c4 = Array(0.0, 0.0, 1.0, 1.0, 3.0) // T
    val c5 = Array(0.0, 0.0, 0.0, 0.0, 2.0) // G
    val c6 = Array(0.0, 1.0, 0.0, 0.0, 1.0) // A
    val matrix = Array(c0, c1, c2, c3, c4, c5, c6)
    
    val sw = new SmithWatermanConstantGapScoring("ACGT", "ACATGA", 1.0, 0.0, -1.0, -1.0)

    val max = sw.maxCoordinates(matrix)
    assert(max._1 === 4)
    assert(max._2 === 4)
  }

  test("gather max position from irregular scoring matrix with deletions") {
    //             _    A    C    G    A
    val c0 = Array(0.0, 0.0, 0.0, 0.0, 0.0) // _
    val c1 = Array(0.0, 1.0, 1.0, 1.0, 1.0) // A
    val c2 = Array(0.0, 0.5, 2.0, 1.0, 1.0) // C
    val c3 = Array(0.0, 1.0, 1.5, 2.0, 1.5) // A
    val c4 = Array(0.0, 0.5, 1.0, 1.5, 2.0) // T
    val c5 = Array(0.0, 0.5, 0.5, 2.0, 1.5) // G
    val c6 = Array(0.0, 1.0, 0.5, 1.5, 3.0) // A
    val matrix = Array(c0, c1, c2, c3, c4, c5, c6)
    
    val sw = new SmithWatermanConstantGapScoring("ACGA", "ACATGA", 1.0, 0.0, -0.5, -0.5)

    val max = sw.maxCoordinates(matrix)
    assert(max._1 === 4)
    assert(max._2 === 6)
  }  

  test("score simple alignment with constant gap") {
    val c0 = Array(0.0, 0.0, 0.0, 0.0, 0.0)
    val c1 = Array(0.0, 1.0, 1.0, 1.0, 1.0)
    val c2 = Array(0.0, 1.0, 2.0, 2.0, 2.0)
    val c3 = Array(0.0, 1.0, 2.0, 3.0, 3.0)
    val c4 = Array(0.0, 1.0, 2.0, 3.0, 4.0)
    val matrix = Array(c0, c1, c2, c3, c4)
    
    val sw = new SmithWatermanConstantGapScoring("AAAA", "AAAA", 1.0, 0.0, -1.0, -1.0)

    val (swMatrix, _) = sw.buildScoringMatrix()
    for (j <- 0 to 4) {
      for (i <- 0 to 4) {
        assert(fpEquals(swMatrix(i)(j), matrix(i)(j)))
      }
    }
  }

  test("score irregular scoring matrix") {
    //             _    A    C    G    T
    val c0 = Array(0.0, 0.0, 0.0, 0.0, 0.0) // _
    val c1 = Array(0.0, 1.0, 0.0, 0.0, 0.0) // A
    val c2 = Array(0.0, 0.0, 2.0, 1.0, 0.0) // C
    val c3 = Array(0.0, 1.0, 1.0, 2.0, 1.0) // A
    val c4 = Array(0.0, 0.0, 1.0, 1.0, 3.0) // T
    val c5 = Array(0.0, 0.0, 0.0, 2.0, 2.0) // G
    val c6 = Array(0.0, 1.0, 0.0, 1.0, 2.0) // A
    val matrix = Array(c0, c1, c2, c3, c4, c5, c6)
    
    val sw = new SmithWatermanConstantGapScoring("ACATGA", "ACGT", 1.0, 0.0, -1.0, -1.0)

    val (swMatrix, _) = sw.buildScoringMatrix()
    for (i <- 0 to 6) {
      for (j <- 0 to 4) {
        assert(fpEquals(swMatrix(i)(j), matrix(i)(j)))
      }
    }
  }

  test("score irregular scoring matrix with indel") {
    //             _    A    C    G    A
    val c0 = Array(0.0, 0.0, 0.0, 0.0, 0.0) // _
    val c1 = Array(0.0, 1.0, 0.5, 0.0, 1.0) // A
    val c2 = Array(0.0, 0.5, 2.0, 1.5, 1.0) // C
    val c3 = Array(0.0, 1.0, 1.5, 2.0, 2.5) // A
    val c4 = Array(0.0, 0.5, 1.0, 1.5, 2.0) // T
    val c5 = Array(0.0, 0.0, 0.5, 2.0, 1.5) // G
    val c6 = Array(0.0, 1.0, 0.5, 1.5, 3.0) // A
    val matrix = Array(c0, c1, c2, c3, c4, c5, c6)
    
    val sw = new SmithWatermanConstantGapScoring("ACATGA", "ACGA", 1.0, 0.0, -0.5, -0.5)

    val (swMatrix, _) = sw.buildScoringMatrix()
    for (i <- 0 to 6) {
      for (j <- 0 to 4) {
        assert(fpEquals(swMatrix(i)(j), matrix(i)(j)))
      }
    }
  }

  val swh = new SmithWatermanConstantGapScoring("", "", 0.0, 0.0, 0.0, 0.0)

  test("can unroll cigars correctly") {
    assert(swh.cigarFromRNNCigar("MDDMMMM")  === "4M2D1M")
    assert(swh.cigarFromRNNCigar("MMMIIMM")  === "2M2I3M")
    assert(swh.cigarFromRNNCigar("MMMMMMMM") === "8M")
  }

  test("execute simple trackback") {
    val c0 = Array('T', 'T', 'T', 'T', 'T')
    val c1 = Array('T', 'B', 'B', 'B', 'B')
    val c2 = Array('T', 'B', 'B', 'B', 'B')
    val c3 = Array('T', 'B', 'B', 'B', 'B')
    val c4 = Array('T', 'B', 'B', 'B', 'B')
    val matrix = Array(c0, c1, c2, c3, c4)
    
    val (cx, cy, _, _) = swh.move(matrix, 4, 4, "", "")

    assert(cx === "4M")
    assert(cy === "4M")
  }

  test("execute trackback with indel") {
    //             _    A    C    G    A
    val c0 = Array('T', 'T', 'T', 'T', 'T') // _
    val c1 = Array('T', 'B', 'J', 'T', 'B') // A
    val c2 = Array('T', 'I', 'B', 'B', 'B') // C
    val c3 = Array('T', 'B', 'J', 'I', 'B') // A
    val c4 = Array('T', 'I', 'J', 'B', 'B') // T
    val c5 = Array('T', 'T', 'I', 'B', 'B') // G
    val c6 = Array('T', 'B', 'J', 'B', 'B') // A
    val matrix = Array(c0, c1, c2, c3, c4, c5, c6)
    
    val (cx, cy, _, _) = swh.move(matrix, 6, 4, "", "")

    assert(cx === "2M2I2M")
    assert(cy === "2M2D2M")
  }

  test("run end to end smith waterman for simple reads") {
    val sw = new SmithWatermanConstantGapScoring("AAAA", "AAAA", 1.0, 0.0, -1.0, -1.0)

    assert(sw.cigarX.toString === "4M")
    assert(sw.cigarY.toString === "4M")
  }

  test("run end to end smith waterman for short sequences with indel") {
    val sw = new SmithWatermanConstantGapScoring("ACATGA", "ACGA", 1.0, 0.0, -0.333, -0.333)

    assert(sw.cigarX.toString === "2M2I2M")
    assert(sw.cigarY.toString === "2M2D2M")
  }

  test("run end to end smith waterman for longer sequences with snp") {
    // ATTAGACTACTTAATATACAGATTTACCCCAATAGA
    // ATTAGACTACTTAATATACAGAATTACCCCAATAGA
    val sw = new SmithWatermanConstantGapScoring("ATTAGACTACTTAATATACAGATTTACCCCAATAGA", 
                                                 "ATTAGACTACTTAATATACAGAATTACCCCAATAGA",
                                                 1.0, 0.0, -0.333, -0.333)

    assert(sw.cigarX.toString === "36M")
    assert(sw.cigarY.toString === "36M")
  }

  test("run end to end smith waterman for longer sequences with short indel") {
    // ATTAGACTACTTAATATACAGATTTACCCCAATAGA
    // ATTAGACTACTTAATATACAGA__TACCCCAATAGA
    val sw = new SmithWatermanConstantGapScoring("ATTAGACTACTTAATATACAGATTTACCCCAATAGA", 
                                                 "ATTAGACTACTTAATATACAGATACCCCAATAGA",
                                                 1.0, 0.0, -0.333, -0.333)

    assert(sw.cigarX.toString === "22M2I12M")
    assert(sw.cigarY.toString === "22M2D12M")
  }

  test("run end to end smith waterman for shorter sequence in longer sequence") {
    // ATTAGACTACTTAATATACAGATTTACCCCAATAGA
    //         ACTTAATATACAGATTTACC
    val sw = new SmithWatermanConstantGapScoring("ATTAGACTACTTAATATACAGATTTACCCCAATAGA", 
                                                 "ACTTAATATACAGATTTACC",
                                                 1.0, 0.0, -0.333, -0.333)

    assert(sw.cigarX.toString === "20M")
    assert(sw.cigarY.toString === "20M")
    assert(sw.xStart === 8)
  }

  test("run end to end smith waterman for shorter sequence in longer sequence, with indel") {
    // ATTAGACTACTTAATATACAGATTTACCCCAATAGA
    //         ACTTAATAT__AGATTTACC
    val sw = new SmithWatermanConstantGapScoring("ATTAGACTACTTAATATACAGATTTACCCCAATAGA", 
                                                 "ACTTAATATAGATTTACC",
                                                 1.0, 0.0, -0.333, -0.333)

    assert(sw.cigarX.toString === "9M2I9M")
    assert(sw.cigarY.toString === "9M2D9M")
    assert(sw.xStart === 8)
  }

}
