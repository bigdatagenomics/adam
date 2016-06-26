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
package org.bdgenomics.adam.algorithms.smithwaterman

/**
 * Performs a pairwise alignment of two sequences using constant penalties.
 *
 * @see scoreFn
 *
 * @param xSequence The first sequence in the pair.
 * @param ySequence The second sequence in the pair.
 * @param wMatch The alignment gap penalty for a residue match.
 * @param wMismatch The alignment gap penalty for a residue mismatch.
 * @param wInsert The alignment gap penalty for an insertion in the first
 *   sequence.
 * @param wDelete The alignment gap penalty for a deletion in the first
 *   sequence.
 */
case class SmithWatermanConstantGapScoring(xSequence: String,
                                           ySequence: String,
                                           wMatch: Double,
                                           wMismatch: Double,
                                           wInsert: Double,
                                           wDelete: Double) extends SmithWatermanGapScoringFromFn {

  /**
   * Scores residues using scoring constants.
   *
   * * If a deletion is observed (xResidue is a gap), then the deletion
   *   penalty is returned.
   * * If an insertion is observed (yResidue is a gap), then the insertion
   *   penalty is returned.
   * * Else, the residues are compared, and either the match or mismatch
   *   penalty is returned.
   *
   * The position indices are ignored, so no affine/open-continue gap model is
   * incorporated.
   *
   * @param xPos Residue position from first sequence.
   * @param yPos Residue position from second sequence.
   * @param xResidue Residue from first sequence.
   * @param yResidue Residue from second sequence.
   * @return Returns the gap score for this residue pair.
   */
  protected def scoreFn(xPos: Int, yPos: Int, xResidue: Char, yResidue: Char): Double = {
    if (xResidue == yResidue) {
      wMatch
    } else if (xResidue == '_') {
      wDelete
    } else if (yResidue == '_') {
      wInsert
    } else {
      wMismatch
    }
  }
}
