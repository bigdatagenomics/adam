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

import net.sf.samtools.Cigar

abstract class SmithWaterman(xSequence: String, ySequence: String) {

  var max = 0.0
  var maxX = 0
  var maxY = 0

  lazy val scoringMatrix = buildScoringMatrix
  lazy val (cigarX, cigarY, alignmentX, alignmentY) = trackback

  def buildScoringMatrix(): Array[Array[Double]]

  protected def trackback(): (Cigar, Cigar, String, String)

}
