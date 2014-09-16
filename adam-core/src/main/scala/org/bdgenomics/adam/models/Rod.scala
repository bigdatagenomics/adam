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
package org.bdgenomics.adam.models

import org.bdgenomics.formats.avro.Pileup

/**
 * Class representing a set of pileup bases at a specific locus.
 *
 * @param position Position on the reference genome.
 * @param pileups A list representing the bases at this locus.
 */
case class Rod(position: ReferencePosition, pileups: List[Pileup]) {
  // all bases must be at the same position
  require(pileups.forall(ReferencePosition(_) == position))

  lazy val isSingleSample: Boolean = pileups.map(_.getSampleId).distinct.length == 1

  /**
   * Splits this rod out by samples.
   *
   * @return A list of rods, each corresponding to a single sample.
   */
  def splitBySamples(): List[Rod] = {
    if (isSingleSample) {
      List(new Rod(position, pileups))
    } else {
      pileups.groupBy(_.getSampleId).values.toList.map(pg => new Rod(position, pg))
    }
  }
}
