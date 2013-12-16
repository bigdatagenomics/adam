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
package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.avro.ADAMPileup
import org.apache.spark.rdd.RDD

/**
 * Class representing a set of pileup bases at a specific locus.
 *
 * @param position Position on the reference genome. Coordinate system is 0 based.
 * @param pileups A list representing the bases at this locus.
 */
case class ADAMRod (position: Long, pileups: List[ADAMPileup]) {
  // all bases must be at the same position
  require(pileups.forall(_.getPosition.toLong == position))

  lazy val isSingleSample: Boolean = pileups.map(_.getRecordGroupSample).distinct.length == 1

  /**
   * Splits this rod out by samples.
   *
   * @return A list of rods, each corresponding to a single sample.
   */
  def splitBySamples (): List[ADAMRod] = {
    if(isSingleSample) {
      List(new ADAMRod(position, pileups))
    } else {
      pileups.groupBy(_.getRecordGroupSample).values.toList.map(pg => new ADAMRod(position, pg))
    }
  }
  
}
