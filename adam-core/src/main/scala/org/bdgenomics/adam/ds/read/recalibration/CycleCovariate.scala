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
package org.bdgenomics.adam.ds.read.recalibration

import org.bdgenomics.formats.avro.Alignment

/**
 * A covariate representing sequencer base errors that are correlated with
 * sequencer cycle. Maps first of pair reads into positive cycles and
 * second of pair reads into negative cycles.
 */
private[adam] class CycleCovariate extends Covariate[Int] {

  /**
   * @param read The read to compute cycle covariates for.
   * @return Returns an integer array where the array elements indicate the
   *   sequencer cycle that a base was from.
   */
  def compute(read: Alignment): Array[Int] = {
    val (initial, increment) = initialization(read)
    val seqLength = read.getSequence.length
    val cycleArray = new Array[Int](seqLength)
    var idx = 0
    var currVal = initial
    while (idx < seqLength) {
      cycleArray(idx) = currVal
      idx += 1
      currVal += increment
    }
    cycleArray
  }

  /**
   * @param read The read to generate cycle values for. Used to get the
   *   pairing, strand, and read length.
   * @return Returns (initialValue, increment)
   */
  private def initialization(read: Alignment): (Int, Int) = {
    if (!read.getReadNegativeStrand) {
      if (read.getReadInFragment != 0) {
        (-1, -1)
      } else {
        (1, 1)
      }
    } else {
      val readLength = read.getSequence.length
      if (read.getReadInFragment != 0) {
        (-readLength, 1)
      } else {
        (readLength, -1)
      }
    }
  }

  val csvFieldName: String = "Cycle"
}

