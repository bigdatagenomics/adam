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

import org.bdgenomics.adam.util.PhredUtils
import scala.annotation.tailrec

/**
 * This class represents a sequence of bases where the sequence is as yet
 * undetermined, but probabilities are available for the base-wise composition
 * of each position in the sequence.
 */
case class ProbabilisticSequence(sequence: Array[Array[Double]]) {

  private[models] def maxOf(idx: Int): Int = {
    @tailrec def getMaxIdx(currMax: Double, currMaxIdx: Int, currIdx: Int): Int = {
      // return if we pass 3
      if (currIdx >= 3) {
        currMaxIdx
      } else {
        // update idx
        val newIdx = currIdx + 1

        // do we have a new max?
        val (newMax, newMaxIdx) = if (sequence(idx)(newIdx) > currMax) {
          (sequence(idx)(newIdx), newIdx)
        } else {
          (currMax, currMaxIdx)
        }

        // recurse
        getMaxIdx(newMax, newMaxIdx, newIdx)
      }
    }

    getMaxIdx(sequence(idx)(0), 0, 0)
  }

  private[models] def idxToBase(idx: Int): Char = idx match {
    case 0 => 'A'
    case 1 => 'C'
    case 2 => 'G'
    case 3 => 'T'
    case _ => throw new IllegalArgumentException("Index out of range.")
  }

  def softMax() {
    (0 until sequence.length).foreach(i => {
      val normalizeFactor = sequence(i).sum

      // normalize all values
      (0 to 3).foreach(j => sequence(i)(j) = sequence(i)(j) / normalizeFactor)
    })
  }

  def toSequence(): (String, Array[Int]) = {
    val sb = new StringBuilder
    val phredArray = new Array[Int](sequence.length)

    // loop over bases and pick the best
    (0 until sequence.length).foreach(i => {
      // get max index at position
      val maxIdx = maxOf(i)

      // append base to string builder
      sb += idxToBase(maxIdx)

      // set phred score at position
      phredArray(i) = PhredUtils.successProbabilityToPhred(sequence(i)(maxIdx))
    })

    (sb.toString, phredArray)
  }
}
