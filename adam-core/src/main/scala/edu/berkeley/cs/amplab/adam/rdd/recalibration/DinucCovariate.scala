/*
 * Copyright (c) 2014 The Regents of the University of California
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

package edu.berkeley.cs.amplab.adam.rdd.recalibration

import edu.berkeley.cs.amplab.adam.rich.DecadentRead
import edu.berkeley.cs.amplab.adam.rich.DecadentRead._

// TODO: should inherit from something like AbstractCovariate[(DNABase, DNABase)]
class DinucCovariate extends AbstractCovariate[(Char, Char)] {
  // TODO: does this covariate even make sense? why not (isNegative: Boolean, (Char, Char))
  // instead of reversing the sense of the strand? or is the machine's chemistry such that
  // this is what makes the most sense?

  def compute(read: DecadentRead): Seq[Option[(Char, Char)]] = {
    val origSequence = read.sequence.map(_.base)

    if(read.isNegativeRead) {
      computeDinucs(complement(origSequence.reverse)).reverse
    } else {
      computeDinucs(origSequence)
    }
  }

  private def computeDinucs(sequence: Seq[Char]): Seq[Option[(Char, Char)]] = {
    sequence.zipWithIndex.map{ case (current, index) =>
      if(index > 0 && sequence(index - 1) != 'N') {
        Some((sequence(index - 1), current))
      } else {
        None
      }
    }
  }

  private def complement(sequence: Seq[Char]): Seq[Char] = {
    sequence.map{
      case 'A' => 'T'
      case 'T' => 'A'
      case 'C' => 'G'
      case 'G' => 'C'
      case 'N' => 'N'
    }
  }

  override def toCSV(option: Option[Value]): String = option match {
    case None => "NN"
    case Some(value) => "%s%s".format(value._1, value._2)
  }

  override def csvFieldName: String = "Dinuc"

  override def equals(other: Any) = other match {
    case that: DinucCovariate => true
    case _ => false
  }

  override def hashCode = 0x9EAC50CB
}
