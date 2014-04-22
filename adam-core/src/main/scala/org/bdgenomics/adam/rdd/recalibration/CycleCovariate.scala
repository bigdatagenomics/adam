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

package org.bdgenomics.adam.rdd.recalibration

import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.rich.DecadentRead._

// This is based on the CycleCovariate in GATK 1.6.
class CycleCovariate extends AbstractCovariate[Int] {
  def compute(read: DecadentRead): Seq[Option[Int]] = {
    val (initial, increment) = initialization(read)
    Range(0, read.residues.length).map(pos => Some(initial + increment * pos))
  }

  // Returns (initialValue, increment)
  private def initialization(read: DecadentRead): (Int, Int) = {
    if (!read.isNegativeRead) {
      if (read.isSecondOfPair) {
        (-1, -1)
      } else {
        (1, 1)
      }
    } else {
      if (read.isSecondOfPair) {
        (-read.residues.length, 1)
      } else {
        (read.residues.length, -1)
      }
    }
  }

  override def csvFieldName: String = "Cycle"

  override def equals(other: Any) = other match {
    case that: CycleCovariate => true
    case _                    => false
  }

  override def hashCode = 0x83EFAB61
}

