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

// TODO: should inherit from something like AbstractCovariate[(DNABase, DNABase)]
class DinucCovariate extends AbstractCovariate[(Char, Char)] {
  def compute(read: DecadentRead): Seq[Option[(Char, Char)]] = {
    val sequence = read.residues.map(_.base)
    if (read.isNegativeRead) {
      /* Use the reverse-complement of the sequence to get back the original
       * sequence as it was read by the sequencing machine. The sequencer
       * always reads from the 5' to the 3' end of each strand, but the output
       * from the aligner is always in the same sense as the reference, so we
       * use the reverse-complement if this read was originally from the
       * complementary strand.
       */
      dinucs(complement(sequence.reverse)).reverse
    } else {
      dinucs(sequence)
    }
  }

  private def dinucs(sequence: Seq[Char]): Seq[Option[(Char, Char)]] = {
    sequence.zipWithIndex.map {
      case (current, index) =>
        assert(Seq('A', 'C', 'T', 'G', 'N').contains(current))
        def previous = sequence(index - 1)
        if (index > 0 && previous != 'N' && current != 'N') {
          Some((previous, current))
        } else {
          None
        }
    }
  }

  private def complement(sequence: Seq[Char]): Seq[Char] = {
    sequence.map {
      case 'A' => 'T'
      case 'T' => 'A'
      case 'C' => 'G'
      case 'G' => 'C'
      case 'N' => 'N'
    }
  }

  override def toCSV(option: Option[Value]): String = option match {
    case None        => "NN"
    case Some(value) => "%s%s".format(value._1, value._2)
  }

  override def csvFieldName: String = "Dinuc"

  override def equals(other: Any) = other match {
    case that: DinucCovariate => true
    case _                    => false
  }

  override def hashCode = 0x9EAC50CB
}
