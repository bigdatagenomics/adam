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
import scala.annotation.tailrec

/**
 * An error covariate that tracks quality score estimation errors that are
 * correlated with two nucleotides appearing in sequence in a read.
 */
private[adam] class DinucCovariate extends Covariate[(Char, Char)] {

  /**
   * @param read The read to compute the covariate for.
   * @return Returns an array of dinucleotides.
   */
  def compute(read: Alignment): Array[(Char, Char)] = {
    val sequence = read.getSequence
    if (read.getReadNegativeStrand) {

      /* Use the reverse-complement of the sequence to get back the original
       * sequence as it was read by the sequencing machine. The sequencer
       * always reads from the 5' to the 3' end of each strand, but the output
       * from the aligner is always in the same sense as the reference, so we
       * use the reverse-complement if this read was originally from the
       * complementary strand.
       */
      revDinucs(complement(sequence))
    } else {
      fwdDinucs(sequence)
    }
  }

  private[recalibration] def fwdDinucs(sequence: String): Array[(Char, Char)] = {
    val array = new Array[(Char, Char)](sequence.length)
    dinucs(sequence, false, array)
  }

  private[recalibration] def revDinucs(sequence: String): Array[(Char, Char)] = {
    val array = new Array[(Char, Char)](sequence.length)
    dinucs(sequence, true, array)
  }

  @tailrec private def dinucs(sequence: String,
                              swap: Boolean,
                              array: Array[(Char, Char)],
                              idx: Int = 0): Array[(Char, Char)] = {
    if (idx < 0 || idx >= sequence.length) {
      array
    } else {
      val current = sequence(idx)
      // previously, this was implemented as a lookup into a set
      // unrolling this and not using a set is 40% faster
      //
      // this is ugly, but as they say,
      // "At 50, everyone has the face they deserve"
      require(current == 'A' ||
        current == 'C' ||
        current == 'G' ||
        current == 'T' ||
        current == 'N',
        "Saw invalid base %s. Accepted bases are A,C,G,T,N.".format(current))
      val elem = if ((!swap && idx > 0) || (swap && idx < sequence.length - 1)) {
        val previous = if (swap) {
          sequence(idx + 1)
        } else {
          sequence(idx - 1)
        }
        if (previous != 'N' && current != 'N') {
          (previous, current)
        } else {
          ('N', 'N')
        }
      } else {
        ('N', 'N')
      }
      array(idx) = elem
      dinucs(sequence, swap, array, idx = idx + 1)
    }
  }

  override def toCSV(cov: (Char, Char)): String = {
    "%s%s".format(cov._1, cov._2)
  }

  private def complement(sequence: String): String = {
    sequence.map {
      case 'A' => 'T'
      case 'T' => 'A'
      case 'C' => 'G'
      case 'G' => 'C'
      case _   => 'N'
    }
  }

  val csvFieldName: String = "Dinuc"
}
