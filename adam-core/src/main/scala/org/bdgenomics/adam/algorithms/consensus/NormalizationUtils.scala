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
package org.bdgenomics.adam.algorithms.consensus

import htsjdk.samtools.{ Cigar, CigarOperator }
import org.bdgenomics.adam.rich.RichAlignment
import org.bdgenomics.adam.rich.RichCigar
import org.bdgenomics.formats.avro.Alignment
import scala.annotation.tailrec
import scala.collection.JavaConversions._

/**
 * Utility for left normalizing INDELs in alignments.
 */
private[adam] object NormalizationUtils {

  /**
   * Given a cigar, returns the cigar with the position of the cigar shifted left.
   *
   * @param read Read whose Cigar should be left align.
   * @return Cigar fully moved left.
   */
  def leftAlignIndel(read: Alignment): Cigar = {
    var indelPos = -1
    var pos = 0
    var indelLength = 0
    var readPos = 0
    var referencePos = 0
    var isInsert = false
    val richRead = RichAlignment(read)
    val cigar = richRead.samtoolsCigar

    // find indel in cigar
    cigar.getCigarElements.map(elem => {
      elem.getOperator match {
        case (CigarOperator.I) =>
          if (indelPos == -1) {
            indelPos = pos
            indelLength = elem.getLength
          } else {
            // if we see a second indel, return the cigar
            return cigar
          }
          pos += 1
          isInsert = true
        case (CigarOperator.D) =>
          if (indelPos == -1) {
            indelPos = pos
            indelLength = elem.getLength
          } else {
            // if we see a second indel, return the cigar
            return cigar
          }
          pos += 1
        case _ =>
          pos += 1
          if (indelPos == -1) {
            if (elem.getOperator.consumesReadBases()) {
              readPos += elem.getLength
            }
            if (elem.getOperator.consumesReferenceBases()) {
              referencePos += elem.getLength
            }
          }
      }
    })

    // if there is an indel, shift it, else return
    if (indelPos != -1) {

      val readSeq: String = read.getSequence

      // if an insert, get variant and preceeding bases from read
      // if delete, pick variant, from reference, preceeding bases from read
      val variant = if (isInsert) {
        readSeq.drop(readPos).take(indelLength)
      } else {
        val refSeq = richRead.mdTag.get.getReference(read)
        refSeq.drop(referencePos).take(indelLength)
      }

      // preceeding sequence must always come from read
      // if preceeding sequence does not come from read, we may left shift through a SNP
      val preceeding = readSeq.take(readPos)

      // identify the number of bases to shift by
      val shiftLength = numberOfPositionsToShiftIndel(variant, preceeding)

      shiftIndel(RichCigar(cigar), indelPos, shiftLength)
    } else {
      cigar
    }
  }

  /**
   * Returns the maximum number of bases that an indel can be shifted left during left normalization.
   * Requires that the indel has been trimmed. For an insertion, this should be called on read data
   *
   * @param variant Bases of indel variant sequence.
   * @param preceeding Bases of sequence to left of variant.
   * @return The number of bases to shift an indel for it to be left normalized.
   */
  private[consensus] def numberOfPositionsToShiftIndel(variant: String, preceeding: String): Int = {

    // tail recursive function to determine shift
    @tailrec def numberOfPositionsToShiftIndelAccumulate(variant: String, preceeding: String, accumulator: Int): Int = {
      if (preceeding.length == 0 || preceeding.last != variant.last) {
        // the indel cannot be moved further left if we do not have bases in front of our indel, or if we cannot barrel rotate the indel
        accumulator
      } else {
        // barrel rotate variant
        val newVariant = variant.last + variant.dropRight(1)
        // trim preceeding sequence
        val newPreceeding = preceeding.dropRight(1)
        numberOfPositionsToShiftIndelAccumulate(newVariant, newPreceeding, accumulator + 1)
      }
    }

    numberOfPositionsToShiftIndelAccumulate(variant, preceeding, 0)
  }

  /**
   * Shifts an indel left by n. Is tail call recursive.
   *
   * @param cigar Cigar to shift.
   * @param position Position of element to move.
   * @param shifts Number of bases to shift element.
   * @return Cigar that has been shifted as far left as possible.
   */
  @tailrec private[consensus] def shiftIndel(cigar: RichCigar, position: Int, shifts: Int): Cigar = {
    // generate new cigar with indel shifted by one
    val newCigar = cigar.moveLeft(position)

    // if there are no more shifts to do, or if shifting breaks the cigar, return old cigar
    if (shifts == 0 || !newCigar.isWellFormed(cigar.getLength())) {
      cigar.cigar
    } else {
      shiftIndel(newCigar, position, shifts - 1)
    }
  }
}
