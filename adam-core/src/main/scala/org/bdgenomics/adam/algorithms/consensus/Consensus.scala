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
import org.bdgenomics.adam.models.{ ReferencePosition, ReferenceRegion }
import scala.collection.JavaConversions._

/**
 * Singleton object for generating consensus sequences from alignments.
 *
 * Provides a helper method for turning a local read alignment into a consensus.
 */
private[adam] object Consensus extends Serializable {

  /**
   * Generates a consensus sequence from a local read alignment.
   *
   * Parses the read CIGAR and uses the read sequence to create consensuses. A
   * consensus sequence is generated if an insertion or deletion is observed.
   * When an insertion is observed, the insertion is cut from the read sequence
   * to create the consensus.
   *
   * @note This method can only generate a single consensus from a read. Reads
   *   with more than one INDEL operator are ignored.
   *
   * @param sequence Read sequence.
   * @param start The start position of the read alignment.
   * @param cigar The CIGAR string for the local alignment of the read.
   * @return Returns an optional consensus sequence if there is exactly one
   *   insertion/deletion in the read.
   */
  def generateAlternateConsensus(sequence: String,
                                 start: ReferencePosition,
                                 cigar: Cigar): Option[Consensus] = {

    var readPos = 0
    var referencePos = start.pos

    // do we have a single indel alignment block?
    if (cigar.getCigarElements.count(elem => elem.getOperator == CigarOperator.I ||
      elem.getOperator == CigarOperator.D) == 1) {

      // loop over elements and generate consensuses for indel blocks
      cigar.getCigarElements.flatMap(cigarElement => {
        cigarElement.getOperator match {
          case CigarOperator.I => Some(new Consensus(sequence.substring(readPos,
            readPos + cigarElement.getLength),
            ReferenceRegion(start.referenceName,
              referencePos - 1,
              referencePos)))
          case CigarOperator.D => Some(new Consensus("",
            ReferenceRegion(start.referenceName,
              referencePos,
              referencePos + cigarElement.getLength + 1)))
          case _ => {
            if (cigarElement.getOperator.consumesReadBases &&
              cigarElement.getOperator.consumesReferenceBases) {
              readPos += cigarElement.getLength
              referencePos += cigarElement.getLength
            }
            None
          }
        }
      }).headOption
    } else {
      None
    }
  }
}

/**
 * An INDEL alt allele to be realigned against.
 *
 * A consensus represents an INDEL that will be realigned against. This class
 * stores the alternate allele string, and the location that this allele spans.
 *
 * @param consensus The alternate allele sequence. Empty if a deletion.
 * @param index The reference region that this allele spans.
 */
private[adam] case class Consensus(consensus: String, index: ReferenceRegion) {

  /**
   * Inserts this consensus sequence into a reference genome sequence.
   *
   * @throws IllegalArgumentException If the consensus doesn't overlap with the
   *   provided reference sequence.
   *
   * @param reference The reference genome string of this genomic region.
   * @param rr The genomic region we are splicing into.
   * @return Returns the sequence corresponding to the reference with this
   *   allele spliced in.
   */
  def insertIntoReference(reference: String, rr: ReferenceRegion): String = {
    require(rr.contains(index),
      "Consensus not contained in reference region: %s vs. %s.".format(
        index, rr))

    if (consensus.isEmpty) {
      "%s%s".format(reference.substring(0, (index.start - rr.start).toInt),
        reference.substring((index.end - rr.start - 1).toInt))
    } else {
      "%s%s%s".format(reference.substring(0, (index.start - rr.start + 1).toInt),
        consensus,
        reference.substring((index.end - rr.start).toInt))
    }
  }

  override def toString: String = {
    if (index.start + 1 != index.end) {
      "Deletion over " + index.toString
    } else {
      "Inserted " + consensus + " at " + index.toString
    }
  }
}
