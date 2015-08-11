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

import htsjdk.samtools.{ Cigar, CigarOperator }
import org.bdgenomics.adam.util.ImplicitJavaConversions._

object Consensus extends Serializable {

  def generateAlternateConsensus(sequence: String, start: ReferencePosition, cigar: Cigar): Option[Consensus] = {

    var readPos = 0
    var referencePos = start.pos

    if (cigar.getCigarElements.count(elem => elem.getOperator == CigarOperator.I || elem.getOperator == CigarOperator.D) == 1) {
      cigar.getCigarElements.foreach(cigarElement => {
        cigarElement.getOperator match {
          case CigarOperator.I => return Some(new Consensus(sequence.substring(readPos, readPos + cigarElement.getLength), ReferenceRegion(start.referenceName, referencePos, referencePos + 1)))
          case CigarOperator.D => return Some(new Consensus("", ReferenceRegion(start.referenceName, referencePos, referencePos + cigarElement.getLength + 1)))
          case _ => {
            if (cigarElement.getOperator.consumesReadBases && cigarElement.getOperator.consumesReferenceBases) {
              readPos += cigarElement.getLength
              referencePos += cigarElement.getLength
            } else {
              return None
            }
          }
        }
      })
      None
    } else {
      None
    }
  }

}

case class Consensus(consensus: String, index: ReferenceRegion) {

  def insertIntoReference(reference: String, refStart: Long, refEnd: Long): String = {
    if (index.start < refStart || index.start > refEnd || index.end - 1 < refStart || index.end - 1 > refEnd) {
      throw new IllegalArgumentException("Consensus and reference do not overlap: " + index + " vs. " + refStart + " to " + refEnd)
    } else {
      reference.substring(0, (index.start - refStart).toInt) + consensus + reference.substring((index.end - 1 - refStart).toInt)
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
