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

import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions._
import net.sf.samtools.{Cigar, CigarOperator, CigarElement}
import scala.collection.immutable.NumericRange

object Consensus {

  def generateAlternateConsensus (sequence: String, start: Long, cigar: Cigar): Option[Consensus] = {
    
    var readPos = 0
    var referencePos = start

    if (cigar.getCigarElements.filter(elem => elem.getOperator == CigarOperator.I || elem.getOperator == CigarOperator.D).length == 1) {
      cigar.getCigarElements.foreach(cigarElement => {
        cigarElement.getOperator match {
          case CigarOperator.I => return Some(new Consensus(sequence.substring(readPos, readPos + cigarElement.getLength), referencePos to referencePos))
          case CigarOperator.D => return Some(new Consensus("", referencePos until (referencePos + cigarElement.getLength)))
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

case class Consensus (consensus: String, index: NumericRange[Long]) {

  def insertIntoReference (reference: String, refStart: Long, refEnd: Long): String = {
    if (index.head < refStart || index.head > refEnd || index.end < refStart || index.end > refEnd) {
      throw new IllegalArgumentException("Consensus and reference do not overlap: " + index + " vs. " + refStart + " to " + refEnd)
    } else {
      reference.substring(0, (index.head - refStart).toInt) + consensus + reference.substring((index.end - refStart).toInt)
    }
  }

}
