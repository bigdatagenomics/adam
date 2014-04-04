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

package org.bdgenomics.adam.rich

import net.sf.samtools.{ Cigar, CigarOperator, TextCigarCodec, CigarElement }
import org.bdgenomics.adam.util.ImplicitJavaConversions._
import scala.annotation.tailrec

object RichCigar {

  def apply(cigar: Cigar) = {
    new RichCigar(cigar)
  }

  implicit def cigarToRichCigar(cigar: Cigar): RichCigar = new RichCigar(cigar)

}

class RichCigar(cigar: Cigar) {

  lazy val numElements: Int = cigar.numCigarElements

  // number of alignment blocks is defined as the number of segments in the sequence that are a cigar match
  lazy val numAlignmentBlocks: Int = {
    cigar.getCigarElements.map(element => {
      element.getOperator match {
        case CigarOperator.M => 1
        case _ => 0
      }
    }).reduce(_ + _)
  }

  /**
   * Moves a single element in the cigar left by one position.
   *
   * @param index Index of the element to move.
   * @return New cigar with this element moved left.
   */
  def moveLeft(index: Int): Cigar = {
    // var elements = List[CigarElement]()
    // deepclone instead of empty list initialization
    var elements = cigar.getCigarElements.map(e => new CigarElement(e.getLength, e.getOperator))

    /**
     * Moves an element of a cigar left.
     *
     * @param index Element to move left.
     * @param cigarElements List of cigar elements to move.
     * @return List of cigar elements with single element moved.
     */
    @tailrec def moveCigarLeft(head: List[CigarElement],
      index: Int,
      cigarElements: List[CigarElement]): List[CigarElement] = {
      if (index == 1) {
        val elementToTrim = cigarElements.head
        val elementToMove: Option[CigarElement] = Some(cigarElements(1))
        val elementToPad: Option[CigarElement] = if (cigarElements.length > 2) {
          Some(cigarElements(2))
        } else {
          None
        }
        val elementsAfterPad = if (cigarElements.length > 4) {
          cigarElements.drop(3)
        } else {
          List[CigarElement]()
        }

        // if we are at the position to move, then we take one from it and add to the next element
        val elementMovedLeft: Option[CigarElement] = if (elementToTrim.getLength > 1) {
          Some(new CigarElement(elementToTrim.getLength - 1, elementToTrim.getOperator))
        } else {
          None
        }

        // if there are no elements afterwards to pad, add a match operator with length 1 to the end
        // if there are elements afterwards, pad the first one
        val elementPadded = elementToPad match {
          case Some(o: CigarElement) => Some(new CigarElement(o.getLength + 1, o.getOperator))
          case _ => Some(new CigarElement(1, CigarOperator.M))
        }

        // flatmap to remove empty options
        val changedElements: List[CigarElement] = List(elementMovedLeft, elementToMove, elementPadded).flatMap((o: Option[CigarElement]) => o)

        // cat lists together
        head ::: changedElements ::: elementsAfterPad
      } else if (index == 0 || cigarElements.length < 2) {
        head ::: cigarElements
      } else {
        moveCigarLeft(head ::: List(cigarElements.head), index - 1, cigarElements.tail)
      }
    }

    // create cigar from new list
    new Cigar(moveCigarLeft(List[CigarElement](), index, elements))
  }

  def getLength(): Int = {
    cigar.getCigarElements.map(_.getLength).reduce(_ + _)
  }

  /**
   * Checks to see if Cigar is well formed. We assume that it is well formed if the cigar lenmgth matches
   * the read length.
   *
   * @param readLength Length of the read sequence.
   */
  def isWellFormed(readLength: Int): Boolean = {
    readLength == getLength
  }

}
