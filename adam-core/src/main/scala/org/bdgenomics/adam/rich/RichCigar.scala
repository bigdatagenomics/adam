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
package org.bdgenomics.adam.rich

import htsjdk.samtools.{ Cigar, CigarOperator, CigarElement }
import scala.annotation.tailrec
import scala.collection.JavaConversions._

/**
 * A wrapper around a CIGAR object that provides additional convenience methods.
 *
 * @param cigar The CIGAR describing an alignment.
 */
private[adam] case class RichCigar(cigar: Cigar) {

  /**
   * Moves a single element in the cigar left by one position.
   *
   * @param index Index of the element to move.
   * @return New cigar with this element moved left.
   */
  def moveLeft(index: Int): RichCigar = {
    // var elements = List[CigarElement]()
    // deepclone instead of empty list initialization
    val elements = cigar.getCigarElements.map(e => new CigarElement(e.getLength, e.getOperator))

    /**
     * Moves an element of a cigar left.
     *
     * @param index Element to move left.
     * @param cigarElements List of cigar elements to move.
     * @return List of cigar elements with single element moved.
     */
    @tailrec def moveCigarLeft(
      head: List[CigarElement],
      index: Int,
      cigarElements: List[CigarElement]): List[CigarElement] = {
      if (index == 1) {
        val elementToTrim = cigarElements.headOption
        val elementToMove: Option[CigarElement] = PartialFunction.condOpt(cigarElements) {
          case _ :: x :: _ => x
        }
        val elementToPad: Option[CigarElement] = PartialFunction.condOpt(cigarElements) {
          case _ :: _ :: x :: _ => x
        }
        val elementsAfterPad = cigarElements.drop(3)

        // if we are at the position to move, then we take one from it and add to the next element
        val elementMovedLeft: Option[CigarElement] = elementToTrim.flatMap { (ett) =>
          if (ett.getLength > 1) {
            Some(new CigarElement(ett.getLength - 1, ett.getOperator))
          } else {
            None
          }
        }

        // if there are no elements afterwards to pad, add a match operator with length 1 to the end
        // if there are elements afterwards, pad the first one
        val elementPadded = elementToPad match {
          case Some(o: CigarElement) => Some(new CigarElement(o.getLength + 1, o.getOperator))
          case _                     => Some(new CigarElement(1, CigarOperator.M))
        }

        // flatmap to remove empty options
        val changedElements: List[CigarElement] = List(elementMovedLeft, elementToMove, elementPadded).flatMap((o: Option[CigarElement]) => o)

        // cat lists together
        head ::: changedElements ::: elementsAfterPad
      } else if (index == 0 || cigarElements.length < 2) {
        head ::: cigarElements
      } else {
        moveCigarLeft(head :+ cigarElements.head, index - 1, cigarElements.tail)
      }
    }

    // create cigar from new list
    RichCigar(new Cigar(moveCigarLeft(List[CigarElement](), index, elements.toList)))
  }

  /**
   * @return Gets the length of the alignment as described by the CIGAR.
   *
   * @see isWellFormed
   */
  def getLength(): Int = {
    cigar.getCigarElements.map(_.getLength).sum
  }

  /**
   * Checks to see if Cigar is well formed. We assume that it is well formed if the cigar lenmgth matches
   * the read length.
   *
   * @param readLength Length of the read sequence.
   * @return Returns true if the given read length matches the alignment length.
   *
   * @see getLength
   */
  def isWellFormed(readLength: Int): Boolean = {
    readLength == getLength
  }
}
