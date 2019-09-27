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
import org.bdgenomics.adam.rich.RichAlignment._
import org.bdgenomics.adam.rich.RichAlignment
import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.immutable.NumericRange
import scala.util.matching.Regex

/**
 * Enumeration describing sequence events in an MD tag:
 *
 * * Match is a sequence match (i.e., every base in the aligned sequence matches
 *   the reference sequence)
 * * Mismatch is a sequence mismatch (there are bases that do not match the
 *   reference sequence)
 * * Delete indicates that bases that were present in the reference sequence
 *   were deleted in the read sequence
 */
object MdTagEvent extends Enumeration {
  val Match, Mismatch, Delete = Value
}

/**
 * Companion object to MdTag case class. Provides methods for building an
 * MdTag model from a read.
 */
object MdTag {

  private val digitPattern = new Regex("\\d+")

  // for description, see base enum in adam schema
  private val basesPattern = new Regex("[AaGgCcTtNnUuKkMmRrSsWwBbVvHhDdXxYy]+")

  /**
   * Builds an MD tag object from the string representation of an MD tag and the
   * start position of the read.
   *
   * @param mdTagInput Textual MD tag/mismatchingPositions string.
   * @param referenceStart The read start position.
   * @param cigar Cigar operators for the read
   * @return Returns a populated MD tag.
   */
  def apply(
    mdTagInput: String,
    referenceStart: Long,
    cigar: Cigar): MdTag = {

    var matches = List[NumericRange[Long]]()
    var mismatches = Map[Long, Char]()
    var deletions = Map[Long, Char]()

    val numCigarOperators = cigar.numCigarElements()
    var cigarIdx = 0
    var mdTagStringOffset = 0
    var referencePos = referenceStart
    var cigarOperatorIndex = 0
    var usedMatchingBases = 0

    if (mdTagInput == null || mdTagInput == "0") {
      new MdTag(referenceStart, List(), Map(), Map())
    } else {
      val mdTag = mdTagInput.toUpperCase
      // Parse all CIGAR operators until the end of the mdTag string
      while (cigarIdx < numCigarOperators && mdTagStringOffset < mdTagInput.length) {
        val cigarElement = cigar.getCigarElement(cigarIdx)
        val nextDigit = digitPattern.findPrefixOf(mdTag.substring(mdTagStringOffset))
        (cigarElement.getOperator, nextDigit) match {

          case (_, None) if mdTagStringOffset == 0 =>
            throw new IllegalArgumentException(s"MdTag $mdTagInput does not start with a digit")

          case (_, Some(matchingBases)) if matchingBases.toInt == 0 =>
            mdTagStringOffset += 1

          case (CigarOperator.M | CigarOperator.EQ, Some(matchingBases)) =>
            val numMatchingBases = math.min(cigarElement.getLength - cigarOperatorIndex, matchingBases.toInt - usedMatchingBases)

            if (numMatchingBases > 0) {
              matches ::= NumericRange(referencePos, referencePos + numMatchingBases, 1L)

              // Move the reference position the length of the matching sequence
              referencePos += numMatchingBases

              // Move ahead in the current CIGAR operator
              cigarOperatorIndex += numMatchingBases

              // Move ahead in the current MdTag digit
              usedMatchingBases += numMatchingBases
            }

            if (matchingBases.toInt == usedMatchingBases) {
              mdTagStringOffset += matchingBases.length
              usedMatchingBases = 0
            }

            // If the M operator has been fully read move on to the next operator
            if (cigarOperatorIndex == cigarElement.getLength) {
              cigarIdx += 1
              cigarOperatorIndex = 0
            }

          case (CigarOperator.M | CigarOperator.X, None) =>
            basesPattern.findPrefixOf(mdTag.substring(mdTagStringOffset)) match {
              // Must have found a base or digit pattern in CigarOperator M
              case None =>
                throw new IllegalArgumentException(
                  s"No match or mismatched bases found for ${cigar.toString} in MDTag $mdTag"
                )
              case Some(mismatchedBases) =>
                mismatchedBases.foreach {
                  base =>
                    mismatches += (referencePos -> base)
                    referencePos += 1
                }
                mdTagStringOffset += mismatchedBases.length
                cigarOperatorIndex += mismatchedBases.length
            }
            // If the M operator has been fully read move on to the next operator
            if (cigarOperatorIndex == cigarElement.getLength) {
              cigarIdx += 1
              cigarOperatorIndex = 0
            }

          case (CigarOperator.DELETION, None) =>
            mdTag.charAt(mdTagStringOffset) match {
              case '^' =>
                // Skip ahead of the deletion '^' character
                mdTagStringOffset += 1
                basesPattern.findPrefixOf(mdTag.substring(mdTagStringOffset)) match {
                  case None =>
                    throw new IllegalArgumentException(s"No deleted bases found ${cigar.toString} in MDTag $mdTag")
                  case Some(deletedBases) if deletedBases.length == cigarElement.getLength =>
                    deletedBases.foreach {
                      base =>
                        deletions += (referencePos -> base)
                        referencePos += 1
                    }
                    mdTagStringOffset += deletedBases.length
                  case Some(deletedBases) =>
                    throw new IllegalArgumentException(
                      s"Element ${cigarElement.getLength}${cigarElement.getOperator.toString} in cigar ${cigar.toString} contradicts number of bases listed in MDTag: ^${deletedBases}"
                    )
                }
                cigarIdx += 1
                cigarOperatorIndex = 0

              case _ =>
                throw new IllegalArgumentException(
                  s"CIGAR ${cigar.toString} indicates deletion found but no deleted bases in MDTag $mdTagInput"
                )
            }

          case (CigarOperator.N, _) =>
            referencePos += cigarElement.getLength
            cigarIdx += 1
            cigarOperatorIndex = 0

          case (CigarOperator.INSERTION | CigarOperator.H | CigarOperator.S | CigarOperator.P, _) =>
            cigarIdx += 1
        }
      }
      new MdTag(referenceStart, matches, mismatches, deletions)
    }
  }

  /**
   * Helper function for moving the alignment of a read.
   *
   * @param reference String corresponding to the reference sequence overlapping this read.
   * @param sequence String corresponding to the sequence of read bases.
   * @param newCigar Cigar for the new alignment of this read.
   * @param readStart Start position of the new read alignment.
   * @return MdTag corresponding to the new alignment.
   */
  private def moveAlignment(reference: String, sequence: String, newCigar: Cigar, readStart: Long): MdTag = {
    var referencePos = 0
    var readPos = 0

    var matches: List[NumericRange[Long]] = List[NumericRange[Long]]()
    var mismatches: Map[Long, Char] = Map[Long, Char]()
    var deletions: Map[Long, Char] = Map[Long, Char]()

    // loop over cigar elements and fill sets
    newCigar.getCigarElements.foreach(cigarElement => {
      cigarElement.getOperator match {
        case CigarOperator.M => {
          var rangeStart = 0L
          var inMatch = false

          // dirty dancing to recalculate match sets
          for (i <- 0 until cigarElement.getLength) {
            if (reference(referencePos) ==
              sequence(readPos)) {
              if (!inMatch) {
                rangeStart = referencePos.toLong
                inMatch = true
              }
            } else {
              if (inMatch) {
                // we are no longer inside of a match, so use until
                matches = ((rangeStart + readStart) until (referencePos.toLong + readStart)) :: matches
                inMatch = false
              }

              mismatches += ((referencePos + readStart) -> reference(referencePos))
            }

            readPos += 1
            referencePos += 1
          }

          // we are currently in a match, so use to
          if (inMatch) {
            matches = ((rangeStart + readStart) until (referencePos.toLong + readStart)) :: matches
          }
        }
        case CigarOperator.D => {
          for (i <- 0 until cigarElement.getLength) {
            deletions += ((referencePos + readStart) -> reference(referencePos))

            referencePos += 1
          }
        }
        case _ => {
          if (cigarElement.getOperator.consumesReadBases) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases) {
            throw new IllegalArgumentException("Cannot handle operator: " + cigarElement.getOperator)
          }
        }
      }
    })

    new MdTag(readStart, matches, mismatches, deletions)
  }

  /**
   * Given a single read and an updated Cigar, recalculates the MD tag.
   *
   * @note For this method, the read must be mapped and adjustments to the cigar must not have led to a change in the alignment start position.
   * If the alignment position has been changed, then the moveAlignment function with a new reference must be used.
   *
   * @param read Record for current alignment.
   * @param newCigar Realigned cigar string.
   * @return Returns an MD tag for the new read alignment.
   *
   * @see apply
   */
  def moveAlignment(read: RichAlignment, newCigar: Cigar): MdTag = {
    val reference = read.mdTag.get.getReference(read.record)

    moveAlignment(reference, read.record.getSequence, newCigar, read.record.getStart)
  }

  /**
   * Given a single read, an updated reference, and an updated Cigar, this method calculates a new MD tag.
   *
   * @note If the alignment start position has not changed (e.g., the alignment change is that an indel in the read was left normalized), then
   * the two argument (RichADAMRecord, Cigar) moveAlignment function should be used.
   *
   * @param read Read to write a new alignment for.
   * @param newCigar The Cigar for the new read alignment.
   * @param newReference Reference sequence to write alignment against.
   * @param newAlignmentStart The position of the new read alignment.
   * @return Returns an MD tag for the new read alignment.
   *
   * @see apply
   */
  def moveAlignment(read: RichAlignment, newCigar: Cigar, newReference: String, newAlignmentStart: Long): MdTag = {
    moveAlignment(newReference, read.record.getSequence, newCigar, newAlignmentStart)
  }

  /**
   * Creates an MD tag object given a read, and the accompanying reference alignment.
   *
   * @param read Sequence of bases in the read.
   * @param reference Reference sequence that the read is aligned to.
   * @param cigar The CIGAR for the reference alignment.
   * @param start The start position of the read alignment.
   * @return Returns a populated MD tag.
   */
  def apply(read: String, reference: String, cigar: Cigar, start: Long): MdTag = {
    var matchCount = 0
    var delCount = 0
    var string = ""
    var readPos = 0
    var refPos = 0

    // loop over all cigar elements
    cigar.getCigarElements.foreach(cigarElement => {
      cigarElement.getOperator match {
        case CigarOperator.M | CigarOperator.EQ | CigarOperator.X => {
          for (i <- 0 until cigarElement.getLength) {
            if (read(readPos) == reference(refPos)) {
              matchCount += 1
            } else {
              string += matchCount.toString + reference(refPos)
              matchCount = 0
            }
            readPos += 1
            refPos += 1
            delCount = 0
          }
        }
        case CigarOperator.D => {
          for (i <- 0 until cigarElement.getLength) {
            if (delCount == 0) {
              string += matchCount.toString + "^"
            }
            string += reference(refPos)

            matchCount = 0
            delCount += 1
            refPos += 1
          }
        }
        case _ => {
          if (cigarElement.getOperator.consumesReadBases) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases) {
            throw new IllegalArgumentException("Cannot handle operator: " + cigarElement.getOperator)
          }
        }
      }
    })

    string += matchCount.toString

    apply(string, start, cigar)
  }
}

/**
 * Represents the mismatches and deletions present in a read that has been
 * aligned to a reference genome. The MD tag can be used to reconstruct
 * the reference that an aligned read overlaps.
 *
 * @param start Start position of the alignment.
 * @param matches A list of the ranges over which the read has a perfect
 *                sequence match.
 * @param mismatches A map of all the locations where a base mismatched.
 * @param deletions A map of all locations where a base was deleted.
 */
case class MdTag(
    val start: Long,
    val matches: immutable.List[NumericRange[Long]],
    val mismatches: immutable.Map[Long, Char],
    val deletions: immutable.Map[Long, Char]) {

  /**
   * Returns whether a base is a match against the reference.
   *
   * @param pos Reference based position to check.
   * @return True if base matches reference. False means that the base may be either a mismatch or a deletion.
   */
  def isMatch(pos: Long): Boolean = {
    matches.exists(_.contains(pos))
  }

  /**
   * Returns whether a base is a match against the reference.
   *
   * @param pos ReferencePosition object describing where to check.
   * @return True if base matches reference. False means that the base may be either a mismatch or a deletion.
   */
  def isMatch(pos: ReferencePosition): Boolean = {
    matches.exists(_.contains(pos.pos))
  }

  /**
   * Returns the mismatched base at a position.
   *
   * @param pos Reference based position.
   * @return The base at this position in the reference.
   */
  def mismatchedBase(pos: Long): Option[Char] = {
    mismatches.get(pos)
  }

  /**
   * Returns the base that was deleted at a position.
   *
   * @param pos Reference based position.
   * @return The base that was deleted at this position in the reference.
   */
  def deletedBase(pos: Long): Option[Char] = {
    deletions.get(pos)
  }

  /**
   * Returns whether this read has any mismatches against the reference.
   *
   * @return True if this read has mismatches. We do not return true if the read has no mismatches but has deletions.
   */
  def hasMismatches: Boolean = {
    mismatches.nonEmpty
  }

  /**
   * Returns the number of mismatches against the reference.
   *
   * @return Number of mismatches against the reference
   */
  def countOfMismatches: Int = {
    mismatches.size
  }

  /**
   * Returns the end position of the record described by this MD tag.
   *
   * @return The reference based end position of this tag.
   */
  def end(): Long = {
    val ends = matches.map(_.end - 1) ::: mismatches.keys.toList ::: deletions.keys.toList
    ends.max
  }

  /**
   * Given a read, returns the reference.
   *
   * @param read A read for which one desires the reference sequence.
   * @param withGaps If true, applies INDEL gaps to the reference. Else, returns
   *   the raw reference sequence.
   * @return A string corresponding to the reference overlapping this read.
   */
  def getReference(read: RichAlignment, withGaps: Boolean = false): String = {
    getReferenceSequence(read.getSequence,
      read.samtoolsCigar,
      read.getStart,
      withGaps = withGaps)
  }

  /**
   * Given a read sequence, cigar, and a reference start position, returns the reference.
   *
   * @param readSequence The base sequence of the read.
   * @param cigar The cigar for the read.
   * @param referenceFrom The starting point of this read alignment vs. the reference.
   * @param withGaps If true, applies INDEL gaps to the reference. Else, returns
   *   the raw reference sequence.
   * @return A string corresponding to the reference overlapping this read.
   */
  private def getReferenceSequence(readSequence: String,
                                   cigar: Cigar,
                                   referenceFrom: Long,
                                   withGaps: Boolean = false): String = {

    var referencePos = start
    var readPos = 0
    var reference = ""

    // loop over all cigar elements
    cigar.getCigarElements.foreach(cigarElement => {
      cigarElement.getOperator match {
        case CigarOperator.M | CigarOperator.EQ | CigarOperator.X => {
          // if we are a match, loop over bases in element
          for (i <- 0 until cigarElement.getLength) {
            // if a mismatch, get from the mismatch set, else pull from read
            mismatches.get(referencePos) match {
              case Some(base) => reference += base
              case _          => reference += readSequence(readPos)
            }

            readPos += 1
            referencePos += 1
          }
        }
        case CigarOperator.D => {
          if (!withGaps) {
            // if a delete, get from the delete pool
            for (i <- 0 until cigarElement.getLength) {
              reference += {
                deletions.get(referencePos) match {
                  case Some(base) => base
                  case _          => throw new IllegalStateException("Could not find deleted base at cigar offset " + i)
                }
              }
              referencePos += 1
            }
          } else {
            referencePos += cigarElement.getLength
          }
        }
        case _ => {
          if (cigarElement.getOperator.consumesReadBases) {
            val insLength = cigarElement.getLength
            if (withGaps) {
              reference += ("_" * insLength)
            }
            readPos += insLength
          }
          if (cigarElement.getOperator.consumesReferenceBases) {
            throw new IllegalArgumentException("Cannot handle operator: " + cigarElement.getOperator)
          }
        }
      }
    })

    reference
  }

  /**
   * Converts an MdTag object to a properly formatted MD string.
   *
   * @return MD string corresponding to [0-9]+(([A-Z]|\&#94;[A-Z]+)[0-9]+)
   * @see http://zenfractal.com/2013/06/19/playing-with-matches/
   */
  override def toString: String = {
    if (matches.isEmpty && mismatches.isEmpty && deletions.isEmpty) {
      "0"
    } else {
      var mdString = ""
      var lastWasMatch = false
      var lastWasDeletion = false
      var matchRun = 0

      // loop over positions in tag - FSM for building string
      (start to end).foreach(i => {
        if (isMatch(i)) {
          if (lastWasMatch) {
            // if in run of matches, increment count
            matchRun += 1
          } else {
            // if first match, reset match count and set flag
            matchRun = 1
            lastWasMatch = true
          }

          // clear state
          lastWasDeletion = false
        } else if (deletions.contains(i)) {
          if (!lastWasDeletion) {
            // write match count before deletion
            mdString += (if (lastWasMatch) matchRun.toString else "0")
            // add deletion caret
            mdString += "^"

            // set state
            lastWasMatch = false
            lastWasDeletion = true
          }

          // add deleted base
          mdString += deletions(i)
        } else if (mismatches.contains(i)) {
          // write match count before mismatch
          mdString += (if (lastWasMatch) matchRun.toString else "0")

          mdString += mismatches(i)

          // clear state
          lastWasMatch = false
          lastWasDeletion = false
        }
      })

      // if we have more matches, write count
      mdString += (if (lastWasMatch) matchRun.toString else "0")

      mdString
    }
  }

  /**
   * We implement equality checking by seeing whether two MD tags are at the
   * same position and have the same value.
   *
   * @param other An object to compare to.
   * @return True if the object is an MD tag at the same position and with the
   *         same string value. Else, false.
   */
  override def equals(other: Any): Boolean = other match {
    case that: MdTag => toString == that.toString && start == that.start
    case _           => false
  }

  /**
   * We can check equality against MdTags.
   *
   * @param other Object to see if we can compare against.
   * @return Returns True if the object is an MdTag.
   */
  def canEqual(other: Any): Boolean = other.isInstanceOf[MdTag]

  /**
   * @return We implement hashing by hashing the string representation of the
   *         MD tag.
   */
  override def hashCode: Int = toString().hashCode
}
