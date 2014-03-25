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
package edu.berkeley.cs.amplab.adam.util

import scala.collection.immutable
import scala.collection.immutable.NumericRange
import scala.util.matching.Regex
import net.sf.samtools.{Cigar, CigarOperator, CigarElement}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._

object MdTagEvent extends Enumeration {
  val Match, Mismatch, Delete = Value
}

object MdTag {

  private val digitPattern = new Regex("\\d+")
  // for description, see base enum in adam schema
  private val basesPattern = new Regex("[AaGgCcTtNnUuKkMmRrSsWwBbVvHhDdXxYy]+")

  def apply(mdTagInput: String, referenceStart: Long): MdTag = {
    var matches = List[NumericRange[Long]]()
    var mismatches = Map[Long, Char]()
    var deletes = Map[Long, Char]()

    if (mdTagInput != null && mdTagInput.length > 0) {
      val mdTag = mdTagInput.toUpperCase
      val end = mdTag.length

      var offset = 0
      var referencePos = referenceStart

      def readMatches(errMsg: String): Unit = {
        digitPattern.findPrefixOf(mdTag.substring(offset)) match {
          case None => throw new IllegalArgumentException(errMsg)
          case Some(s) =>
            val length = s.toInt
            if (length > 0) {
              matches ::= NumericRange(referencePos, referencePos + length, 1L)
            }
            offset += s.length
            referencePos += length
        }
      }

      readMatches("MD tag must start with a digit")

      while (offset < end) {
        val mdTagType = {
          if (mdTag.charAt(offset) == '^') {
            offset += 1
            MdTagEvent.Delete
          } else {
            MdTagEvent.Mismatch
          }
        }
        basesPattern.findPrefixOf(mdTag.substring(offset)) match {
          case None => throw new IllegalArgumentException("Failed to find deleted or mismatched bases after a match: %s".format(mdTagInput))
          case Some(bases) =>
            mdTagType match {
              case MdTagEvent.Delete =>
                bases.foreach {
                  base =>
                    deletes += (referencePos -> base)
                    referencePos += 1
                }
              case MdTagEvent.Mismatch =>
                bases.foreach {
                  base =>
                    mismatches += (referencePos -> base)
                    referencePos += 1
                }
            }
            offset += bases.length
        }
        readMatches("MD tag should have matching bases after mismatched or missing bases")
      }
    }

    new MdTag(matches, mismatches, deletes)
  }

  /**
   * From an updated read alignment, writes a new MD tag.
   *
   * @param read Record for current alignment.
   * @param newCigar Realigned cigar string.
   * @return Returns an MD tag for the new read alignment.
   *
   * @see moveAlignment
   */
  def apply(read: RichADAMRecord, newCigar: Cigar): MdTag = {
    moveAlignment(read, newCigar)
  }

  /**
   * From an updated read alignment, writes a new MD tag.
   * 
   * @param read Read to write a new alignment for.
   * @param newReference Reference sequence to write alignment against.
   * @param newCigar The Cigar for the new read alignment.
   * @param newAlignmentStart The position of the new read alignment.
   * @return Returns an MD tag for the new read alignment.
   *
   * @see moveAlignment
   */
  def apply(read: RichADAMRecord, newCigar: Cigar, newReference: String, newAlignmentStart: Long): MdTag = {
    moveAlignment(read, newCigar, newReference, newAlignmentStart)
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
  private def moveAlignment (reference: String, sequence: String, newCigar: Cigar, readStart: Long): MdTag = {
    var referencePos = 0
    var readPos = 0

    var matches: List[NumericRange[Long]] = List[NumericRange[Long]]()
    var mismatches: Map[Long, Char] = Map[Long, Char]()
    var deletes: Map[Long, Char] = Map[Long, Char]()

    // loop over cigar elements and fill sets
    newCigar.getCigarElements.foreach(cigarElement => {
      cigarElement.getOperator match {
        case CigarOperator.M => {
          var rangeStart = 0L
          var inMatch = false

          // dirty dancing to recalculate match sets
          for (i <- 0 until cigarElement.getLength) {
            if (reference(referencePos) == sequence(readPos)) {
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
            deletes += ((referencePos + readStart) -> reference(referencePos))

            referencePos += 1
          }
        }
        case _ => {
          if (cigarElement.getOperator.consumesReadBases) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases) {
            throw new IllegalArgumentException ("Cannot handle operator: " + cigarElement.getOperator)
          }
        }
      }
    })

    new MdTag(matches, mismatches, deletes)
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
  def moveAlignment (read: RichADAMRecord, newCigar: Cigar): MdTag = {
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
  def moveAlignment (read: RichADAMRecord, newCigar: Cigar, newReference: String, newAlignmentStart: Long): MdTag = {
    moveAlignment(newReference, read.record.getSequence, newCigar, newAlignmentStart)
  }

  def apply (read: String, reference: String, cigar: Cigar, start: Long): MdTag = {
    var matchCount = 0
    var delCount = 0
    var string = ""
    var readPos = 0
    var refPos = 0

    // loop over all cigar elements
    cigar.getCigarElements.foreach(cigarElement => {
      cigarElement.getOperator match {
        case CigarOperator.M => {
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
            throw new IllegalArgumentException ("Cannot handle operator: " + cigarElement.getOperator)
          }
        }
      }
    })

    string += matchCount.toString
    
    apply(string, start)
  }
}

class MdTag(
    private val matches: immutable.List[NumericRange[Long]],
    private val mismatches: immutable.Map[Long, Char],
    private val deletes: immutable.Map[Long, Char]) {

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
    deletes.get(pos)
  }

  /**
   * Returns whether this read has any mismatches against the reference.
   *
   * @return True if this read has mismatches. We do not return true if the read has no mismatches but has deletions.
   */
  def hasMismatches(): Boolean = {
    !mismatches.isEmpty
  }

  /**
   * Returns the start position of the record described by this MD tag.
   *
   * @return The reference based start position of this tag.
   */
  def start(): Long = {
    val starts = matches.map(_.start) ::: mismatches.keys.toList ::: deletes.keys.toList
    starts.reduce(_ min _)
  }

  /**
   * Returns the end position of the record described by this MD tag.
   *
   * @return The reference based end position of this tag.
   */
  def end(): Long = {
    val ends = matches.map(_.end - 1) ::: mismatches.keys.toList ::: deletes.keys.toList
    ends.reduce(_ max _)
  }

  /**
   * Given a read, returns the reference.
   *
   * @param read A read for which one desires the reference sequence.
   * @return A string corresponding to the reference overlapping this read.
   */
  def getReference (read: RichADAMRecord): String = {
    getReference (read.getSequence, read.samtoolsCigar, read.getStart)
  }

  /**
   * Given a read sequence, cigar, and a reference start position, returns the reference.
   *
   * @param readSequence The base sequence of the read.
   * @param cigar The cigar for the read.
   * @param referenceFrom The starting point of this read alignment vs. the reference.
   * @return A string corresponding to the reference overlapping this read.
   */
  def getReference (readSequence: String, cigar: Cigar, referenceFrom: Long): String = {

    var referencePos = start()
    var readPos = 0
    var reference = ""

    // loop over all cigar elements
    cigar.getCigarElements.foreach(cigarElement => {
      cigarElement.getOperator match {
        case CigarOperator.M => {
          // if we are a match, loop over bases in element
          for (i <- (0 until cigarElement.getLength)) {
            // if a mismatch, get from the mismatch set, else pull from read
            if (mismatches.contains(referencePos)) {
              reference += {
                mismatches.get(referencePos) match {
                  case Some(base) => base
                  case _ => throw new IllegalStateException("Could not find mismatching base at cigar offset"+i)
                }
              }
            } else {
              reference += readSequence(readPos)
            }

            readPos += 1
            referencePos += 1
          }
        }
        case CigarOperator.D => {
          // if a delete, get from the delete pool
          for (i <- (0 until cigarElement.getLength)) {
            reference += {
              deletes.get(referencePos) match {
                case Some(base) => base
                case _ => throw new IllegalStateException("Could not find deleted base at cigar offset "+i)
              }
            }

            referencePos += 1
          }
        }
        case _ => {
          // ignore inserts
          if (cigarElement.getOperator.consumesReadBases) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases) {
            throw new IllegalArgumentException ("Cannot handle operator: " + cigarElement.getOperator)
          }
        }
      }
    })

    reference
  }

  /**
   * Converts an MdTag object to a properly formatted MD string.
   *
   * @return MD string corresponding to [0-9]+(([A-Z]|\^[A-Z]+)[0-9]+)
   * @see http://zenfractal.com/2013/06/19/playing-with-matches/
   */
  override def toString (): String = {
    var mdString = ""
    var lastWasMatch = false
    var lastWasDeletion = false
    var matchRun = 0
                    
    // loop over positions in tag - FSM for building string
    for (i <- start() to end()) {
      if(isMatch(i)) {
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
      } else if (deletes.contains(i)) {
        if (!lastWasDeletion) {
          // write match count before deletion
          if (lastWasMatch) {
            mdString += matchRun.toString
          } else {
            mdString += "0"
          }
          // add deletion caret
          mdString += "^"

          // set state
          lastWasMatch = false
          lastWasDeletion = true
        }

        // add deleted base
        mdString += deletes(i)
      } else {        
        // write match count before mismatch
        if (lastWasMatch) {
          mdString += matchRun.toString
        } else {
          mdString += "0"
        }

        mdString += mismatches(i)

        // clear state
        lastWasMatch = false
        lastWasDeletion = false
      }
    }

    // if we have more matches, write count
    if (lastWasMatch) {
      mdString += matchRun.toString
    } else {
      mdString += "0"
    }

    mdString
  }
  
}
