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

import scala.collection.immutable.NumericRange
import scala.util.matching.Regex

object MdTagEvent extends Enumeration {
  val Match, Mismatch, Delete = Value
}

object MdTag {

  val digitPattern = new Regex("\\d+")
  // for description, see base enum in adam schema
  val basesPattern = new Regex("[AaGgCcTtNnUuKkMmRrSsWwBbVvHhDdXx]+")

  def apply(mdTag: String, referenceStart: Long): MdTag = {
    new MdTag(mdTag, referenceStart)
  }

  def apply(mdTag: String): MdTag = {
    apply(mdTag, 0L)
  }
}

class MdTag(mdTagInput: String, referenceStart: Long) {

  private var matches = List[NumericRange[Long]]()
  private var mismatches = Map[Long, Char]()
  private var deletes = Map[Long, Char]()

  if (mdTagInput != null && mdTagInput.length > 0) {
    val mdTag = mdTagInput.toUpperCase
    val end = mdTag.length

    var offset = 0
    var referencePos = referenceStart

    def readMatches(errMsg: String): Unit = {
      MdTag.digitPattern.findPrefixOf(mdTag.substring(offset)) match {
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
      MdTag.basesPattern.findPrefixOf(mdTag.substring(offset)) match {
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

  def isMatch(pos: Long): Boolean = {
    matches.exists(_.contains(pos))
  }

  def mismatchedBase(pos: Long): Option[Char] = {
    mismatches.get(pos)
  }

  def deletedBase(pos: Long): Option[Char] = {
    deletes.get(pos)
  }

}
