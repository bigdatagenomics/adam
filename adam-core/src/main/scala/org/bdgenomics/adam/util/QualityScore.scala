/*
 * Copyright (c) 2014 The Regents of the University of California
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

package org.bdgenomics.adam.util

class QualityScore(val phred: Int) extends Ordered[QualityScore] with Serializable {
  // Valid range of phred + 33 is described by the regex "[!-~]".
  require(phred + 33 >= '!'.toInt && phred + 33 <= '~'.toInt, "Phred %s out of range".format(phred))

  def successProbability = PhredUtils.phredToSuccessProbability(phred)

  def errorProbability = PhredUtils.phredToErrorProbability(phred)

  def toChar: Char = (phred + 33).toChar

  override def compare(that: QualityScore) = this.phred compare that.phred

  override def toString = "Q%02d".format(phred)

  override def equals(other: Any): Boolean = other match {
    case that: QualityScore => this.phred == that.phred
    case _                  => false
  }

  override def hashCode: Int = Util.hashCombine(0x26C2E0BA, phred.hashCode)
}

object QualityScore {
  val zero = new QualityScore(0)

  def apply(phred: Int) = new QualityScore(phred)

  def toString(quals: Seq[QualityScore]): String =
    String.valueOf(quals.map(_.toChar).toArray)

  def fromErrorProbability(p: Double) =
    new QualityScore(PhredUtils.errorProbabilityToPhred(p))
}
