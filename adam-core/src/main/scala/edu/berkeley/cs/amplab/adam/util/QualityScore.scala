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

package edu.berkeley.cs.amplab.adam.util

class QualityScore(val value: Int) extends Ordered[QualityScore] with Serializable {
  require(value >= 0 && value < 256)

  def successProbability = PhredUtils.phredToSuccessProbability(value)

  def errorProbability = PhredUtils.phredToErrorProbability(value)

  def logSuccessProb = math.log(successProbability)

  def logErrorProb = math.log(errorProbability)

  def toChar: Char = {
    val result: Int = value + 33
    // valid range of result is described by regex "[!-~]"
    assert(result >= '!'.toInt && result <= '~'.toInt)
    result.toChar
  }

  override def compare(that: QualityScore) = this.value compare that.value

  override def toString = "Q%02d".format(value)

  override def equals(other: Any): Boolean = other match {
    case that: QualityScore => this.value == that.value
    case _ => false
  }

  override def hashCode: Int = Util.hashCombine(0x26C2E0BA, value.hashCode)
}

object QualityScore {
  def apply(value: Int) = new QualityScore(value)

  def toString(quals: Seq[QualityScore]): String =
    String.valueOf(quals.map(_.toChar))

  def fromErrorProbability(p: Double) =
    new QualityScore(PhredUtils.errorProbabilityToPhred(p))
}
