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

import org.bdgenomics.adam.util.PhredUtils

/**
 * Model describing a single Quality score.
 *
 * @param phred The phred score.
 */
private[adam] case class QualityScore(phred: Int) extends Ordered[QualityScore] with Serializable {

  // Valid range of phred + 33 is described by the regex "[!-~]".
  require(phred + 33 >= '!'.toInt && phred + 33 <= '~'.toInt, "Phred %s out of range".format(phred))

  /**
   * @return This quality score as a success probability.
   */
  def successProbability: Double = PhredUtils.phredToSuccessProbability(phred)

  /**
   * @return This quality score as an error probability.
   */
  def errorProbability: Double = PhredUtils.phredToErrorProbability(phred)

  /**
   * @return Returns this quality score as a visible ASCII character. Assumes
   *   the Illumina encoding (Phred 0 == 33 == '!').
   */
  def toChar: Char = (phred + 33).toChar

  override def compare(that: QualityScore) = this.phred compare that.phred

  override def toString = "Q%02d".format(phred)

  override def equals(other: Any): Boolean = other match {
    case that: QualityScore => this.phred == that.phred
    case _                  => false
  }

  override def hashCode: Int = phred
}

/**
 * Companion object for building quality score objects.
 */
private[adam] object QualityScore {

  /**
   * The lowest quality score.
   */
  val zero = QualityScore(0)

  /**
   * @param quals A seq of quality scores to encode.
   * @return Returns this seq of quality scores as a visible ASCII string.
   *   Assumes the Illumina encoding (Phred 0 == 33 == '!').
   */
  def toString(quals: Seq[QualityScore]): String = {
    String.valueOf(quals.map(_.toChar).toArray)
  }

  /**
   * Builds a quality score given an error probability.
   *
   * @param p The error probability to translate into a phred scaled quality
   *   score.
   * @return Returns a QualityScore equivalent to this error probability, but
   *   rounded to the nearest int.
   */
  def fromErrorProbability(p: Double) = {
    QualityScore(PhredUtils.errorProbabilityToPhred(p))
  }
}
