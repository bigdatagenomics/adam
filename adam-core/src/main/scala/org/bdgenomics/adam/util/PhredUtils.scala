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
package org.bdgenomics.adam.util

import scala.math.{
  exp,
  expm1,
  pow,
  log,
  log1p,
  log10,
  round
}

/**
 * Helper singleton for converting Phred scores to/from probabilities.
 *
 * As a reminder, given an error probability \epsilon, the Phred score q is:
 *
 * q = -10 log_{10} \epsilon
 */
object PhredUtils extends Serializable {

  // doubles in log space underflow at the probability equivalent to Phred 3233
  val minValue = 3233

  private lazy val phredToErrorProbabilityCache: Array[Double] = {
    (0 until 256).map { p => pow(10.0, -p / 10.0) }.toArray
  }

  private lazy val phredToSuccessProbabilityCache: Array[Double] = {
    phredToErrorProbabilityCache.map { p => 1.0 - p }
  }

  private val MLOG10_DIV10 = -log(10.0) / 10.0
  private val M10_DIV_LOG10 = -10.0 / log(10.0)

  /**
   * @param phred The phred score to convert.
   * @return The input phred score as a log success probability.
   */
  def phredToLogProbability(phred: Int): Double = {
    if (phred < 156) {
      log(phredToSuccessProbability(phred)).toFloat
    } else {
      log1p(-exp(phred * MLOG10_DIV10))
    }
  }

  /**
   * @param phred The phred score to convert.
   * @return The input phred score as a success probability. If the phred score
   *   is above 255, we clip to 255.
   */
  def phredToSuccessProbability(phred: Int): Double = if (phred <= 0) {
    phredToSuccessProbabilityCache(0)
  } else if (phred < 255) {
    phredToSuccessProbabilityCache(phred)
  } else {
    phredToSuccessProbabilityCache(255)
  }

  /**
   * @param phred The phred score to convert.
   * @return The input phred score as an error probability. If the phred score
   *   is above 255, we clip to 255.
   */
  def phredToErrorProbability(phred: Int): Double = if (phred <= 0) {
    phredToErrorProbabilityCache(0)
  } else if (phred < 255) {
    phredToErrorProbabilityCache(phred)
  } else {
    phredToErrorProbabilityCache(255)
  }

  private def probabilityToPhred(p: Double): Int = math.round(-10.0 * log10(p)).toInt

  /**
   * @param p A success probability to convert to phred.
   * @return One minus the input probability as a phred score, rounded to the
   *   nearest int.
   */
  def successProbabilityToPhred(p: Double): Int = probabilityToPhred(1.0 - p)

  /**
   * @param p An error probability to convert to phred.
   * @return The input probability as a phred score, rounded to the nearest int.
   */
  def errorProbabilityToPhred(p: Double): Int = probabilityToPhred(p)

  /**
   * @param p A log-scaled success probability to conver to phred.
   * @return Returns this probability as a Phred score. If the log value is 0.0,
   *   we clip the phred score to Int.MaxValue.
   */
  def logProbabilityToPhred(p: Double): Int = if (p == 0.0 || p == -0.0) {
    minValue
  } else {
    round(M10_DIV_LOG10 * log(-expm1(p))).toInt
  }
}
