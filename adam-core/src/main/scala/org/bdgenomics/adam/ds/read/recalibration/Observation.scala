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
package org.bdgenomics.adam.ds.read.recalibration

import org.bdgenomics.adam.util.PhredUtils

/**
 * An empirical frequency count of mismatches from the reference.
 *
 * @param total The total number of bases observed.
 * @param mismatches The number of mismatchign bases observed.
 */
private[adam] case class Observation(total: Long, mismatches: Long) {
  require(mismatches >= 0 && mismatches <= total)

  /**
   * @param that An observation to merge with.
   * @return Returns a new observation that contains the sum values across
   *   both input observations.
   */
  def +(that: Observation) = {
    new Observation(this.total + that.total, this.mismatches + that.mismatches)
  }

  /**
   * @return Returns the empirically estimated probability of a mismatch, as a
   *   Phred scaled int.
   */
  def empiricalQuality: Int = {
    PhredUtils.errorProbabilityToPhred(bayesianErrorProbability())
  }

  /**
   * Estimates the probability of a mismatch under a Bayesian model with
   * Binomial likelihood and Beta(a, b) prior. When a = b = 1, this is also
   * known as "Laplace's rule of succession".
   *
   * TODO: Beta(1, 1) is the safest choice, but maybe Beta(1/2, 1/2) is more
   * accurate?
   *
   * @param a Beta distribution alpha parameter.
   * @param b Beta distribution beta parameter.
   * @return Returns the bayesian error probability of a base in this class
   *   being an error.
   */
  def bayesianErrorProbability(a: Double = 1.0,
                               b: Double = 1.0): Double = {
    (a + mismatches) / (a + b + total)
  }

  /**
   * @return Format as string compatible with GATK's CSV output
   */
  def toCSV: Seq[String] = {
    Seq(total.toString, mismatches.toString, empiricalQuality.toString)
  }

  override def toString: String = {
    "%s / %s (%s)".format(mismatches, total, empiricalQuality)
  }
}

private[recalibration] object Observation {
  val empty = new Observation(0, 0)

  /**
   * @param isMismatch Whether this observed base was a mismatch against the
   *   reference or not.
   * @return Returns a new observation with one base observed, and either one
   *   or zero observed mismatches.
   */
  def apply(isMismatch: Boolean) = {
    new Observation(1, if (isMismatch) 1 else 0)
  }
}

