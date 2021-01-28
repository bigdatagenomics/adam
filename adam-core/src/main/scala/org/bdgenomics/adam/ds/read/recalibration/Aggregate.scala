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
 * A class representing the aggregated mismatch count in a covariate.
 *
 * @param total The total number of bases in this covariate.
 * @param mismatches The total number of mismatching bases in this covariate.
 * @param expectedMismatches The expected number of bases that we would see
 *   mismatching, given the quality scores assigned to the bases in this
 *   covariate.
 */
private[recalibration] class Aggregate(
    total: Long, // number of total observations
    mismatches: Long, // number of mismatches observed
    val expectedMismatches: Double // expected number of mismatches based on reported quality scores
    ) extends Observation(total, mismatches) {

  require(expectedMismatches <= total)

  /**
   * @return Returns the probability of a base in this covariate being an error,
   *   given the number of observed bases and the number of observed errors.
   */
  def reportedErrorProbability: Double = expectedMismatches / total.toDouble

  /**
   * @param that Another aggregate to merge with.
   * @return Returns the sum total of bases, errors, and expected errors between
   *   these two aggregates.
   */
  def +(that: Aggregate): Aggregate =
    new Aggregate(
      this.total + that.total,
      this.mismatches + that.mismatches,
      this.expectedMismatches + that.expectedMismatches
    )
}

private[recalibration] object Aggregate {
  val empty: Aggregate = new Aggregate(0, 0, 0)

  /**
   * @param key The error covariate that generated bases. Used to get the
   *   predicted error rate from the quality score.
   * @param value The observation with the number of observed bases and errors.
   * @return Returns an aggregate that uses the predicted error rate from the
   *   error covariate and the number of bases from the observation to say
   *   how many bases are expected to be in error.
   */
  def apply(key: CovariateKey, value: Observation) = {
    new Aggregate(value.total,
      value.mismatches,
      PhredUtils.phredToErrorProbability(key.qualityScore.toInt - 33) * value.total)
  }
}

