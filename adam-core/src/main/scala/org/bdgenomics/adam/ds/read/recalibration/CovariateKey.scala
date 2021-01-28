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

/**
 * Represents a tuple containing a value for each covariate.
 *
 * @param readGroupId The ID of the read group that bases in this error
 *   covariate came from.
 * @param qualityScore The quality score of bases in this covariate.
 * @param cycle The sequencer cycle that bases in this covariate came from.
 * @param dinucPrev The nucleotide preceding this base.
 * @param dinucCurr The nucleotide that was observed.
 * @param shouldInclude Whether this base should be included in the
 *   recalibration table.
 * @param isMismatch Whether this base was a mismatch against the reference.
 */
private[adam] case class CovariateKey(
    readGroupId: Int,
    qualityScore: Char,
    cycle: Int,
    dinucPrev: Char,
    dinucCurr: Char,
    shouldInclude: Boolean = true,
    isMismatch: Boolean = false) {

  /**
   * @return Returns a new CovariateKey with the shouldInclude and isMismatch
   *   fields reset to their defaults.
   */
  def toDefault: CovariateKey = {
    copy(shouldInclude = true, isMismatch = false)
  }

  /**
   * @return Returns true if either the observed or preceeding bases were 'N'.
   *
   * @note This is required to render CSV formatting that matches the GATK.
   */
  def containsNone: Boolean = {
    dinucPrev == 'N' && dinucCurr == 'N'
  }

  /**
   * @return Packages the dinucleotide covariate as a tuple.
   */
  def dinuc: (Char, Char) = {
    (dinucPrev, dinucCurr)
  }
}
