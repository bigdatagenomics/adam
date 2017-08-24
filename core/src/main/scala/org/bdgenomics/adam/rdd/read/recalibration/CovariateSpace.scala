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
package org.bdgenomics.adam.rdd.read.recalibration

import org.bdgenomics.adam.models.RecordGroupDictionary
import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * Represents the space of all possible CovariateKeys for the given set
 * of Covariates.
 */
private[adam] object CovariateSpace extends Serializable {

  private val cycle = new CycleCovariate
  private val dinuc = new DinucCovariate

  /**
   * Generates an array of observed covariates for a given read.
   *
   * @param read The read to generate covariates for.
   * @param recordGroups A record group dictionary containing the record group
   *   that generated this read.
   * @return Returns an array of error covariates.
   *
   * @note This method is provided solely as a convenience method for testing.
   */
  private[recalibration] def apply(
    read: AlignmentRecord,
    recordGroups: RecordGroupDictionary): Array[CovariateKey] = {
    apply(read,
      Array.fill(read.getSequence.length) { true },
      Array.fill(read.getSequence.length) { false },
      recordGroups)
  }

  /**
   * Generates an array of observed covariates for a given read.
   *
   * @param read The read to generate covariates for.
   * @param toInclude An array indicating whether each base in the read should
   *   be included in the generated observation table.
   * @param isMismatch An array indicating whether the base was a mismatch
   *   against the reference genome.
   * @param recordGroups A record group dictionary containing the record group
   *   that generated this read.
   * @return Returns an array of error covariates, one per base in the read.
   */
  def apply(read: AlignmentRecord,
            toInclude: Array[Boolean],
            isMismatch: Array[Boolean],
            recordGroups: RecordGroupDictionary): Array[CovariateKey] = {
    val cycles = cycle.compute(read)
    val dinucs = dinuc.compute(read)
    assert(cycles.length == dinucs.length)
    assert(cycles.length == toInclude.length)
    assert(cycles.length == isMismatch.length)

    // Construct the CovariateKeys
    val readLength = cycles.length
    val covariateArray = new Array[CovariateKey](readLength)
    val qualities = read.getQual
    var idx = 0
    while (idx < readLength) {
      val residueCycle = cycles(idx)
      val (residueDinucPrev, residueDinucCurr) = dinucs(idx)
      covariateArray(idx) = new CovariateKey(
        recordGroups.getIndex(read.getRecordGroupName),
        qualities(idx),
        residueCycle,
        residueDinucPrev,
        residueDinucCurr,
        shouldInclude = toInclude(idx),
        isMismatch = isMismatch(idx))
      idx += 1
    }
    covariateArray
  }

  /**
   * Formats a given covariate to match the GATK's CSV output.
   *
   * @param key The error covariate to render.
   * @param recordGroups A dictionary mapping recordGroupIds to record groups.
   * @return Returns a Seq containing CSV cells for a single row of the CSV file.
   */
  def toCSV(key: CovariateKey,
            recordGroups: RecordGroupDictionary): Seq[String] = {
    Seq(recordGroups.recordGroups(key.readGroupId).recordGroupName,
      (key.quality.toInt - 33).toString,
      cycle.toCSV(key.cycle),
      dinuc.toCSV((key.dinucPrev, key.dinucCurr)))
  }

  /**
   * @return The CSV header line as a Seq with the header field per column.
   */
  def csvHeader: Seq[String] = {
    Seq("ReadGroup",
      "ReportedQ",
      cycle.csvFieldName,
      dinuc.csvFieldName)
  }
}
