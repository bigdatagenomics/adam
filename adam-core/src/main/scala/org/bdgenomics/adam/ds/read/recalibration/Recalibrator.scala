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

import java.lang.StringBuilder
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro.Alignment

/**
 * The engine that recalibrates a read given a table of recalibrated qualities.
 *
 * @param table A table mapping error covariates back to finalized qualities.
 * @param minAcceptableAsciiPhred The minimum quality score to attempt to
 *   recalibrate, encoded as an ASCII character on the Illumina (33) scale.
 */
private[recalibration] case class Recalibrator(
    table: RecalibrationTable,
    minAcceptableAsciiPhred: Char) {

  /**
   * Rewrites the qualities for a read, given the recalibration table.
   *
   * @param record The read to recalibrate.
   * @param covariates An array containing the covariate that each base in this
   *   read mapped to.
   * @return Returns a recalibrated read if the read has defined qualities and
   * if the covariates for this read were observed.
   */
  def apply(record: Alignment,
            covariates: Array[CovariateKey]): Alignment = {
    if (record.getQualityScores != null &&
      covariates.nonEmpty) {
      Alignment.newBuilder(record)
        .setQualityScores(computeQual(record, covariates))
        .setOriginalQualityScores(record.getQualityScores)
        .build()
    } else {
      record
    }
  }

  /**
   * @param record The read to recalibrate.
   * @param covariates An array containing the covariate that each base in this
   *   read mapped to.
   * @return Returns the new quality scores for this read by querying the error
   *   covariate table.
   */
  private def computeQual(read: Alignment,
                          covariates: Array[CovariateKey]): String = {
    val origQuals = read.getQualityScores
    val quals = new StringBuilder(origQuals)
    val newQuals = table(covariates)
    var idx = 0
    while (idx < origQuals.length) {
      // Keep original quality score if below recalibration threshold
      if (origQuals(idx) >= minAcceptableAsciiPhred) {
        quals.setCharAt(idx, newQuals(idx))
      }
      idx += 1
    }

    quals.toString
  }
}

private[recalibration] object Recalibrator {

  /**
   * @param observed The observation table to invert to build the recalibrator.
   * @param minAcceptableAsciiPhred The minimum quality score to attempt to
   *   recalibrate, encoded as an ASCII character on the Illumina (33) scale.
   * @return Returns a recalibrator gained by inverting the observation table.
   */
  def apply(observed: ObservationTable,
            minAcceptableAsciiPhred: Char): Recalibrator = {
    Recalibrator(RecalibrationTable(observed), minAcceptableAsciiPhred)
  }
}
