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

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.SnpTable
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.recalibration._
import edu.berkeley.cs.amplab.adam.rich.DecadentRead
import edu.berkeley.cs.amplab.adam.rich.DecadentRead._
import edu.berkeley.cs.amplab.adam.util.PhredQualityScore
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * Base Quality Score Recalibration
 * ================================
 *
 * The algorithm proceeds in two phases:
 *
 *  1. Make a pass over the reads to collect statistics and build the
 *     recalibration tables.
 *
 *  2. Perform a second pass over the reads to apply the recalibration and
 *     assign adjusted quality scores.
 */
class BaseQualityRecalibration(
    val reads: RDD[DecadentRead],
    val knownSnps: SnpTable)
extends Serializable with Logging {

  // Covariates to use when computing the correction
  val covariates = CovariateSpace(new ReadGroupCovariate, new QualityScoreCovariate)

  // Bases with quality less than this will be ignored in Phase 1
  val minAcceptableQuality = PhredQualityScore(6)

  // Compute and apply recalibration
  def apply(): RDD[ADAMRecord] = {
    // first phase
    val observed: ObservationTable = reads.
      filter(_.isCanonicalRecord).map(observe).
      fold(ObservationTable.empty(covariates))(_ += _)

    // second phase
    val recalibrator = Recalibrator(observed)
    reads.map(recalibrator)
  }

  // Compute observation table for a single read
  private def observe(read: DecadentRead): ObservationTable = {
    // FIXME: should insertions be skipped or not?
    def shouldIncludeResidue(residue: Residue) =
      residue.quality >= minAcceptableQuality &&
        !residue.isInsertion &&
        !knownSnps.isMasked(residue)

    val residues = read.sequence.filter(shouldIncludeResidue)
    ObservationTable(covariates, residues)
  }
}

object BaseQualityRecalibration {
  def apply(rdd: RDD[ADAMRecord], knownSnps: SnpTable): RDD[ADAMRecord] = {
    new BaseQualityRecalibration(cloy(rdd), knownSnps).apply()
  }
}
