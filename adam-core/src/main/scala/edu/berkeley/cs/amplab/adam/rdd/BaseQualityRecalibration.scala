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
import edu.berkeley.cs.amplab.adam.util.QualityScore
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

  // Additional covariates to use when computing the correction
  // TODO: parameterize
  val covariates = CovariateSpace(
    //new DinucCovariate
  )

  // Bases with quality less than this will be skipped and left alone
  // TODO: parameterize
  val minAcceptableQuality = QualityScore(6)

  // Compute and apply recalibration
  def apply(): RDD[ADAMRecord] = {
    def shouldIncludeRead(read: DecadentRead) =
      read.isCanonicalRecord && read.alignmentQuality.exists(_ > QualityScore(0))

    // first phase
    val observed: ObservationTable = reads.
      filter(shouldIncludeRead).map(observe).
      aggregate(ObservationAccumulator(covariates))(_ ++= _, _ += _).result

    println("ObservationTable:\n%s".format(observed.toCSV))

    // second phase
    val recalibrator = Recalibrator(observed, minAcceptableQuality)
    reads.map(recalibrator)
  }

  // Compute observation table for a single read
  private def observe(read: DecadentRead): Seq[(CovariateKey, Observation)] = {
    def shouldIncludeResidue(residue: Residue) =
      residue.quality >= minAcceptableQuality &&
        !residue.isInsertion &&
        !knownSnps.isMasked(residue)

    // Compute keys and filter out skipped residues
    val keys: Seq[(CovariateKey, Residue)] =
      covariates(read).zip(read.sequence).filter(x => shouldIncludeResidue(x._2))

    // Construct result
    keys.map{ case (key, residue) => (key, Observation(residue.isSNP)) }
  }
}

object BaseQualityRecalibration {
  def apply(rdd: RDD[ADAMRecord], knownSnps: SnpTable): RDD[ADAMRecord] = {
    new BaseQualityRecalibration(cloy(rdd), knownSnps).apply()
  }
}
