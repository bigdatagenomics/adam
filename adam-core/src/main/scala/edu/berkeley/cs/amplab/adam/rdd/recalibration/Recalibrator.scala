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

package edu.berkeley.cs.amplab.adam.rdd.recalibration

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.DecadentRead
import edu.berkeley.cs.amplab.adam.rich.DecadentRead._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.util.QualityScore

class Recalibrator(val table: RecalibrationTable)
  extends (DecadentRead => ADAMRecord) with Serializable {

  def apply(read: DecadentRead): ADAMRecord = {
    // TODO: recalibrate
    val record: ADAMRecord = read.record
    ADAMRecord.newBuilder(record).
      setQual(computeQual(read)).
      setOrigQual(record.getQual()).
      build()
  }

  private def computeQual(read: DecadentRead): CharSequence =
    QualityScore.toString(read.sequence.map(table))
}

object Recalibrator {
  def apply(observed: ObservationTable): Recalibrator = {
    new Recalibrator(RecalibrationTable(observed))
  }
}

class RecalibrationTable(
    // covariates for this recalibration
    val space: CovariateSpace,
    // marginal by read group
    val globalTable: Map[String, Aggregate],
    // marginal by read group and quality
    val qualityTable: Map[(String, QualityScore), Aggregate],
    // marginals for each optional covariate by read group and quality
    val extraTables: IndexedSeq[Map[(String, QualityScore, Covariate#Value), Aggregate]])
  extends (Residue => QualityScore) with Serializable {

  // Compute updated QualityScore for this Residue
  def apply(residue: Residue): QualityScore = {
    val readGroup = residue.read.readGroup
    val residueLogP = residue.quality.logErrorProb
    val globalDelta = computeGlobalDelta(readGroup)
    val qualityDelta = qualityMarginal(readGroup, residue.quality).map(_ - residueLogP - globalDelta).getOrElse(0.0)
    val covariatesDelta = 0 // TODO: handle extra covariates
    val correctedLogP = residueLogP + globalDelta + qualityDelta + covariatesDelta
    QualityScore.fromErrorProbability(math.exp(correctedLogP))
  }

  def computeGlobalDelta(readGroup: String): Double = {
    val bucket = globalTable(readGroup)
    bucket.empiricalQuality.logErrorProb - bucket.reportedQuality.logErrorProb
  }

  def qualityMarginal(readGroup: String, quality: QualityScore): Option[Double] =
    qualityTable.get((readGroup, quality)).map(_.empiricalQuality.logErrorProb)
}

object RecalibrationTable {
  def apply(observed: ObservationTable): RecalibrationTable = {
    // The ".map(identity)" calls are needed to force the result to be serializable.
    // See https://issues.scala-lang.org/browse/SI-7005
    def aggregateExtra(idx: Int) = observed.aggregate((k, v) => (k.readGroup, k.quality, k.extras(idx))).map(identity)
    val globalTables = observed.aggregate((k, v) => k.readGroup).map(identity)
    val qualityTables = observed.aggregate((k, v) => (k.readGroup, k.quality)).map(identity)
    val extrasTables = Range(0, observed.space.extras.length).map(aggregateExtra)
    new RecalibrationTable(observed.space, globalTables, qualityTables, extrasTables)
  }
}
