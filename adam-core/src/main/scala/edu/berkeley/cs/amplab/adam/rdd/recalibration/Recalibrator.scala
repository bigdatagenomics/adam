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

class Recalibrator(val table: RecalibrationTable, val minAcceptableQuality: QualityScore)
  extends (DecadentRead => ADAMRecord) with Serializable {

  def apply(read: DecadentRead): ADAMRecord = {
    val record: ADAMRecord = read.record
    ADAMRecord.newBuilder(record).
      setQual(QualityScore.toString(computeQual(read))).
      setOrigQual(record.getQual()).
      build()
  }

  def computeQual(read: DecadentRead): Seq[QualityScore] = {
    val origQuals = read.sequence.map(_.quality)
    val newQuals = table(read)
    origQuals.zip(newQuals).map{ case (origQ, newQ) =>
      // Keep original quality score if below recalibration threshold
      if(origQ >= minAcceptableQuality) newQ else origQ
    }
  }
}

object Recalibrator {
  def apply(observed: ObservationTable, minAcceptableQuality: QualityScore): Recalibrator = {
    new Recalibrator(RecalibrationTable(observed), minAcceptableQuality)
  }
}

class RecalibrationTable(
    // covariates for this recalibration
    val covariates: CovariateSpace,
    // marginal by read group
    val globalTable: Map[String, Aggregate],
    // marginal by read group and quality
    val qualityTable: Map[(String, QualityScore), Aggregate],
    // marginals for each optional covariate by read group and quality
    val extraTables: IndexedSeq[Map[(String, QualityScore, Covariate#Value), Aggregate]])
  extends (DecadentRead => Seq[QualityScore]) with Serializable {

  def apply(read: DecadentRead): Seq[QualityScore] =
    covariates(read).map(lookup)

  def lookup(key: CovariateKey): QualityScore = {
    val residuePhred = key.quality.phred
    val globalDelta = computeGlobalDelta(key)
    val qualityDelta = computeQualityDelta(key, residuePhred + globalDelta)
    val extrasDelta = computeExtrasDelta(key, residuePhred + globalDelta + qualityDelta)
    val correctedPhred = residuePhred + globalDelta + qualityDelta + extrasDelta
    QualityScore(correctedPhred)
  }

  def computeGlobalDelta(key: CovariateKey): Int = {
    globalTable.get(key.readGroup).
      map(bucket => bucket.empiricalQuality.phred - bucket.reportedQuality.phred).
      getOrElse(0)
  }

  def computeQualityDelta(key: CovariateKey, offset: Int): Int = {
    qualityTable.get((key.readGroup, key.quality)).
      map(aggregate => aggregate.empiricalQuality.phred - offset).
      getOrElse(0)
  }

  def computeExtrasDelta(key: CovariateKey, offset: Int): Int = {
    // Returns sum(delta for each extra covariate)
    assert(extraTables.size == key.extras.size)
    extraTables.zip(key.extras).map{ case (table, value) =>
      table.get((key.readGroup, key.quality, value)).
        map(aggregate => aggregate.empiricalQuality.phred - offset).
        getOrElse(0)
    }.fold(0)(_ + _)
  }
}

object RecalibrationTable {
  def apply(observed: ObservationTable): RecalibrationTable = {
    // The ".map(identity)" calls are needed to force the result to be serializable.
    // See https://issues.scala-lang.org/browse/SI-7005
    def aggregateExtra(idx: Int) = observed.
      aggregate((k, v) => (k.readGroup, k.quality, k.extras(idx))).
      map(identity)
    val globalTables = observed.aggregate((k, v) => k.readGroup).map(identity)
    val qualityTables = observed.aggregate((k, v) => (k.readGroup, k.quality)).map(identity)
    val extrasTables = Range(0, observed.space.extras.length).map(aggregateExtra)
    new RecalibrationTable(observed.space, globalTables, qualityTables, extrasTables)
  }
}
