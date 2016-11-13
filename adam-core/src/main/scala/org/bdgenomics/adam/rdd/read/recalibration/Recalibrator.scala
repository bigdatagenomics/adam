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

import org.bdgenomics.adam.models.QualityScore
import org.bdgenomics.adam.rich.DecadentRead.Residue
import org.bdgenomics.adam.rich.RichAlignmentRecord._
import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.instrumentation.Timers._
import scala.math.{ exp, log }

private[recalibration] class Recalibrator(val table: RecalibrationTable, val minAcceptableQuality: QualityScore)
    extends Serializable {

  def apply(r: (Option[DecadentRead], Option[AlignmentRecord])): AlignmentRecord = RecalibrateRead.time {
    r._1.fold(r._2.get)(read => {
      val record: AlignmentRecord = read.record
      if (record.getQual != null) {
        AlignmentRecord.newBuilder(record)
          .setQual(QualityScore.toString(computeQual(read)))
          .setOrigQual(record.getQual)
          .build()
      } else {
        record
      }
    })
  }

  def computeQual(read: DecadentRead): Seq[QualityScore] = ComputeQualityScore.time {
    val origQuals = read.residues.map(_.quality)
    val newQuals = table(read)
    origQuals.zip(newQuals).map {
      case (origQ, newQ) =>
        // Keep original quality score if below recalibration threshold
        if (origQ >= minAcceptableQuality) newQ else origQ
    }
  }
}

private[recalibration] object Recalibrator {
  def apply(observed: ObservationTable, minAcceptableQuality: QualityScore): Recalibrator = {
    new Recalibrator(RecalibrationTable(observed), minAcceptableQuality)
  }
}

private[recalibration] class RecalibrationTable(
  // covariates for this recalibration
  val covariates: CovariateSpace,
  // marginal and quality scores by read group,
  val globalTable: Map[String, (Aggregate, QualityTable)])
    extends (DecadentRead => Seq[QualityScore]) with Serializable {

  // TODO: parameterize?
  val maxQualScore = QualityScore(50)

  val maxLogP = log(maxQualScore.errorProbability)

  def apply(read: DecadentRead): Seq[QualityScore] = {
    val globalEntry: Option[(Aggregate, QualityTable)] = globalTable.get(read.readGroup)
    val globalDelta = computeGlobalDelta(globalEntry)
    val extraValues: IndexedSeq[Seq[Option[Covariate#Value]]] = getExtraValues(read)
    read.residues.zipWithIndex.map(lookup(_, globalEntry, globalDelta, extraValues))
  }

  def lookup(residueWithIndex: (Residue, Int), globalEntry: Option[(Aggregate, QualityTable)], globalDelta: Double,
             extraValues: IndexedSeq[Seq[Option[Covariate#Value]]]): QualityScore = {
    val (residue, index) = residueWithIndex
    val residueLogP = log(residue.quality.errorProbability)
    val qualityEntry: Option[(Aggregate, ExtrasTables)] = getQualityEntry(residue.quality, globalEntry)
    val qualityDelta = computeQualityDelta(qualityEntry, residueLogP + globalDelta)
    val extrasDelta = computeExtrasDelta(qualityEntry, index, extraValues, residueLogP + globalDelta + qualityDelta)
    val correctedLogP = residueLogP + globalDelta + qualityDelta + extrasDelta
    qualityFromLogP(correctedLogP)
  }

  def qualityFromLogP(logP: Double): QualityScore = {
    val boundedLogP = math.min(0.0, math.max(maxLogP, logP))
    QualityScore.fromErrorProbability(exp(boundedLogP))
  }

  def computeGlobalDelta(globalEntry: Option[(Aggregate, QualityTable)]): Double = {
    globalEntry.map(bucket => log(bucket._1.empiricalErrorProbability) - log(bucket._1.reportedErrorProbability)).
      getOrElse(0.0)
  }

  def getQualityEntry(
    quality: QualityScore,
    globalEntry: Option[(Aggregate, QualityTable)]): Option[(Aggregate, ExtrasTables)] = {
    globalEntry.flatMap(_._2.table.get(quality))
  }

  def computeQualityDelta(qualityEntry: Option[(Aggregate, ExtrasTables)], offset: Double): Double = {
    qualityEntry.map(bucket => log(bucket._1.empiricalErrorProbability) - offset).
      getOrElse(0.0)
  }

  def computeExtrasDelta(maybeQualityEntry: Option[(Aggregate, ExtrasTables)], residueIndex: Int,
                         extraValues: IndexedSeq[Seq[Option[Covariate#Value]]], offset: Double): Double = {
    // Returns sum(delta for each extra covariate)
    maybeQualityEntry.map(qualityEntry => {
      val extrasTables = qualityEntry._2.extrasTables
      assert(extrasTables.size == extraValues.size)
      var extrasDelta = 0.0
      var index = 0
      extraValues.foreach(residueValues => {
        val table = extrasTables(index)
        extrasDelta += table.get(residueValues(residueIndex)).
          map(aggregate => log(aggregate.empiricalErrorProbability) - offset).
          getOrElse(0.0)
        index += 1
      })
      extrasDelta
    }).getOrElse(0.0)
  }

  def getExtraValues(read: DecadentRead): IndexedSeq[Seq[Option[Covariate#Value]]] = GetExtraValues.time {
    covariates.extras.map(extra => extra(read))
  }

}

object RecalibrationTable {

  def apply(observed: ObservationTable): RecalibrationTable = {
    // The ".map(identity)" calls are needed to force the result to be serializable.
    val globalTable: Map[String, (Aggregate, QualityTable)] = observed.entries.groupBy(_._1.readGroup).map(globalEntry => {
      (globalEntry._1, (aggregateObservations(globalEntry._2), new QualityTable(computeQualityTable(globalEntry, observed.space))))
    }).map(identity)
    new RecalibrationTable(observed.space, globalTable)
  }

  def computeQualityTable(
    globalEntry: (String, Map[CovariateKey, Observation]),
    space: CovariateSpace): Map[QualityScore, (Aggregate, ExtrasTables)] = {
    globalEntry._2.groupBy(_._1.quality).map(qualityEntry => {
      (qualityEntry._1, (aggregateObservations(qualityEntry._2), new ExtrasTables(computeExtrasTables(qualityEntry._2, space))))
    }).map(identity)
  }

  def computeExtrasTables(
    table: Map[CovariateKey, Observation],
    space: CovariateSpace): IndexedSeq[Map[Option[Covariate#Value], Aggregate]] = {
    Range(0, space.extras.length).map(index => {
      table.groupBy(_._1.extras(index)).map(extraEntry => {
        (extraEntry._1, aggregateObservations(extraEntry._2))
      }).map(identity)
    })
  }

  def aggregateObservations[K](observations: Map[CovariateKey, Observation]): Aggregate = {
    observations.map { case (oldKey, obs) => Aggregate(oldKey, obs) }.fold(Aggregate.empty)(_ + _)
  }

}

private[recalibration] class QualityTable(
  val table: Map[QualityScore, (Aggregate, ExtrasTables)]) extends Serializable

private[recalibration] class ExtrasTables(
  val extrasTables: IndexedSeq[Map[Option[Covariate#Value], Aggregate]]) extends Serializable
