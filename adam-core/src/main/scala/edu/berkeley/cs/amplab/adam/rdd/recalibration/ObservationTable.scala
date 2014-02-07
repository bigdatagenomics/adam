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

import edu.berkeley.cs.amplab.adam.rich.DecadentRead
import edu.berkeley.cs.amplab.adam.rich.DecadentRead._
import edu.berkeley.cs.amplab.adam.util.QualityScore
import edu.berkeley.cs.amplab.adam.util.Util
import scala.collection.mutable

class CovariateKey(val parts: Seq[Covariate#Value]) extends Serializable {
  override def toString: String = "[" + parts.mkString(", ") + "]"

  override def equals(other: Any) = other match {
    case that: CovariateKey => this.parts == that.parts
    case _ => false
  }

  override def hashCode = Util.hashCombine(0xD20D1E51, parts.hashCode)
}

class CovariateSpace(val covariates: IndexedSeq[Covariate]) extends Serializable {
  require(covariates.length > 0)

  def apply(residue: Residue): CovariateKey =
    new CovariateKey(covariates.map(_.compute(residue)))

  override def equals(other: Any): Boolean = other match {
    case that: CovariateSpace => this.covariates == that.covariates
    case _ => false
  }

  override def hashCode = Util.hashCombine(0x48C35799, covariates.hashCode)
}

object CovariateSpace {
  def apply(covariates: Covariate*): CovariateSpace =
    new CovariateSpace(covariates.toIndexedSeq)
}

class Observation(val total: Long, val mismatches: Long) extends Serializable {
  require(mismatches >= 0 && mismatches <= total)

  def +(that: Observation) =
    new Observation(this.total + that.total, this.mismatches + that.mismatches)

  def empiricalQuality: QualityScore = bayesianQualityEstimate

  // Estimates the probability of a mismatch under a Bayesian model with Binomial likelihood
  // and Beta(α, β) prior. When α = β = 1, this is also known as "Laplace's rule of succession".
  // TODO: Beta(1, 1) is the safest choice, but maybe Beta(1/2, 1/2) is more accurate?
  def bayesianQualityEstimate: QualityScore = bayesianQualityEstimate(1, 1)
  def bayesianQualityEstimate(α: Double, β: Double): QualityScore =
    QualityScore.fromErrorProbability((α + mismatches) / (α + β + total))

  // GATK 1.6 uses the following, which they describe as "Yates's correction". However,
  // it doesn't match the Wikipedia entry which describes a different formula used
  // for correction of chi-squared independence tests on contingency tables.
  // TODO: Figure out this discrepancy.
  def gatkQualityEstimate: QualityScore = gatkQualityEstimate(1)
  def gatkQualityEstimate(smoothing: Int): QualityScore =
    Seq(QualityScore(50), gatkQualityEstimateUnclipped(smoothing)).min
  def gatkQualityEstimateUnclipped(smoothing: Int): QualityScore =
    QualityScore.fromErrorProbability((smoothing + mismatches) / (smoothing + total))

  override def toString: String =
    "%s / %s (%s)".format(mismatches, total, empiricalQuality)

  override def equals(other: Any): Boolean = other match {
    case that: Observation => this.total == that.total && this.mismatches == that.mismatches
    case _ => false
  }

  override def hashCode = Util.hashCombine(0x634DAED9, total.hashCode, mismatches.hashCode)
}

object Observation {
  val empty = new Observation(0, 0)

  def apply(isMismatch: Boolean) = new Observation(1, if(isMismatch) 1 else 0)
}

class ObservationTable(
    val covariates: CovariateSpace,
    val entries: Map[CovariateKey, Observation]
  ) extends Serializable {

  override def toString = entries.map{ case (k, v) => "%s\t%s".format(k, v) }.mkString("\n")
}

class ObservationAccumulator(val covariates: CovariateSpace) extends Serializable {
  private val entries = mutable.HashMap[CovariateKey, Observation]()

  def ++= (that: Seq[(CovariateKey, Observation)]): ObservationAccumulator = {
    that.foreach { case (k, v) => accum(k, v) }
    this
  }

  def += (that: ObservationAccumulator): ObservationAccumulator = {
    if(this.covariates != that.covariates)
      throw new IllegalArgumentException("Can only combine observations with matching CovariateSpaces")
    that.entries.foreach { case (k, v) =>  accum(k, v) }
    this
  }

  def accum(key: CovariateKey, value: Observation): ObservationAccumulator = {
    entries(key) = value + entries.getOrElse(key, Observation.empty)
    this
  }

  def result: ObservationTable = new ObservationTable(covariates, entries.toMap)
}

object ObservationAccumulator {
  def apply(covariates: CovariateSpace) = new ObservationAccumulator(covariates)
}
