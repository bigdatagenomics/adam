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

class Observation(val total: Long, val mismatches: Long) extends Serializable {
  require(mismatches >= 0 && mismatches <= total)

  /**
   * Whether to emulate GATK's calculations.
   */
  val emulateGATK: Boolean = true

  def this(that: Observation) = this(that.total, that.mismatches)

  def +(that: Observation) =
    new Observation(this.total + that.total, this.mismatches + that.mismatches)

  /**
   * Empirically estimated probability of a mismatch.
   */
  def empiricalErrorProbability: Double =
    if(!emulateGATK) bayesianErrorProbability else gatkErrorProbability

  /**
   * Empirically estimated probability of a mismatch, as a QualityScore.
   */
  def empiricalQuality: QualityScore =
    QualityScore.fromErrorProbability(empiricalErrorProbability)

  /**
   * Estimates the probability of a mismatch under a Bayesian model with
   * Binomial likelihood and Beta(a, b) prior. When a = b = 1, this is also
   * known as "Laplace's rule of succession".
   *
   * TODO: Beta(1, 1) is the safest choice, but maybe Beta(1/2, 1/2) is more
   * accurate?
   */
  def bayesianErrorProbability: Double = bayesianErrorProbability(1, 1)
  def bayesianErrorProbability(a: Double, b: Double): Double = (a + mismatches) / (a + b + total)

  // GATK 1.6 uses the following, which they describe as "Yates's correction". However,
  // it doesn't match the Wikipedia entry which describes a different formula used
  // for correction of chi-squared independence tests on contingency tables.
  // TODO: Figure out this discrepancy.
  def gatkErrorProbability: Double = gatkErrorProbability(1)
  def gatkErrorProbability(smoothing: Double): Double = {
    val errProb = (mismatches + smoothing) / (total + smoothing)
    Seq(QualityScore(50).errorProbability, errProb + 0.0001).max
  }

  // Format as string compatible with GATK's CSV output
  def toCSV: Seq[String] = Seq(total.toString, mismatches.toString, empiricalQualityForCSV.phred.toString)

  def empiricalQualityForCSV: QualityScore =
    if(!emulateGATK) empiricalQuality else QualityScore.fromErrorProbability(gatkErrorProbability(0))

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

class Aggregate private(
    total: Long,        // number of total observations
    mismatches: Long,   // number of mismatches observed
    val expectedMismatches: Double  // expected number of mismatches based on reported quality scores
  ) extends Observation(total, mismatches) {

  require(expectedMismatches <= total)

  def reportedErrorProbability: Double = expectedMismatches / total.toDouble

  def +(that: Aggregate): Aggregate =
    new Aggregate(
      this.total + that.total,
      this.mismatches + that.mismatches,
      this.expectedMismatches + that.expectedMismatches)
}

object Aggregate {
  val empty: Aggregate = new Aggregate(0, 0, 0)

  def apply(key: CovariateKey, value: Observation) =
    new Aggregate(value.total, value.mismatches, key.quality.errorProbability * value.mismatches)
}

class ObservationTable(
    val space: CovariateSpace,
    val entries: Map[CovariateKey, Observation]
  ) extends Serializable {

  override def toString = entries.map { case (k, v) => "%s\t%s".format(k, v) }.mkString("\n")

  // Format as CSV compatible with GATK's output
  def toCSV: String = {
    val rows = entries.map { case (key, obs) =>
      space.toCSV(key) ++ obs.toCSV ++ (if(key.containsNone) Seq("**") else Seq())
    }
    (Seq(csvHeader) ++ rows).map(_.mkString(",")).mkString("\n")
  }

  def csvHeader: Seq[String] = space.csvHeader ++ Seq("TotalCount", "MismatchCount", "EmpiricalQ", "IsSkipped")

  // `func' computes the aggregation key
  def aggregate[K](func: (CovariateKey, Observation) => K): Map[K, Aggregate] = {
    val grouped = entries.groupBy { case (key, value) => func(key, value) }
    val newEntries = grouped.mapValues(bucket =>
      bucket.map { case (oldKey, obs) => Aggregate(oldKey, obs) }.fold(Aggregate.empty)(_ + _))
    newEntries.toMap
  }
}

class ObservationAccumulator(val space: CovariateSpace) extends Serializable {
  private val entries = mutable.HashMap[CovariateKey, Observation]()

  def ++= (that: Seq[(CovariateKey, Observation)]): ObservationAccumulator = {
    that.foreach { case (k, v) => accum(k, v) }
    this
  }

  def += (that: ObservationAccumulator): ObservationAccumulator = {
    if(this.space != that.space)
      throw new IllegalArgumentException("Can only combine observations with matching CovariateSpaces")
    that.entries.foreach { case (k, v) =>  accum(k, v) }
    this
  }

  def accum(key: CovariateKey, value: Observation): ObservationAccumulator = {
    entries(key) = value + entries.getOrElse(key, Observation.empty)
    this
  }

  def result: ObservationTable = new ObservationTable(space, entries.toMap)
}

object ObservationAccumulator {
  def apply(space: CovariateSpace) = new ObservationAccumulator(space)
}
