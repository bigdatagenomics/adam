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
import edu.berkeley.cs.amplab.adam.util.Util
import scala.collection.mutable

// FIXME: This should really be an inner class of CovariateSpace
class CovariateKey(val parts: Seq[Covariate#Value]) extends Serializable {
  override def toString: String = "[" + parts.mkString(", ") + "]"

  override def equals(other: Any) = other match {
    case that: CovariateKey =>
      this.parts == that.parts

    case _ => false
  }

  override def hashCode = Util.hashCombine(0xD20D1E51, parts.hashCode)
}

class CovariateSpace(val covariates: IndexedSeq[Covariate]) extends Serializable {
  require(covariates.length > 0)

  def apply(residue: Residue): CovariateKey =
    new CovariateKey(covariates.map(_.compute(residue)))
}

object CovariateSpace {
  def apply(covariates: Covariate*): CovariateSpace =
    new CovariateSpace(covariates.toIndexedSeq)
}

class Observation(val total: Long, val mismatches: Long) extends Serializable {
  require(mismatches >= 0 && mismatches <= total)

  def +(that: Observation) =
    new Observation(this.total + that.total, this.mismatches + that.mismatches)

  // Estimates the probability of a mismatch under a model with Binomial likelihood
  // and Beta(1, 1) prior distribution. Also known as "Laplace's rule of succession".
  // TODO: Beta(1, 1) is a safe choice, but maybe Beta(1/2, 1/2) is better?
  def estimatedErrorProbability: Double =
    (1.0 + mismatches) / (2.0 + total)

  // GATK 1.6 uses the following, which they describe as "Yates's correction". However,
  // this doesn't match the Wikipedia entry which describes a different formula that's
  // for correction of chi-squared independence tests on contingency tables.
  // TODO: Figure out what's going on.
  def gatkEstimatedErrorProbability(smoothing: Int = 1): Double =
    (smoothing + mismatches) / (smoothing + total)

  override def toString: String = "%s / %s (%.2f%%)".format(mismatches, total, (100.0 * mismatches) / total)

  override def equals(other: Any) = other match {
    case that: Observation =>
      this.total == that.total && this.mismatches == that.mismatches

    case _ => false
  }

  override def hashCode = Util.hashCombine(0x634DAED9, total.hashCode, mismatches.hashCode)
}

object Observation {
  val empty = new Observation(0, 0)

  val match_ = new Observation(1, 0)

  val mismatch = new Observation(1, 1)

  def apply(isMismatch: Boolean) = new Observation(1, if(isMismatch) 1 else 0)
}

class ObservationTable(val covariates: CovariateSpace, initialEntries: Seq[(CovariateKey, Observation)]) extends Serializable {
  val entries = mutable.HashMap[CovariateKey, Observation](initialEntries: _*)

  def += (that: Seq[(CovariateKey, Observation)]): ObservationTable = {
    that.foreach{ case (key, value) => this.entries(key) = this.entries.getOrElse(key, Observation.empty) + value }
    this
  }

  def += (that: ObservationTable): ObservationTable = {
    this += that.entries.toSeq
  }
}

object ObservationTable {
  def empty(covariates: CovariateSpace): ObservationTable = {
    new ObservationTable(covariates, Seq.empty)
  }

  def apply(covariates: CovariateSpace, entries: Seq[(CovariateKey, Observation)]): ObservationTable = {
    new ObservationTable(covariates, entries)
  }
}
