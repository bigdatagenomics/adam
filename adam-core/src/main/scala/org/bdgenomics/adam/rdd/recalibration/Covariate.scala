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

package org.bdgenomics.adam.rdd.recalibration

import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.rich.DecadentRead._
import org.bdgenomics.adam.util.QualityScore
import org.bdgenomics.adam.util.Util

/**
 * A Covariate represents a predictor, also known as a "feature" or
 * "independent variable".
 *
 * @note Concrete implementations of Covariate should inherit from
 * AbstractCovariate, not Covariate.
 */
trait Covariate {
  type Value

  /**
   * Given a read, computes the value of this covariate for each residue in the
   * read.
   *
   * The returned values must be in the same order as the residues. A value
   * of None means this covariate does not apply to the corresponding residue.
   *
   * Example: The DinucCovariate returns a pair of bases for each residue,
   * except for bases at the start of a read, for which it returns None.
   */
  def compute(read: DecadentRead): Seq[Option[Value]]

  def apply(read: DecadentRead): Seq[Option[Value]] = compute(read)

  // Format the provided Value to be compatible with GATK's CSV output
  def toCSV(option: Option[Value]): String = option match {
    case None        => "(none)"
    case Some(value) => value.toString
  }

  // A short name for this covariate, used in CSV output header
  def csvFieldName: String
}

abstract class AbstractCovariate[ValueT] extends Covariate with Serializable {
  override type Value = ValueT
}

/**
 * Represents a tuple containing a value for each covariate.
 *
 * The values for mandatory covariates are stored in member fields and optional
 * covariate valuess are in `extras`.
 */
class CovariateKey(
    val readGroup: String,
    val quality: QualityScore,
    val extras: Seq[Option[Covariate#Value]]) extends Serializable {

  def parts: Seq[Any] = Seq(readGroup, quality) ++ extras

  def containsNone: Boolean = extras.exists(_.isEmpty)

  override def toString: String = "[" + parts.mkString(", ") + "]"

  override def equals(other: Any) = other match {
    case that: CovariateKey =>
      this.readGroup == that.readGroup && this.quality == that.quality && this.extras == that.extras
    case _ => false
  }

  override def hashCode = Util.hashCombine(0xD20D1E51, parts.hashCode)
}

/**
 * Represents the abstract space of all possible CovariateKeys for the given set
 * of Covariates.
 */
class CovariateSpace(val extras: IndexedSeq[Covariate]) extends Serializable {
  // Computes the covariate values for all residues in this read
  def apply(read: DecadentRead): Seq[CovariateKey] = {
    // Ask each 'extra' covariate to compute its values for this read
    val extraVals = extras.map(cov => {
      val result = cov(read)
      // Each covariate must return a value per Residue
      assert(result.size == read.residues.size)
      result
    })

    // Construct the CovariateKeys
    read.residues.zipWithIndex.map {
      case (residue, residueIdx) =>
        val residueExtras = extraVals.map(_(residueIdx))
        new CovariateKey(read.readGroup, residue.quality, residueExtras)
    }
  }

  // Format the provided key to be compatible with GATK's CSV output
  def toCSV(key: CovariateKey): Seq[String] = {
    val extraFields: Seq[String] = extras.zip(key.extras).map {
      case (cov, value) => cov.toCSV(value.asInstanceOf[Option[cov.Value]])
    }
    Seq(key.readGroup, key.quality.phred.toString) ++ extraFields
  }

  def csvHeader: Seq[String] = Seq("ReadGroup", "ReportedQ") ++ extras.map(_.csvFieldName)

  override def equals(other: Any): Boolean = other match {
    case that: CovariateSpace => this.extras == that.extras
    case _                    => false
  }

  override def hashCode = Util.hashCombine(0x48C35799, extras.hashCode)
}

object CovariateSpace {
  def apply(extras: Covariate*): CovariateSpace =
    new CovariateSpace(extras.toIndexedSeq)
}
