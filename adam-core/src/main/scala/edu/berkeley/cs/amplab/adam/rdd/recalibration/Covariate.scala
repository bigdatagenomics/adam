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

trait Covariate {
  type Value

  def compute(read: DecadentRead): Seq[Value]

  def apply(read: DecadentRead): Seq[Value] = compute(read)
}

abstract class AbstractCovariate[ValueT] extends Covariate with Serializable {
  override type Value = ValueT
}

class CovariateKey(
    val readGroup: String,
    val quality: QualityScore,
    val extras: Seq[Covariate#Value]
) extends Serializable {

  def parts: Seq[Any] = Seq(readGroup, quality) ++ extras

  override def toString: String = "[" + parts.mkString(", ") + "]"

  override def equals(other: Any) = other match {
    case that: CovariateKey =>
      this.readGroup == that.readGroup && this.quality == that.quality && this.extras == that.extras
    case _ => false
  }

  override def hashCode = Util.hashCombine(0xD20D1E51, parts.hashCode)
}

class CovariateSpace(val extras: IndexedSeq[Covariate]) extends Serializable {
  // Computes the covariate values for all residues in this read
  def apply(read: DecadentRead): Seq[CovariateKey] = {
    // Ask each 'extra' covariate to compute its values for this read
    val extraVals = extras.map(cov => {
      val result = cov(read)
      // Each covariate must return a value per Residue
      assert(result.size == read.sequence.size)
      result
    })

    // Construct the CovariateKeys
    read.sequence.zipWithIndex.map{ case (residue, residueIdx) =>
      val residueExtras = extraVals.map(_(residueIdx))
      new CovariateKey(read.readGroup, residue.quality, residueExtras)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: CovariateSpace => this.extras == that.extras
    case _ => false
  }

  override def hashCode = Util.hashCombine(0x48C35799, extras.hashCode)
}

object CovariateSpace {
  def apply(extras: Covariate*): CovariateSpace =
    new CovariateSpace(extras.toIndexedSeq)
}
