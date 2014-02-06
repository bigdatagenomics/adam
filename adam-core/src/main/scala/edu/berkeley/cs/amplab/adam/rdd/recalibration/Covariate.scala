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
import edu.berkeley.cs.amplab.adam.util.PhredQualityScore
import scala.math.Ordering.Implicits._

trait Covariate {
  type Value

  implicit def ordering: Ordering[Value]

  def compute(residue: Residue): Value

  def apply(residue: Residue): Value = compute(residue)

  def compare(left: Value, right: Value): Int
}

abstract class AbstractCovariate[ValueT](implicit val ordering: Ordering[ValueT])
  extends Covariate with Serializable {

  override type Value = ValueT

  override def compare(left: Value, right: Value): Int = {
    ordering.compare(left, right)
  }
}

class ReadGroupCovariate extends AbstractCovariate[String] {
  override def compute(residue: Residue) = residue.read.readGroup

  override def equals(other: Any): Boolean = other match {
    case that: ReadGroupCovariate => true
    case _ => false
  }

  override def hashCode = 0xFF972A0B
}

class QualityScoreCovariate extends AbstractCovariate[PhredQualityScore] {
  override def compute(residue: Residue) = residue.quality

  override def equals(other: Any): Boolean = other match {
    case that: QualityScoreCovariate => true
    case _ => false
  }

  override def hashCode = 0xC5354788
}
