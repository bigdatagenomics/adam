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
package org.bdgenomics.adam.algorithms.distributions

import scala.math.{ exp, Pi, pow, sqrt }

case class UnivariateNormalDistribution(mu: Double, sigma: Double)
    extends UnivariateContinuousDistribution {

  assert(sigma > 0.0, "Sigma must be strictly greater than 0. Was provided: " + toString())

  val norm = sigma * sqrt(2.0 * Pi)
  val sigma2 = 2 * pow(sigma, 2.0)

  def probabilityDensity(point: Double): Double = {
    exp(-pow(point - mu, 2.0) / sigma2) / norm
  }

  override def toString(): String = {
    "µ: " + mu + " σ: " + sigma
  }
}
