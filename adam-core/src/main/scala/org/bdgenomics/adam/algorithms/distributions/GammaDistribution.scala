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

import org.apache.commons.math3.distribution.{ GammaDistribution => ApacheGammaDistribution }
import org.apache.commons.math3.special.Gamma
import scala.math.{ exp, pow }

object GammaDistribution {

  /**
   * For a given mean and standard deviation (σ), we can build a Gamma
   * distribution. This is because we know that the mean of a Gamma distribution
   * equals α/β, and the standard deviation of a Gamma distribution equals
   * sqrt(α)/β.
   *
   * @param mean Average of points in the distribution.
   * @param sigma Standard deviation of points in the distribution.
   * @return Returns a Gamma Distribution that maximizes the likelihood that
   *         corresponds to this mean and sigma.
   */
  def fromMeanAndSigma(mean: Double, sigma: Double): GammaDistribution = {
    val alpha = pow(2.0, mean / sigma)
    GammaDistribution(alpha, alpha / mean)
  }

  def apply(alpha: Double, beta: Double): GammaDistribution = {
    // if we can compute the fixed coefficients explicitly, use our lightweight implementation
    if ((pow(beta, alpha) / Gamma.gamma(alpha)).isNaN) {
      val aGamma = new ApacheGammaDistribution(alpha, 1.0 / beta)
      new SerializableApacheGammaWrapper(aGamma)
    } else {
      new GammaDistribution(alpha, beta)
    }
  }
}

class GammaDistribution protected (val alpha: Double, val beta: Double)
    extends UnivariateContinuousDistribution with Serializable {

  // precalculate the denominator and invert, as the gamma function is expensive
  val coefficient = pow(beta, alpha) / Gamma.gamma(alpha)

  /**
   * The probability density function of the Gamma distribution is:
   *
   * p(x | α, β) = \frac{x^{α - 1} exp(-xβ)}{\frac{1}{β}^α Γ(α)}
   *
   * For computational efficiency, we precompute the denominator.
   *
   * @param x Input value.
   * @return The probability density of x in this distribution.
   */
  def probabilityDensity(x: Double): Double = {
    coefficient * pow(x, alpha - 1.0) * exp(x * -beta)
  }

  override def toString(): String = {
    "α: " + alpha + " β: " + beta
  }
}

private class SerializableApacheGammaWrapper(aGamma: ApacheGammaDistribution)
    extends GammaDistribution(aGamma.getShape, 1.0 / aGamma.getScale) {

  /**
   * The probability density function of the Gamma distribution is:
   *
   * p(x | α, β) = \frac{x^{α - 1} exp(-xβ)}{\frac{1}{β}^α Γ(α)}
   *
   * @param x Input value.
   * @return The probability density of x in this distribution.
   */
  override def probabilityDensity(x: Double): Double = {
    aGamma.density(x)
  }
}
