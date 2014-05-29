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

import org.apache.commons.math3.special.Gamma
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import scala.math.{ log => mathLog, max, pow }
import scala.reflect.ClassTag

object GammaMixtureModel
    extends MixtureOfUCDInitByKMeansFitByEM[GammaDistribution] {

  override val quitEarly = true

  protected def mStep(rdd: RDD[(Array[Double], Double)],
                      distributions: Array[GammaDistribution],
                      iter: Int): (Array[GammaDistribution], Array[Double]) = {
    // persist rdd
    rdd.cache()

    // compute weighting
    val weighting = rdd.map(kv => kv._1).aggregate(new Array[Double](distributions.length))(
      aggregateArray, aggregateArray)
    val count = rdd.count().toDouble

    // collect trusted distribution
    val newDistributions = update(rdd, weighting, distributions, 10.0 / (iter.toDouble + 10.0))

    // unpersist rdd
    rdd.unpersist()

    // return distributions
    (newDistributions, weighting.map(_ / count))
  }

  private def update(rdd: RDD[(Array[Double], Double)],
                     weights: Array[Double],
                     previousDistributions: Array[GammaDistribution],
                     a: Double): Array[GammaDistribution] = {
    val numDistributions = weights.length
    val n = weights.sum

    // compute coefficients to avoid recomputing them in map
    val psiAlphas = previousDistributions.map(d => psi(d.alpha))
    val logLambdas = previousDistributions.map(d => mathLog(d.beta))
    val coeffs = (0 until numDistributions).map(i => logLambdas(i) - psiAlphas(i)).toArray

    // map to collect sufficient statistics
    val lambdaDenominators = rdd.map(kv => kv._1.map(_ * kv._2))
      .aggregate(new Array[Double](numDistributions))(aggregateArray, aggregateArray)
    val g = rdd.map(kv => (0 until numDistributions).map(i => kv._1(i) * (mathLog(kv._2) + coeffs(i))).toArray)
      .aggregate(new Array[Double](numDistributions))(aggregateArray, aggregateArray)

    // create new distribution
    (0 until numDistributions).map(i => GammaDistribution(previousDistributions(i).alpha + a * (g(i) / n),
      (previousDistributions(i).alpha * weights(i)) / lambdaDenominators(i)))
      .toArray
  }

  protected def initializeDistribution(mean: Double, sigma: Double): GammaDistribution = {
    GammaDistribution.fromMeanAndSigma(mean, sigma)
  }

  private def psi(x: Double): Double = {
    // we have a reasonable approximation for x >= 2; else, call to apache which has a
    // more precise but more expensive implementation
    if (x >= 2.0) {
      mathLog(x - 0.5) + 1.0 / (24 * pow(x - 0.5, 2.0))
    } else {
      Gamma.digamma(x)
    }
  }
}
