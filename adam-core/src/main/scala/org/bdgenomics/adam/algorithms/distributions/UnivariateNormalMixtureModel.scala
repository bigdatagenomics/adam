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

import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import scala.math.{ log => mathLog, max, pow, sqrt }
import scala.reflect.ClassTag

object UnivariateNormalMixtureModel
    extends MixtureOfUCDInitByKMeansFitByEM[UnivariateNormalDistribution] {

  protected def mStep(rdd: RDD[(Array[Double], Double)],
                      distributions: Array[UnivariateNormalDistribution],
                      iter: Int): (Array[UnivariateNormalDistribution], Array[Double]) = {
    // persist rdd
    rdd.cache()

    // compute weighting
    val weighting = rdd.map(kv => kv._1).aggregate(new Array[Double](distributions.length))(
      aggregateArray, aggregateArray)
    val count = rdd.count().toDouble

    // collect trusted distribution
    val newDistributions = update(rdd, weighting)

    // unpersist rdd
    rdd.unpersist()

    // return distributions
    (newDistributions, weighting.map(_ / count))
  }

  protected def initializeDistribution(mean: Double, sigma: Double): UnivariateNormalDistribution = {
    UnivariateNormalDistribution(mean, sigma)
  }

  protected def update(rdd: RDD[(Array[Double], Double)],
                       weights: Array[Double]): Array[UnivariateNormalDistribution] = {
    val numDistributions = weights.length

    // map to collect sufficient statistics
    val mus = rdd.map(kv => kv._1.map(_ * kv._2))
      .aggregate(new Array[Double](numDistributions))(aggregateArray, aggregateArray)
    (0 until mus.length).foreach(i => mus(i) /= weights(i))

    val sigmas = rdd.map(kv => (0 until numDistributions).map(i => kv._1(0) * pow(kv._2 - mus(0), 2.0)).toArray)
      .aggregate(new Array[Double](numDistributions))(aggregateArray, aggregateArray)

    // create new distribution
    (0 until numDistributions).map(i => UnivariateNormalDistribution(mus(i),
      sqrt(sigmas(i) / weights(i))))
      .toArray
  }
}
