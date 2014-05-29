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
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.SparkFunSuite
import scala.math.abs

class GammaMixtureModelSuite extends SparkFunSuite {

  def fpCompare(a: Double, b: Double, epsilon: Double = 1e-3): Boolean = {
    abs(a - b) <= epsilon
  }

  sparkTest("fit a single gamma") {
    // give a random seed to avoid non-determinism between different runs
    val rv = new MersenneTwister(1111L)
    val gamma = new ApacheGammaDistribution(rv, 2.0, 0.5)

    val randomValues = (0 until 10000).map(i => gamma.sample)

    val rdd = sc.parallelize(randomValues)

    val distributions = GammaMixtureModel.fit(rdd, 1, 100)

    assert(distributions.length === 1)
    assert(fpCompare(distributions(0).alpha, 2.0, 0.1))
    assert(fpCompare(distributions(0).beta, 2.0, 0.05))
  }

  sparkTest("fit two gaussians with same beta") {
    // give a random seed to avoid non-determinism between different runs
    val rv = new MersenneTwister(1234L)
    val gamma0 = new ApacheGammaDistribution(rv, 2.0, 0.5)
    val gamma1 = new ApacheGammaDistribution(rv, 8.0, 2.0)

    val randomValues0 = (0 until 5000).map(i => gamma0.sample)
    val randomValues1 = (0 until 5000).map(i => gamma1.sample)

    val rdd = sc.parallelize(randomValues0 ++ randomValues1)

    val distributions = GammaMixtureModel.fit(rdd, 2, 100)

    assert(distributions.length === 2)
    assert(fpCompare(distributions(0).alpha, 2.0, 0.2))
    assert(fpCompare(distributions(0).beta, 2.0, 0.2))
    assert(fpCompare(distributions(1).alpha, 8.0, 1.6))
    assert(fpCompare(distributions(1).beta, 0.5, 0.1))
  }

  sparkTest("fit two overlapping gammas") {
    // give a random seed to avoid non-determinism between different runs
    val rv = new MersenneTwister(4321L)
    val gamma0 = new ApacheGammaDistribution(rv, 1.0, 0.5)
    val gamma1 = new ApacheGammaDistribution(rv, 9.0, 2.0)

    val randomValues0 = (0 until 5000).map(i => gamma0.sample)
    val randomValues1 = (0 until 5000).map(i => gamma1.sample)

    val rdd = sc.parallelize(randomValues0 ++ randomValues1)

    val distributions = GammaMixtureModel.fit(rdd, 2, 100)

    assert(distributions.length === 2)
    assert(fpCompare(distributions(0).alpha, 1.0, 0.05))
    assert(fpCompare(distributions(0).beta, 2.0, 0.05))
    assert(fpCompare(distributions(1).alpha, 9.0, 0.7))
    assert(fpCompare(distributions(1).beta, 0.5, 0.05))
  }
}
