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
import org.bdgenomics.adam.util.SparkFunSuite
import scala.math.abs
import scala.util.Random

class UnivariateNormalMixtureModelSuite extends SparkFunSuite {

  def fpCompare(a: Double, b: Double, epsilon: Double = 1e-3): Boolean = {
    abs(a - b) <= epsilon
  }

  sparkTest("fit a single gaussian") {
    // give a random seed to avoid non-determinism between different runs
    val rv = new Random(1111L)

    val randomValues0 = (0 until 10000).map(i => rv.nextGaussian)

    val rdd = sc.parallelize(randomValues0)

    val distributions = UnivariateNormalMixtureModel.fit(rdd, 1, 100)

    assert(distributions.length === 1)
    assert(fpCompare(distributions(0).mu, 0.0, 0.05))
    assert(fpCompare(distributions(0).sigma, 1.0, 0.05))
  }

  sparkTest("fit two gaussians with same standard deviation") {
    // give a random seed to avoid non-determinism between different runs
    val rv = new Random(1234L)

    val randomValues0 = (0 until 5000).map(i => rv.nextGaussian)
    val randomValues2 = (0 until 5000).map(i => 2.0 + rv.nextGaussian)

    val rdd = sc.parallelize(randomValues0 ++ randomValues2)

    val distributions = UnivariateNormalMixtureModel.fit(rdd, 2, 100)

    assert(distributions.length === 2)
    assert(fpCompare(distributions(0).mu, 0.0, 0.05))
    assert(fpCompare(distributions(0).sigma, 1.0, 0.05))
    assert(fpCompare(distributions(1).mu, 2.0, 0.05))
    assert(fpCompare(distributions(1).sigma, 1.0, 0.05))
  }

  sparkTest("fit two overlapping gaussians") {
    // give a random seed to avoid non-determinism between different runs
    val rv = new Random(4321L)

    val randomValues0p5 = (0 until 5000).map(i => rv.nextGaussian)
    val randomValues2 = (0 until 5000).map(i => 1.0 + rv.nextGaussian)

    val rdd = sc.parallelize(randomValues0p5 ++ randomValues2)

    val distributions = UnivariateNormalMixtureModel.fit(rdd, 2, 100)

    assert(distributions.length === 2)
    assert(fpCompare(distributions(0).mu, 0.0, 0.1))
    assert(fpCompare(distributions(0).sigma, 1.0, 0.1))
    assert(fpCompare(distributions(1).mu, 1.0, 0.1))
    assert(fpCompare(distributions(1).sigma, 1.0, 0.1))
  }
}
