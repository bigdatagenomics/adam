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

import scala.math.abs

object CategoricalDistribution {

  def softmax(likelihoods: Array[Double]): CategoricalDistribution = {
    val sum = likelihoods.sum
    CategoricalDistribution(likelihoods.map(l => l / sum))
  }
}

case class CategoricalDistribution(probabilities: Array[Double])
    extends UnivariateDiscreteDistribution {

  assert(abs(1.0 - probabilities.sum) < 1e-6, "Probabilities must sum to 1. Was given: " + probabilities.toString)
  private val categories = probabilities.length

  def probability(category: Int): Double = {
    assert(category >= 0 && category < categories,
      "Category out of range: " + category + ", only have " + categories + " categories.")
    probabilities(category)
  }

  override def toString(): String = {
    "categories: " + (0 until categories).map(i => i + ": " + probabilities(i)).reduce(_ + ", " + _)
  }
}
