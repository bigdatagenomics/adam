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
package org.bdgenomics.adam.rdd.normalization

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import scala.math.sqrt

object ZScoreNormalization extends Serializable with Logging {

  /**
   * Normalizes an RDD of double values by computing the Z score for each value.
   * Per point, the Z score (also known as standard score) is computed by
   * subtracting the mean across all values from the point, and then dividing
   * by the standard deviation across all points.
   *
   * @param rdd RDD of (Double, Value) pairs to be normalized.
   * @returns Returns an RDD where the original double value has been replaced
   * by the Z score for that point.
   *
   * @tparam T Type of data passed along.
   */
  def apply[T](rdd: RDD[(Double, T)]): RDD[(Double, T)] = {
    val cachedRdd = rdd.cache

    // compute mean and standard deviation
    val n = cachedRdd.count
    val mu = mean(cachedRdd.map(kv => kv._1), n)
    val sigma = sqrt(variance(cachedRdd.map(kv => kv._1), n, mu))

    // update keys
    log.info("Normalizing by z-score with µ: " + mu + " and σ: " + sigma)
    val update = cachedRdd.map(kv => ((kv._1 - mu) / sigma, kv._2))

    // unpersist rdd
    cachedRdd.unpersist()

    // return
    update
  }

  /**
   * Computes the mean of a set of samples.
   *
   * @param rdd An RDD of doubles.
   * @param n The number of samples in the RDD.
   * @return Returns the mean of the RDD of doubles.
   */
  private[normalization] def mean(rdd: RDD[Double], n: Long): Double = {
    rdd.reduce(_ + _) / n.toDouble
  }

  /**
   * Computes the variance of a set of samples.
   *
   * @param rdd An RDD of doubles.
   * @param n The number of samples in the RDD.
   * @param mu The mean of all the samples in the RDD.
   * @return Returns the mean of the RDD of doubles.
   */
  private[normalization] def variance(rdd: RDD[Double], n: Long, mu: Double): Double = {
    rdd.map(d => (d - mu) * (d - mu)).reduce(_ + _) / n.toDouble
  }
}
