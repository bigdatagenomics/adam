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
import org.bdgenomics.adam.models.Interval

object LengthNormalization extends Serializable with Logging {

  /**
   * Normalizes an RDD that contains a double value and an interval by the width
   * of the interval.
   *
   * @param rdd An RDD containing (a value to be normalized, an interval, and an
   * additional data value), for normalization.
   * @return Returns an RDD containing (the double normalized by the interval
   * length, the original interval, the original data value) after normalization.
   *
   * @tparam T Datatype of additional value parameter to maintain.
   *
   * @see pkn
   */
  def apply[I <: Interval, T](rdd: RDD[((Double, I), T)]): RDD[((Double, I), T)] = {
    rdd.map(t => ((t._1._1 / t._1._2.width, t._1._2), t._2))
  }

  /**
   * Normalizes an RDD that contains a double value and an interval by the width
   * of the interval and the total aggregate value of all values. This is useful
   * for calculating entities like reads/fragments per kilobase of transcript
   * per million reads (RPKM/FPKM).
   *
   * @param rdd An RDD containing (a value to be normalized, an interval, and an
   * additional data value), for normalization.
   * @param n Global normalization factor. E.g., for RPKM, n = 1,000,000 (reads
   * per kilobase transcript per _million_ reads).
   *
   * @return Returns an RDD containing (the double normalized by the interval
   * length, the original interval, the original data value) after normalization.
   *
   * @tparam T Datatype of additional value parameter to maintain.
   *
   * @see apply
   */
  def pkn[I <: Interval, T](rdd: RDD[((Double, I), T)],
                            k: Double = 1000.0,
                            n: Double = 1000000.0): RDD[((Double, I), T)] = {
    val cachedRdd = rdd.cache

    // generate count
    val norm = cachedRdd.map(kv => kv._1._1).reduce(_ + _) / n

    // normalize the RDD
    val normalizedRdd = apply(cachedRdd).map(t => ((t._1._1 * norm * k, t._1._2), t._2))

    // uncache
    cachedRdd.unpersist()

    // return
    normalizedRdd
  }
}
