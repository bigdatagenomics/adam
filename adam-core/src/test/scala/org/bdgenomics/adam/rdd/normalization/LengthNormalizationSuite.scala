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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.util.SparkFunSuite
import scala.math.{ abs, sqrt }

class LengthNormalizationSuite extends SparkFunSuite {
  def fpEquals(n1: Double, n2: Double, epsilon: Double = 1e-6): Boolean = {
    abs(n1 - n2) < epsilon
  }

  sparkTest("normalize a single targeted region") {
    val rdd = sc.parallelize(Seq(((1000.0, ReferenceRegion("chr1", 0L, 1001L)), 1)))

    LengthNormalization(rdd)
      .map(t => t._1._1)
      .collect()
      .foreach(fpEquals(_, 1.0))
  }

  sparkTest("normalize a set of targeted regions") {
    val rddVals = sc.parallelize(Seq(1.0, 5.0, 3.0, 4.0, 2.0))

    val rdd = rddVals.zip(sc.parallelize(Seq(1000.0, 500.0, 3215.0, 10000.0, 55000.0)))
      .map(kv => ((kv._1 * kv._2, ReferenceRegion("", 0L, kv._2.toLong + 1)), 1))

    LengthNormalization(rdd)
      .map(t => t._1._1)
      .zip(rddVals)
      .collect()
      .foreach(p => fpEquals(p._1, p._2))
  }

  sparkTest("calculate *pkm type normalization for a set of targeted regions") {
    val rddVals = sc.parallelize(Seq(1.0, 5.0, 3.0, 4.0, 2.0))

    val rdd = rddVals.map(_ * 100000.0)
      .zip(sc.parallelize(Seq(1000.0, 500.0, 3215.0, 10000.0, 55000.0)))
      .map(kv => ((kv._1 * kv._2, ReferenceRegion("", 0L, kv._2.toLong + 1)), 1))

    LengthNormalization.pkn(rdd)
      .map(t => t._1._1)
      .zip(rddVals)
      .collect()
      .foreach(p => fpEquals(p._1, p._2))
  }
}
