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
import org.bdgenomics.adam.util.ADAMFunSuite
import scala.math.{ abs, sqrt }

class ZScoreNormalizationSuite extends ADAMFunSuite {
  def fpEquals(n1: Double, n2: Double, epsilon: Double = 1e-6): Boolean = {
    abs(n1 - n2) < epsilon
  }

  sparkTest("compute mean of a set of samples") {
    val rdd = sc.parallelize(Seq(3.0, 4.0, 5.0, 4.0, 5.0, 3.0, 2.0, 6.0))

    assert(fpEquals(4.0, ZScoreNormalization.mean(rdd, rdd.count)))
  }

  sparkTest("compute variance of a set of samples") {
    val rdd = sc.parallelize(Seq(3.0, 4.0, 5.0, 4.0, 5.0, 3.0, 2.0, 6.0))

    val expected = (4.0 * 1.0 + 2.0 * 4.0) / 8.0

    assert(fpEquals(expected, ZScoreNormalization.variance(rdd, rdd.count, 4.0)))
  }

  sparkTest("variance should be 0 if all elements are the same") {
    val rdd = sc.parallelize(Seq(3.0, 3.0, 3.0, 3.0, 3.0))

    assert(fpEquals(0.0, ZScoreNormalization.variance(rdd, rdd.count,
      ZScoreNormalization.mean(rdd, rdd.count))))
  }

  sparkTest("check z-score for a varying rdd") {
    // this rdd contains a set of values whose square roots are equal to their z-score
    // for this rdd, µ = 0.0, σ = 2.0
    val rdd = sc.parallelize(Seq(-2.0, 0.0, 0.0, 2.0))

    val r = ZScoreNormalization(rdd.map(v => (v, 1)))
      .map(kv => kv._1)
      .zip(rdd)
      .collect()

    r.foreach(p => {
      val p2 = if (p._2 != 0.0) {
        sqrt(abs(p._2)) * p._2 / abs(p._2)
      } else {
        0.0 // need this, else we try to div by 0
      }
      assert(fpEquals(p._1, p2))
    })
  }
}
