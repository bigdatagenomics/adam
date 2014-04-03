/*
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.metrics.aggregators

import org.scalatest._

class AggregatorSuite extends FunSuite {

  test("UniqueAggregator only collects unique values") {
    val agg = new UniqueAggregator[String]()
    val vals: Seq[String] = Seq("x", "x", "y", "x")
    val aggregated = vals.map(x => agg.lift(Seq(x))).reduce(agg.combine)

    assert(aggregated.values.size === 2)
    assert(aggregated.values.contains("x"))
    assert(aggregated.values.contains("y"))
  }

  test("HistogramAggregator counts values correctly") {
    val agg = new HistogramAggregator[Int]()
    val vals: Seq[Int] = Seq(0, 0, 1)
    val aggregated = vals.map(x => agg.lift(Seq(x))).reduce(agg.combine)

    assert(aggregated.count() === 3)
    assert(aggregated.countIdentical() === 2)
    assert(aggregated.valueToCount(0) === 2)
    assert(aggregated.valueToCount(1) === 1)
  }

}
