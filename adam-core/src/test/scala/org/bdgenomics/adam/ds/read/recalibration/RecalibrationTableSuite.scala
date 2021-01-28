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
package org.bdgenomics.adam.ds.read.recalibration

import org.bdgenomics.formats.avro.Alignment
import org.scalatest.FunSuite

class RecalibrationTableSuite extends FunSuite {

  val observedCovariates = Map((CovariateKey(0,
    (50 + 33).toChar,
    2,
    'A',
    'C') -> new Aggregate(1000000, 1, 10.0)),
    (CovariateKey(0,
      (40 + 33).toChar,
      1,
      'N',
      'N') -> new Aggregate(100000, 1, 10.0)))
  val table = RecalibrationTable(new ObservationTable(
    observedCovariates))

  test("look up quality scores in table") {
    val scores = table(observedCovariates.map(_._1).toArray)

    assert(scores.size === 2)
    assert(scores(0) === (50 + 33).toChar)
    assert(scores(1) === (47 + 33).toChar)
  }
}
