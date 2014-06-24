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
package org.bdgenomics.adam.converters

import org.scalatest.FunSuite

class GenotypesToVariantsConverterSuite extends FunSuite {

  test("Simple test of integer RMS") {
    val v = new GenotypesToVariantsConverter
    assert(v.rms(List(1, 1)) === 1)
  }

  test("Simple test of floating point RMS") {
    val v = new GenotypesToVariantsConverter
    val rmsVal = v.rms(List(39.0, -40.0, 41.0))

    // floating point, so apply tolerances
    assert(rmsVal > 40.0 && rmsVal < 40.01)
  }

  test("Max genotype quality should lead to max variant quality") {
    val v = new GenotypesToVariantsConverter
    // if p = 1, then the rest of our samples don't matter
    val vq = v.variantQualityFromGenotypes(List(1.0, 0.0))

    // floating point, so apply tolerances
    assert(vq > 0.999 && vq < 1.001)
  }

  test("Genotype quality = 0.5 for two samples should lead to variant quality of 0.75") {
    val v = new GenotypesToVariantsConverter
    val vq = v.variantQualityFromGenotypes(List(0.5, 0.5))

    // floating point, so apply tolerances
    assert(vq > 0.745 && vq < 0.755)
  }
}
