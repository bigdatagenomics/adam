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
package org.bdgenomics.adam.rdd.read.recalibration

import org.bdgenomics.formats.avro.AlignmentRecord
import org.scalatest.FunSuite

class CycleCovariateSuite extends FunSuite {

  val cc = new CycleCovariate

  test("compute covariates for an unpaired read on the negative strand") {
    val read = AlignmentRecord.newBuilder()
      .setSequence("AGCCTNGT")
      .setQuality("********")
      .setReadNegativeStrand(true)
      .setReadMapped(true)
      .setStart(10L)
      .setMappingQuality(50)
      .setCigar("8M")
      .build
    val covariates = cc.compute(read)
    assert(covariates.size === 8)
    assert(covariates(0) === 8)
    assert(covariates(1) === 7)
    assert(covariates(2) === 6)
    assert(covariates(3) === 5)
    assert(covariates(4) === 4)
    assert(covariates(5) === 3)
    assert(covariates(6) === 2)
    assert(covariates(7) === 1)
  }

  test("compute covariates for a first-of-pair read on the negative strand") {
    val read = AlignmentRecord.newBuilder()
      .setSequence("AGCCTNGT")
      .setQuality("********")
      .setReadNegativeStrand(true)
      .setReadMapped(true)
      .setStart(10L)
      .setMappingQuality(50)
      .setCigar("8M")
      .setReadPaired(true)
      .setReadInFragment(0)
      .build
    val covariates = cc.compute(read)
    assert(covariates.size === 8)
    assert(covariates(0) === 8)
    assert(covariates(1) === 7)
    assert(covariates(2) === 6)
    assert(covariates(3) === 5)
    assert(covariates(4) === 4)
    assert(covariates(5) === 3)
    assert(covariates(6) === 2)
    assert(covariates(7) === 1)
  }

  test("compute covariates for a second-of-pair read on the negative strand") {
    val read = AlignmentRecord.newBuilder()
      .setSequence("AGCCTNGT")
      .setQuality("********")
      .setReadNegativeStrand(true)
      .setReadMapped(true)
      .setStart(10L)
      .setMappingQuality(50)
      .setCigar("8M")
      .setReadPaired(true)
      .setReadInFragment(1)
      .build
    val covariates = cc.compute(read)
    assert(covariates.size === 8)
    assert(covariates(0) === -8)
    assert(covariates(1) === -7)
    assert(covariates(2) === -6)
    assert(covariates(3) === -5)
    assert(covariates(4) === -4)
    assert(covariates(5) === -3)
    assert(covariates(6) === -2)
    assert(covariates(7) === -1)
  }

  test("compute covariates for an unpaired read on the positive strand") {
    val read = AlignmentRecord.newBuilder()
      .setSequence("ACNAGGCT")
      .setQuality("********")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setStart(10L)
      .setMappingQuality(50)
      .setCigar("8M")
      .build
    val covariates = cc.compute(read)
    assert(covariates.size === 8)
    assert(covariates(0) === 1)
    assert(covariates(1) === 2)
    assert(covariates(2) === 3)
    assert(covariates(3) === 4)
    assert(covariates(4) === 5)
    assert(covariates(5) === 6)
    assert(covariates(6) === 7)
    assert(covariates(7) === 8)
  }

  test("compute covariates for a first-of-pair read on the positive strand") {
    val read = AlignmentRecord.newBuilder()
      .setSequence("ACNAGGCT")
      .setQuality("********")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setStart(10L)
      .setMappingQuality(50)
      .setCigar("8M")
      .setReadPaired(true)
      .setReadInFragment(0)
      .build
    val covariates = cc.compute(read)
    assert(covariates.size === 8)
    assert(covariates(0) === 1)
    assert(covariates(1) === 2)
    assert(covariates(2) === 3)
    assert(covariates(3) === 4)
    assert(covariates(4) === 5)
    assert(covariates(5) === 6)
    assert(covariates(6) === 7)
    assert(covariates(7) === 8)
  }

  test("compute covariates for a second-of-pair read on the positive strand") {
    val read = AlignmentRecord.newBuilder()
      .setSequence("ACNAGGCT")
      .setQuality("********")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setStart(10L)
      .setMappingQuality(50)
      .setCigar("8M")
      .setReadPaired(true)
      .setReadInFragment(1)
      .build
    val covariates = cc.compute(read)
    assert(covariates.size === 8)
    assert(covariates(0) === -1)
    assert(covariates(1) === -2)
    assert(covariates(2) === -3)
    assert(covariates(3) === -4)
    assert(covariates(4) === -5)
    assert(covariates(5) === -6)
    assert(covariates(6) === -7)
    assert(covariates(7) === -8)
  }
}
