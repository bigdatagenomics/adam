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

class DinucCovariateSuite extends FunSuite {

  val dc = new DinucCovariate

  test("computing dinucleotide pairs for a single base sequence should return (N,N)") {
    val dinucs = dc.fwdDinucs("A")
    assert(dinucs.size === 1)
    assert(dinucs(0) === ('N', 'N'))
  }

  test("compute dinucleotide pairs for a string of all valid bases") {
    val dinucs = dc.fwdDinucs("AGCGT")
    assert(dinucs.size === 5)
    assert(dinucs(0) === ('N', 'N'))
    assert(dinucs(1) === ('A', 'G'))
    assert(dinucs(2) === ('G', 'C'))
    assert(dinucs(3) === ('C', 'G'))
    assert(dinucs(4) === ('G', 'T'))
  }

  test("compute dinucleotide pairs for a string with an N") {
    val dinucs = dc.fwdDinucs("AGNGT")
    assert(dinucs.size === 5)
    assert(dinucs(0) === ('N', 'N'))
    assert(dinucs(1) === ('A', 'G'))
    assert(dinucs(2) === ('N', 'N'))
    assert(dinucs(3) === ('N', 'N'))
    assert(dinucs(4) === ('G', 'T'))
  }

  test("compute covariates for a read on the negative strand") {
    val read = Alignment.newBuilder()
      .setSequence("AGCCTNGT")
      .setQualityScores("********")
      .setReadNegativeStrand(true)
      .setReadMapped(true)
      .setStart(10L)
      .setMappingQuality(50)
      .setCigar("8M")
      .build
    val covariates = dc.compute(read)
    assert(covariates.size === 8)
    assert(covariates(0) === ('C', 'T'))
    assert(covariates(1) === ('G', 'C'))
    assert(covariates(2) === ('G', 'G'))
    assert(covariates(3) === ('A', 'G'))
    assert(covariates(4) === ('N', 'N'))
    assert(covariates(5) === ('N', 'N'))
    assert(covariates(6) === ('A', 'C'))
    assert(covariates(7) === ('N', 'N'))
  }

  test("compute covariates for a read on the positive strand") {
    val read = Alignment.newBuilder()
      .setSequence("ACNAGGCT")
      .setQualityScores("********")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setStart(10L)
      .setMappingQuality(50)
      .setCigar("8M")
      .build
    val covariates = dc.compute(read)
    assert(covariates.size === 8)
    assert(covariates(0) === ('N', 'N'))
    assert(covariates(1) === ('A', 'C'))
    assert(covariates(2) === ('N', 'N'))
    assert(covariates(3) === ('N', 'N'))
    assert(covariates(4) === ('A', 'G'))
    assert(covariates(5) === ('G', 'G'))
    assert(covariates(6) === ('G', 'C'))
    assert(covariates(7) === ('C', 'T'))
  }
}
