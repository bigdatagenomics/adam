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

import org.bdgenomics.adam.models.{
  ReadGroup,
  ReadGroupDictionary
}
import org.bdgenomics.formats.avro.Alignment
import org.scalatest.FunSuite

class RecalibratorSuite extends FunSuite {

  val table = RecalibrationTable(new ObservationTable(
    Map((CovariateKey(0,
      (50 + 33).toChar,
      2,
      'A',
      'C') -> new Aggregate(1000000, 1, 10.0)),
      (CovariateKey(0,
        (40 + 33).toChar,
        1,
        'N',
        'N') -> new Aggregate(100000, 1, 10.0)))))
  val rgd = ReadGroupDictionary(Seq(ReadGroup("s", "rg0")))

  val read = Alignment.newBuilder
    .setReferenceName("chr1")
    .setReadGroupId("rg0")
    .setStart(10L)
    .setEnd(12L)
    .setSequence("AC")
    .setReadNegativeStrand(false)
    .setQualityScores(Seq(40, 50).map(i => (i + 33).toChar).mkString)
    .setDuplicateRead(false)
    .setReadMapped(true)
    .setReadPaired(false)
    .setReadInFragment(0)
    .setPrimaryAlignment(true)
    .setCigar("2M")
    .setMismatchingPositions("2")
    .setMappingQuality(40)
    .build

  val hiRecalibrator = Recalibrator(table, (48 + 33).toChar)
  val lowRecalibrator = Recalibrator(table, (40 + 33).toChar)

  test("don't replace quality if quality was null") {
    val qualFreeRead = Alignment.newBuilder(read)
      .setQualityScores(null)
      .build
    val recalibratedRead = lowRecalibrator(qualFreeRead,
      Array.empty)
    assert(recalibratedRead.getQualityScores === null)
    assert(recalibratedRead.getOriginalQualityScores === null)
  }

  test("if no covariates, return alignment") {
    val emptyRead = Alignment.newBuilder
      .setReadName("emptyRead")
      .build
    val notRecalibratedRead = lowRecalibrator(emptyRead, Array.empty)
    assert(emptyRead === notRecalibratedRead)
  }

  test("skip recalibration if base is below quality threshold") {
    val recalibratedRead = hiRecalibrator(read,
      BaseQualityRecalibration.observe(read, rgd))
    val expectedRead = Alignment.newBuilder(read)
      .setOriginalQualityScores(read.getQualityScores)
      .build
    assert(recalibratedRead === expectedRead)
  }

  test("recalibrate changed bases above quality threshold") {
    val recalibratedRead = lowRecalibrator(read,
      BaseQualityRecalibration.observe(read, rgd))
    val expectedRead = Alignment.newBuilder(read)
      .setQualityScores(Seq(47, 50).map(i => (i + 33).toChar).mkString)
      .setOriginalQualityScores(read.getQualityScores)
      .build
    assert(recalibratedRead === expectedRead)
  }
}
