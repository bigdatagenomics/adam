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
package org.bdgenomics.adam.rdd.read

import org.bdgenomics.formats.avro.{ AlignmentRecord, Base }
import org.scalatest.FunSuite

class Reads2PileupProcessorSuite extends FunSuite {

  test("can convert a single read with only matches") {
    val quals = List(30, 20, 40, 20, 10)
    val qualString: String = quals.map(p => (p + 33).toChar.toString).fold("")(_ + _)
    val sequence = "ACTAG"

    // build a read with 5 base pairs, all match
    val record: AlignmentRecord = AlignmentRecord.newBuilder()
      .setStart(1L)
      .setMapq(30)
      .setSequence(sequence)
      .setCigar("5M")
      .setEnd(6L)
      .setMismatchingPositions("5")
      .setQual(qualString)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .build()

    val converter = new Reads2PileupProcessor

    // convert pileups
    val pileups = converter.readToPileups(record)

    assert(pileups.length === 5)
    assert(pileups.sortBy(_.getPosition).map(_.getReadBase.toString).fold("")(_ + _) === sequence)
    assert(pileups.sortBy(_.getPosition).map(p => List(p.getSangerQuality))
      .fold(List[Int]())(_ ::: _) === quals)
    assert(pileups.forall(r => r.getReadBase == r.getReferenceBase))
    assert(pileups.forall(_.getMapQuality == 30))
    assert(pileups.forall(_.getRangeLength == null))
  }

  test("can convert a single read with matches and mismatches") {
    val quals = List(30, 20, 40, 20, 10)
    val qualString: String = quals.map(p => (p + 33).toChar.toString).fold("")(_ + _)
    val sequence = "ACTAG"

    // build a read with 5 base pairs, all match
    val record: AlignmentRecord = AlignmentRecord.newBuilder()
      .setStart(1L)
      .setMapq(30)
      .setSequence(sequence)
      .setCigar("5M")
      .setEnd(6L)
      .setMismatchingPositions("4A0")
      .setQual(qualString)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .build()

    val converter = new Reads2PileupProcessor

    // convert pileups
    val pileups = converter.readToPileups(record)

    assert(pileups.length === 5)
    assert(pileups.sortBy(_.getPosition).map(_.getReadBase.toString).fold("")(_ + _) === sequence)
    assert(pileups.sortBy(_.getPosition).map(p => List(p.getSangerQuality))
      .fold(List[Int]())(_ ::: _) === quals)
    assert(pileups.filter(_.getPosition < 5L).forall(r => r.getReadBase == r.getReferenceBase))
    assert(pileups.filter(_.getPosition == 5L).forall(r => r.getReadBase != r.getReferenceBase))
    assert(pileups.filter(_.getPosition == 5L).forall(r => r.getReferenceBase == Base.A))
    assert(pileups.forall(_.getMapQuality == 30))
    assert(pileups.forall(_.getRangeLength == null))
  }

}
