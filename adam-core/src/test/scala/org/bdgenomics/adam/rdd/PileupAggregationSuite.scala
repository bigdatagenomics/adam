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
package org.bdgenomics.adam.rdd

import org.scalatest.FunSuite
import org.bdgenomics.formats.avro.{ ADAMContig, ADAMPileup, Base }

class PileupAggregationSuite extends FunSuite {

  test("aggregating a pileup with two different bases does not change values") {
    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.A)
      .setMapQuality(10)
      .setSangerQuality(30)
      .setCountAtPosition(1)
      .setNumSoftClipped(0)
      .setNumReverseStrand(0)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setReadBase(Base.C)
      .setMapQuality(20)
      .setSangerQuality(40)
      .setCountAtPosition(1)
      .setNumSoftClipped(1)
      .setNumReverseStrand(1)
      .build()

    val pileups = List(p0, p1)

    val aggregator = new PileupAggregator

    val aggregated = aggregator.flatten(pileups)

    assert(aggregated.length === 2)
    assert(aggregated.filter(_.getReadBase == Base.C).head === pileups.filter(_.getReadBase == Base.C).head)
    assert(aggregated.filter(_.getReadBase == Base.A).head === pileups.filter(_.getReadBase == Base.A).head)
  }

  test("aggregating a pileup with a single base type") {
    val c0 = ADAMContig.newBuilder
      .setContigName("chr1")
      .setContigLength(1000)
      .build

    val p0 = ADAMPileup.newBuilder()
      .setContig(c0)
      .setPosition(1L)
      .setReadBase(Base.A)
      .setMapQuality(9)
      .setSangerQuality(31)
      .setCountAtPosition(1)
      .setNumSoftClipped(0)
      .setNumReverseStrand(0)
      .setReadName("read0")
      .setReadStart(0L)
      .setReadEnd(1L)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setContig(c0)
      .setPosition(1L)
      .setReadBase(Base.A)
      .setMapQuality(11)
      .setSangerQuality(29)
      .setCountAtPosition(1)
      .setNumSoftClipped(1)
      .setNumReverseStrand(1)
      .setReadName("read1")
      .setReadStart(1L)
      .setReadEnd(2L)
      .build()

    val pileups = List(p0, p1)

    val aggregator = new PileupAggregator

    val aggregated = aggregator.flatten(pileups)

    assert(aggregated.length === 1)
    assert(aggregated.head.getPosition === 1L)
    assert(aggregated.head.getReadBase === Base.A)
    assert(aggregated.head.getSangerQuality === 30)
    assert(aggregated.head.getMapQuality === 10)
    assert(aggregated.head.getCountAtPosition === 2)
    assert(aggregated.head.getNumSoftClipped === 1)
    assert(aggregated.head.getNumReverseStrand === 1)
    assert(aggregated.head.getReadName === "read0,read1")
    assert(aggregated.head.getReadStart === 0L)
    assert(aggregated.head.getReadEnd === 2L)
  }

  test("aggregating a pileup with a single base type, multiple bases at a position") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build
    val p0 = ADAMPileup.newBuilder()
      .setContig(contig)
      .setPosition(1L)
      .setReadBase(Base.A)
      .setMapQuality(8)
      .setSangerQuality(32)
      .setCountAtPosition(1)
      .setNumSoftClipped(0)
      .setNumReverseStrand(0)
      .setReadName("read0")
      .setReadStart(0L)
      .setReadEnd(1L)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setContig(contig)
      .setPosition(1L)
      .setReadBase(Base.A)
      .setMapQuality(11)
      .setSangerQuality(29)
      .setCountAtPosition(2)
      .setNumSoftClipped(2)
      .setNumReverseStrand(2)
      .setReadName("read1")
      .setReadStart(1L)
      .setReadEnd(2L)
      .build()

    val pileups = List(p0, p1)

    val aggregator = new PileupAggregator

    val aggregated = aggregator.flatten(pileups)

    assert(aggregated.length === 1)
    assert(aggregated.head.getPosition === 1L)
    assert(aggregated.head.getReadBase === Base.A)
    assert(aggregated.head.getSangerQuality === 30)
    assert(aggregated.head.getMapQuality === 10)
    assert(aggregated.head.getCountAtPosition === 3)
    assert(aggregated.head.getNumSoftClipped === 2)
    assert(aggregated.head.getNumReverseStrand === 2)
    assert(aggregated.head.getReadName === "read0,read1")
    assert(aggregated.head.getReadStart === 0L)
    assert(aggregated.head.getReadEnd === 2L)
  }

}
