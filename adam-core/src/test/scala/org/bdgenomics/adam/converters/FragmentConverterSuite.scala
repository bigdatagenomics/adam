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

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._

class FragmentConverterSuite extends ADAMFunSuite {

  test("build a fragment collector and convert to a read") {
    val fcOpt = FragmentCollector(NucleotideContigFragment.newBuilder()
      .setContigName("ctg")
      .setSequence("ACACACAC")
      .setStart(0L)
      .setEnd(8L)
      .build())
    assert(fcOpt.isDefined)
    val (builtContig, builtFragment) = fcOpt.get

    assert(builtContig === "ctg")
    assert(builtFragment.fragments.length === 1)
    val (fragmentRegion, fragmentString) = builtFragment.fragments.head
    assert(fragmentRegion === ReferenceRegion("ctg", 0L, 8L))
    assert(fragmentString === "ACACACAC")

    val convertedReads = FragmentConverter.convertFragment((builtContig, builtFragment))
    assert(convertedReads.size === 1)
    val convertedRead = convertedReads.head

    assert(convertedRead.getSequence === "ACACACAC")
    assert(convertedRead.getReferenceName === "ctg")
    assert(convertedRead.getStart === 0L)
    assert(convertedRead.getEnd === 8L)
  }

  test("if a fragment isn't associated with a contig, don't get a fragment collector") {
    val fcOpt = FragmentCollector(NucleotideContigFragment.newBuilder().build())
    assert(fcOpt.isEmpty)
  }

  sparkTest("convert an rdd of discontinuous fragments, all from the same contig") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContigName("ctg")
      .setSequence("ACACACAC")
      .setStart(0L)
      .setEnd(8L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContigName("ctg")
      .setSequence("AATTCCGGCCTTAA")
      .setStart(14L)
      .setEnd(28L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    assert(reads.length === 2)
    val firstRead = reads.filter(_.getStart == 0L).head
    val secondRead = reads.filter(_.getStart != 0L).head

    assert(firstRead.getSequence === "ACACACAC")
    assert(firstRead.getReferenceName === "ctg")
    assert(firstRead.getStart === 0L)
    assert(firstRead.getEnd === 8L)
    assert(secondRead.getSequence === "AATTCCGGCCTTAA")
    assert(secondRead.getReferenceName === "ctg")
    assert(secondRead.getStart === 14L)
    assert(secondRead.getEnd === 28L)
  }

  sparkTest("convert an rdd of contiguous fragments, all from the same contig") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContigName("ctg")
      .setSequence("ACACACAC")
      .setStart(0L)
      .setEnd(8L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContigName("ctg")
      .setSequence("TGTGTG")
      .setStart(8L)
      .setEnd(14L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContigName("ctg")
      .setSequence("AATTCCGGCCTTAA")
      .setStart(14L)
      .setEnd(28L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    assert(reads.length === 1)
    val read = reads(0)
    assert(read.getSequence === "ACACACACTGTGTGAATTCCGGCCTTAA")
    assert(read.getReferenceName === "ctg")
    assert(read.getStart === 0L)
    assert(read.getEnd === 28L)
  }

  sparkTest("convert an rdd of varied fragments from multiple contigs") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContigName("ctg1")
      .setSequence("ACACACAC")
      .setStart(0L)
      .setEnd(8L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContigName("ctg1")
      .setSequence("TGTGTG")
      .setStart(8L)
      .setEnd(14L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContigName("ctg1")
      .setSequence("AATTCCGGCCTTAA")
      .setStart(14L)
      .setEnd(28L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContigName("ctg2")
      .setSequence("ACACACAC")
      .setStart(0L)
      .setEnd(8L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContigName("ctg2")
      .setSequence("AATTCCGGCCTTAA")
      .setStart(14L)
      .setEnd(28L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContigName("ctg3")
      .setSequence("AATTCCGGCCTTAA")
      .setStart(14L)
      .setEnd(28L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    assert(reads.length === 4)

    val ctg1Reads = reads.filter(_.getReferenceName == "ctg1")
    assert(ctg1Reads.length === 1)

    val ctg1Read = ctg1Reads.head
    assert(ctg1Read.getSequence === "ACACACACTGTGTGAATTCCGGCCTTAA")
    assert(ctg1Read.getReferenceName === "ctg1")
    assert(ctg1Read.getStart === 0L)
    assert(ctg1Read.getEnd === 28L)

    val ctg2Reads = reads.filter(_.getReferenceName == "ctg2")
    assert(ctg2Reads.length === 2)

    val firstCtg2Read = ctg2Reads.filter(_.getStart == 0L).head
    val secondCtg2Read = ctg2Reads.filter(_.getStart != 0L).head

    assert(firstCtg2Read.getSequence === "ACACACAC")
    assert(firstCtg2Read.getReferenceName === "ctg2")
    assert(firstCtg2Read.getStart === 0L)
    assert(firstCtg2Read.getEnd === 8L)
    assert(secondCtg2Read.getSequence === "AATTCCGGCCTTAA")
    assert(secondCtg2Read.getReferenceName === "ctg2")
    assert(secondCtg2Read.getStart === 14L)
    assert(secondCtg2Read.getEnd === 28L)

    val ctg3Reads = reads.filter(_.getReferenceName == "ctg3")
    assert(ctg3Reads.length === 1)

    val ctg3Read = ctg3Reads.head
    assert(ctg3Read.getSequence === "AATTCCGGCCTTAA")
    assert(ctg3Read.getReferenceName === "ctg3")
    assert(ctg3Read.getStart === 14L)
    assert(ctg3Read.getEnd === 28L)
  }
}
