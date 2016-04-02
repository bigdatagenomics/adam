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
    val (builtContig, builtFragment) = FragmentCollector(NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build())

    assert(builtContig.getContigName === "ctg")
    assert(builtFragment.fragments.length === 1)
    val (fragmentRegion, fragmentString) = builtFragment.fragments.head
    assert(fragmentRegion === ReferenceRegion("ctg", 0L, 8L))
    assert(fragmentString === "ACACACAC")

    val convertedReads = FragmentConverter.convertFragment((builtContig, builtFragment))
    assert(convertedReads.size === 1)
    val convertedRead = convertedReads.head

    assert(convertedRead.getSequence === "ACACACAC")
    assert(convertedRead.getContigName === "ctg")
    assert(convertedRead.getStart === 0L)
    assert(convertedRead.getEnd === 8L)
  }

  sparkTest("convert an rdd of discontinuous fragments, all from the same contig") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    assert(reads.length === 2)
    val firstRead = reads.filter(_.getStart == 0L).head
    val secondRead = reads.filter(_.getStart != 0L).head

    assert(firstRead.getSequence === "ACACACAC")
    assert(firstRead.getContigName === "ctg")
    assert(firstRead.getStart === 0L)
    assert(firstRead.getEnd === 8L)
    assert(secondRead.getSequence === "AATTCCGGCCTTAA")
    assert(secondRead.getContigName === "ctg")
    assert(secondRead.getStart === 14L)
    assert(secondRead.getEnd === 28L)
  }

  sparkTest("convert an rdd of contiguous fragments, all from the same contig") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("TGTGTG")
      .setFragmentStartPosition(8L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    assert(reads.length === 1)
    val read = reads(0)
    assert(read.getSequence === "ACACACACTGTGTGAATTCCGGCCTTAA")
    assert(read.getContigName === "ctg")
    assert(read.getStart === 0L)
    assert(read.getEnd === 28L)
  }

  sparkTest("convert an rdd of varied fragments from multiple contigs") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg1").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg1").build())
      .setFragmentSequence("TGTGTG")
      .setFragmentStartPosition(8L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg1").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg2").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg2").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg3").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    assert(reads.length === 4)

    val ctg1Reads = reads.filter(_.getContigName == "ctg1")
    assert(ctg1Reads.length === 1)

    val ctg1Read = ctg1Reads.head
    assert(ctg1Read.getSequence === "ACACACACTGTGTGAATTCCGGCCTTAA")
    assert(ctg1Read.getContigName === "ctg1")
    assert(ctg1Read.getStart === 0L)
    assert(ctg1Read.getEnd === 28L)

    val ctg2Reads = reads.filter(_.getContigName == "ctg2")
    assert(ctg2Reads.length === 2)

    val firstCtg2Read = ctg2Reads.filter(_.getStart == 0L).head
    val secondCtg2Read = ctg2Reads.filter(_.getStart != 0L).head

    assert(firstCtg2Read.getSequence === "ACACACAC")
    assert(firstCtg2Read.getContigName === "ctg2")
    assert(firstCtg2Read.getStart === 0L)
    assert(firstCtg2Read.getEnd === 8L)
    assert(secondCtg2Read.getSequence === "AATTCCGGCCTTAA")
    assert(secondCtg2Read.getContigName === "ctg2")
    assert(secondCtg2Read.getStart === 14L)
    assert(secondCtg2Read.getEnd === 28L)

    val ctg3Reads = reads.filter(_.getContigName == "ctg3")
    assert(ctg3Reads.length === 1)

    val ctg3Read = ctg3Reads.head
    assert(ctg3Read.getSequence === "AATTCCGGCCTTAA")
    assert(ctg3Read.getContigName === "ctg3")
    assert(ctg3Read.getStart === 14L)
    assert(ctg3Read.getEnd === 28L)
  }
}
