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
package org.bdgenomics.adam.rdd.contig

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, NucleotideContigFragment }
import org.scalatest.FunSuite

class FlankReferenceFragmentsSuite extends FunSuite {

  test("don't put flanks on non-adjacent fragments") {
    val testIter = Iterator((ReferenceRegion("chr1", 0L, 10L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("AAAAATTTTT")
      .setFragmentStartPosition(0L)
      .setFragmentEndPosition(9L)
      .build()), (ReferenceRegion("chr1", 20L, 30L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("CCCCCGGGGG")
      .setFragmentStartPosition(20L)
      .setFragmentEndPosition(29L)
      .build()))

    val fragments = FlankReferenceFragments.flank(testIter, 5).toSeq

    assert(fragments.size === 2)
    fragments.foreach(_.getFragmentSequence.length === 10)
    assert(fragments(0).getFragmentSequence === "AAAAATTTTT")
    assert(fragments(0).getFragmentStartPosition === 0L)
    assert(fragments(0).getFragmentEndPosition === 9L)
    assert(fragments(1).getFragmentSequence === "CCCCCGGGGG")
    assert(fragments(1).getFragmentStartPosition === 20L)
    assert(fragments(1).getFragmentEndPosition === 29L)
  }

  test("put flanks on adjacent fragments") {
    val testIter = Iterator((ReferenceRegion("chr1", 0L, 10L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("AAAAATTTTT")
      .setFragmentStartPosition(0L)
      .setFragmentEndPosition(9L)
      .build()), (ReferenceRegion("chr1", 10L, 20L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("NNNNNUUUUU")
      .setFragmentStartPosition(10L)
      .setFragmentEndPosition(19L)
      .build()), (ReferenceRegion("chr1", 20L, 30L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("CCCCCGGGGG")
      .setFragmentStartPosition(20L)
      .setFragmentEndPosition(29L)
      .build()))

    val fragments = FlankReferenceFragments.flank(testIter, 5).toSeq

    assert(fragments.size === 3)
    assert(fragments(0).getFragmentSequence === "AAAAATTTTTNNNNN")
    assert(fragments(0).getFragmentStartPosition === 0L)
    assert(fragments(0).getFragmentEndPosition === 14L)
    assert(fragments(1).getFragmentSequence === "TTTTTNNNNNUUUUUCCCCC")
    assert(fragments(1).getFragmentStartPosition === 5L)
    assert(fragments(1).getFragmentEndPosition === 24L)
    assert(fragments(2).getFragmentSequence === "UUUUUCCCCCGGGGG")
    assert(fragments(2).getFragmentStartPosition === 15L)
    assert(fragments(2).getFragmentEndPosition === 29L)
  }
}
