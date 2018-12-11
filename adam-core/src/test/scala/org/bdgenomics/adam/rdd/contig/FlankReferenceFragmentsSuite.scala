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
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.scalatest.FunSuite

class FlankReferenceFragmentsSuite extends FunSuite {

  test("don't put flanks on non-adjacent fragments") {
    val testIter = Iterator((ReferenceRegion("chr1", 0L, 10L),
      NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setSequence("AAAAATTTTT")
      .setStart(0L)
      .setEnd(9L)
      .build()), (ReferenceRegion("chr1", 20L, 30L),
      NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setSequence("CCCCCGGGGG")
      .setStart(20L)
      .setEnd(29L)
      .build()))

    val fragments = FlankReferenceFragments.flank(testIter, 5).toSeq

    assert(fragments.size === 2)
    fragments.foreach(_.getSequence.length === 10)
    assert(fragments(0).getSequence === "AAAAATTTTT")
    assert(fragments(0).getStart === 0L)
    assert(fragments(0).getEnd === 9L)
    assert(fragments(1).getSequence === "CCCCCGGGGG")
    assert(fragments(1).getStart === 20L)
    assert(fragments(1).getEnd === 29L)
  }

  test("put flanks on adjacent fragments") {
    val testIter = Iterator((ReferenceRegion("chr1", 0L, 10L),
      NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setSequence("AAAAATTTTT")
      .setStart(0L)
      .setEnd(9L)
      .build()), (ReferenceRegion("chr1", 10L, 20L),
      NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setSequence("NNNNNUUUUU")
      .setStart(10L)
      .setEnd(19L)
      .build()), (ReferenceRegion("chr1", 20L, 30L),
      NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setSequence("CCCCCGGGGG")
      .setStart(20L)
      .setEnd(29L)
      .build()))

    val fragments = FlankReferenceFragments.flank(testIter, 5).toSeq

    assert(fragments.size === 3)
    assert(fragments(0).getSequence === "AAAAATTTTTNNNNN")
    assert(fragments(0).getStart === 0L)
    assert(fragments(0).getEnd === 14L)
    assert(fragments(1).getSequence === "TTTTTNNNNNUUUUUCCCCC")
    assert(fragments(1).getStart === 5L)
    assert(fragments(1).getEnd === 24L)
    assert(fragments(2).getSequence === "UUUUUCCCCCGGGGG")
    assert(fragments(2).getStart === 15L)
    assert(fragments(2).getEnd === 29L)
  }
}
