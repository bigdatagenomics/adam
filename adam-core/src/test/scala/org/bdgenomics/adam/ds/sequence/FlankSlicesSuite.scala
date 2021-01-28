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
package org.bdgenomics.adam.ds.sequence

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Slice
import org.scalatest.FunSuite

class FlankSlicesSuite extends FunSuite {

  test("don't put flanks on non-adjacent slices") {
    val testIter = Iterator((ReferenceRegion("chr1", 0L, 10L),
      Slice.newBuilder()
      .setName("chr1")
      .setSequence("AAAAATTTTT")
      .setStart(0L)
      .setEnd(9L)
      .build()), (ReferenceRegion("chr1", 20L, 30L),
      Slice.newBuilder()
      .setName("chr1")
      .setSequence("CCCCCGGGGG")
      .setStart(20L)
      .setEnd(29L)
      .build()))

    val slices = FlankSlices.flank(testIter, 5).toSeq

    assert(slices.size === 2)
    slices.foreach(_.getSequence.length === 10)
    assert(slices(0).getSequence === "AAAAATTTTT")
    assert(slices(0).getStart === 0L)
    assert(slices(0).getEnd === 9L)
    assert(slices(1).getSequence === "CCCCCGGGGG")
    assert(slices(1).getStart === 20L)
    assert(slices(1).getEnd === 29L)
  }

  test("put flanks on adjacent slices") {
    val testIter = Iterator((ReferenceRegion("chr1", 0L, 10L),
      Slice.newBuilder()
      .setName("chr1")
      .setSequence("AAAAATTTTT")
      .setStart(0L)
      .setEnd(9L)
      .build()), (ReferenceRegion("chr1", 10L, 20L),
      Slice.newBuilder()
      .setName("chr1")
      .setSequence("NNNNNUUUUU")
      .setStart(10L)
      .setEnd(19L)
      .build()), (ReferenceRegion("chr1", 20L, 30L),
      Slice.newBuilder()
      .setName("chr1")
      .setSequence("CCCCCGGGGG")
      .setStart(20L)
      .setEnd(29L)
      .build()))

    val slices = FlankSlices.flank(testIter, 5).toSeq

    assert(slices.size === 3)
    assert(slices(0).getSequence === "AAAAATTTTTNNNNN")
    assert(slices(0).getStart === 0L)
    assert(slices(0).getEnd === 14L)
    assert(slices(1).getSequence === "TTTTTNNNNNUUUUUCCCCC")
    assert(slices(1).getStart === 5L)
    assert(slices(1).getEnd === 24L)
    assert(slices(2).getSequence === "UUUUUCCCCCGGGGG")
    assert(slices(2).getStart === 15L)
    assert(slices(2).getEnd === 29L)
  }
}
