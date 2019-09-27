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
package org.bdgenomics.adam.algorithms.consensus

import htsjdk.samtools.TextCigarCodec
import org.bdgenomics.adam.rich.RichAlignment._
import org.bdgenomics.adam.rich.{ RichAlignment, RichCigar }
import org.bdgenomics.formats.avro.Alignment
import org.scalatest.FunSuite

class NormalizationUtilsSuite extends FunSuite {

  test("cannot move an indel left if there are no bases to it's left") {
    assert(NormalizationUtils.numberOfPositionsToShiftIndel("ATC", "") === 0)
  }

  test("move a simple indel to farthest position left until bases run out") {
    assert(NormalizationUtils.numberOfPositionsToShiftIndel("AAA", "AA") === 2)
  }

  test("move a simple indel to farthest position left, past length of indel") {
    assert(NormalizationUtils.numberOfPositionsToShiftIndel("AAA", "TGAAAA") === 4)
  }

  test("cannot move a left normalized indel in a short tandem repeat") {
    assert(NormalizationUtils.numberOfPositionsToShiftIndel("ATAT", "TGTCC") === 0)
  }

  test("move an indel in a short tandem repeat") {
    assert(NormalizationUtils.numberOfPositionsToShiftIndel("ATAT", "TGTCCATATATAT") === 8)
  }

  test("move an indel in a short tandem repeat of more than 2 bases, where shift is not an integer multiple of repeated sequence length") {
    assert(NormalizationUtils.numberOfPositionsToShiftIndel("ATGATG", "TGTCCTGATG") === 5)
  }

  test("moving a simple read with single deletion that cannot shift") {
    val read: RichAlignment = Alignment.newBuilder()
      .setReadMapped(true)
      .setSequence("AAAAACCCCCGGGGGTTTTT")
      .setStart(0)
      .setCigar("10M10D10M")
      .setMismatchingPositions("10^ATATATATAT10")
      .build()

    val new_cigar = NormalizationUtils.leftAlignIndel(read)

    assert(new_cigar.toString == "10M10D10M")
    // TODO: the implicit for Read->RichADAMRecord doesn't get
    // called here for some reason.
    assert(RichAlignment(read).samtoolsCigar.getReadLength === new_cigar.getReadLength)
  }

  test("shift an indel left by 0 in a cigar") {
    val cigar = RichCigar(TextCigarCodec.decode("10M10D10M"))

    assert(cigar.cigar === NormalizationUtils.shiftIndel(cigar, 1, 0))
  }

  test("shift an indel left by 1 in a cigar") {
    val cigar = RichCigar(TextCigarCodec.decode("10M10D10M"))

    val newCigar = NormalizationUtils.shiftIndel(cigar, 1, 1)

    assert(newCigar.toString == "9M10D11M")
  }

  test("do not left align a complex read which is already left aligned") {
    val read = Alignment.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(5)
      .setMismatchingPositions("29^GGGGGGGGGG10G0G0G0G0G0G0G0G0G0G11")
      .build()

    val cigar = NormalizationUtils.leftAlignIndel(read)

    assert(cigar.toString === "29M10D31M")
  }
}
