/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.util

import org.scalatest.FunSuite
import net.sf.samtools.{Cigar, CigarOperator, CigarElement, TextCigarCodec}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.rich.RichCigar
import edu.berkeley.cs.amplab.adam.rich.RichCigar._

class NormalizationUtilsSuite extends FunSuite {

  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

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
    val read: RichADAMRecord = ADAMRecord.newBuilder()
      .setReadMapped(true)
      .setSequence("AAAAACCCCCGGGGGTTTTT")
      .setStart(0)
      .setCigar("10M10D10M")
      .setMismatchingPositions("10^ATATATATAT10")
      .build()

    val new_cigar = NormalizationUtils.leftAlignIndel(read)

    println(new_cigar)
    assert(new_cigar.toString == "10M10D10M")
    // TODO: the implicit for ADAMRecord->RichADAMRecord doesn't get
    // called here for some reason.
    assert(RichADAMRecord(read).samtoolsCigar.getReadLength === new_cigar.getReadLength)
  }

  test("shift an indel left by 0 in a cigar") {
    val cigar = CIGAR_CODEC.decode("10M10D10M")

    assert(cigar === NormalizationUtils.shiftIndel(cigar, 1, 0))
  }

  test("shift an indel left by 1 in a cigar") {
    val cigar = CIGAR_CODEC.decode("10M10D10M")

    val newCigar = NormalizationUtils.shiftIndel(cigar, 1, 1)

    assert(newCigar.toString == "9M10D11M")
  }

  test("do not left align a complex read which is already left aligned") {
    val read = ADAMRecord.newBuilder()
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
