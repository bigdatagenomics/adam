/*
 * Copyright (c) 2013. Regents of the University of California
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
import net.sf.samtools.{Cigar, TextCigarCodec}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._

class MdTagSuite extends FunSuite {

  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

  test("null md tag") {
    MdTag(null, 0L)
  }

  test("zero length md tag") {
    MdTag("", 0L)
  }

  test("md tag with non-digit initial value") {
    intercept[IllegalArgumentException] {
      MdTag("ACTG0", 0L)
    }
  }

  test("md tag invalid base") {
    intercept[IllegalArgumentException] {
      MdTag("0ACTZ", 0L)
    }
  }

  test("md tag with no digit at end") {
    intercept[IllegalArgumentException] {
      MdTag("0ACTG", 0L)
    }
  }

  test("valid md tags") {
    val md1 = MdTag("0A0", 0L)
    assert(md1.mismatchedBase(0) == Some('A'))

    val md2 = MdTag("100", 0L)
    for (i <- 0 until 100) {
      assert(md2.isMatch(i))
    }
    assert(!md2.isMatch(-1))

    val md3 = MdTag("100C2", 0L)
    for (i <- 0 until 100) {
      assert(md3.isMatch(i))
    }
    assert(md3.mismatchedBase(100) == Some('C'))
    for (i <- 101 until 103) {
      assert(md3.isMatch(i))
    }

    val md4 = MdTag("100C0^C20", 0L)
    for (i <- 0 until 100) {
      assert(md4.isMatch(i))
    }
    assert(md4.mismatchedBase(100) == Some('C'))
    assert(md4.deletedBase(101) == Some('C'))
    for (i <- 102 until 122) {
      assert(md4.isMatch(i))
    }

    val deletedString = "ACGTACGTACGT"
    val md5 = MdTag("0^" + deletedString + "10", 0L)
    for (i <- 0 until deletedString.length) {
      assert(md5.deletedBase(i) == Some(deletedString charAt i))
    }

    val md6 = MdTag("22^A79", 0L)
    for (i <- 0 until 22) {
      assert(md6.isMatch(i))
    }
    assert(md6.deletedBase(22) == Some('A'))
    for (i <- 23 until 23 + 79) {
      assert(md6.isMatch(i))
    }

    // seen in 1000G, causes errors in 9c05baa2e0e9c59cbf56e241b8ae3a7b87402fa2
    val md7 = MdTag("39r36c23", 0L)
    for (i <- 0 until 39) {
      assert(md7.isMatch(i))
    }
    assert(md7.mismatchedBase(39) == Some('R'))
    for (i <- 40 until 40 + 36) {
      assert(md7.isMatch(i))
    }
    assert(md7.mismatchedBase(40 + 36) == Some('C'))
    for (i <- 40 + 37 until 40 + 37 + 23) {
      assert(md7.isMatch(i))
    }

    val mdy = MdTag("34Y18G46", 0L)
    assert(mdy.mismatchedBase(34) == Some('Y'))

  }

  test("get start of read with no mismatches or deletions") {
    val tag = MdTag("60", 1L)

    assert(tag.start() === 1L)
  }

  test("get start of read with no mismatches, but with a deletion at the start") {
    val tag = MdTag("0^AC60", 5L)

    assert(tag.start() === 5L)
  }

  test("get start of read with mismatches at the start") {
    val tag = MdTag("0AC60", 10L)

    assert(tag.start() === 10L)
  }

  test("get end of read with no mismatches or deletions") {
    val tag = MdTag("60", 1L)

    assert(tag.end() === 60L)
  }

  test("check that mdtag and rich record return same end") {
    val read = ADAMRecord.newBuilder()
      .setSequence("A" * 60)
      .setStart(1L)
      .setCigar("60M")
      .setMismatchingPositions("60")
      .setReadMapped(true)
      .build()

    // rich adam record returns exclusive end?
    assert(read.mdTag.get.end() === (read.end.get - 1))
  }

  test("get end of read with no mismatches, but a deletion at end") {
    val tag = MdTag("60^AC0", 1L)

    assert(tag.end() === 62L)
  }

  test("get end of read with mismatches and a deletion at end") {
    val tag = MdTag("60^AC0A0C0", 1L)

    assert(tag.end() === 64L)
  }

  test("get correct string out of mdtag with no mismatches") {
    val tag = MdTag("60", 1L)

    assert(tag.toString === "60")
  }

  test("get correct string out of mdtag with mismatches at start") {
    val tag = MdTag("0A0C10", 100L)
    
    assert(tag.toString === "0A0C10")
  }

  test("get correct string out of mdtag with deletion at end") {
    val tag = MdTag("10^GG0", 200L)

    assert(tag.start() === 200L)
    assert(tag.end() === 211L)
    assert(tag.toString === "10^GG0")
  }

  test("get correct string out of mdtag with mismatches at end") {
    val tag = MdTag("10G0G0", 200L)

    assert(tag.start() === 200L)
    assert(tag.end() === 211L)
    assert(tag.toString === "10G0G0")
  }

  test("get correct string out of complex mdtag") {
    val tag = MdTag("0AT0^GC0", 5123L)

    assert(tag.toString === "0A0T0^GC0")
  }

  test("check complex mdtag") {
    val read = ADAMRecord.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(5)
      .setMismatchingPositions("29^GGGGGGGGGG10G0G0G0G0G0G0G0G0G0G11")
      .build()

    val tag = read.mdTag.get

    assert((5 until 34).forall(i => tag.isMatch(i)))
    assert((34 until 44).forall(i => tag.deletedBase(i).get == 'G'))
    assert((44 until 54).forall(i => tag.isMatch(i)))
    assert((54 until 64).forall(i => tag.mismatchedBase(i).get == 'G'))
    assert((64 until read.end.get.toInt).forall(i => tag.isMatch(i)))
    assert(tag.getReference(read) === "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAA")
  }

  test("move a cigar alignment by two for a read") {
    val read = ADAMRecord.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = CIGAR_CODEC.decode("27M10D33M")

    val newTag = MdTag.moveAlignment(read, newCigar)

    assert(newTag.toString === "27^GGGGGGGGGG10G0G0G0G0G0G0G0G0G0G13")
  }

  test("rewrite alignment to all matches") {
    val read = ADAMRecord.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = CIGAR_CODEC.decode("60M")

    val newTag = MdTag.moveAlignment(read, newCigar, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 100L)

    assert(newTag.toString === "60")
    assert(newTag.start() === 100L)
    assert(newTag.end() === 159L)
  }

  test("rewrite alignment to two mismatches followed by all matches") {
    val read = ADAMRecord.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = CIGAR_CODEC.decode("60M")

    val newTag = MdTag.moveAlignment(read, newCigar, "GGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 100L)

    assert(newTag.toString === "0G0G58")
    assert(newTag.start() === 100L)
    assert(newTag.end() === 159L)
  }

  test("rewrite alignment to include a deletion but otherwise all matches") {
    val read = ADAMRecord.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = CIGAR_CODEC.decode("10M10D50M")

    val newTag = MdTag.moveAlignment(read, newCigar, "AAAAAAAAAAGGGGGGGGGGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 100L)

    assert(newTag.toString === "10^GGGGGGGGGG50")
    assert(newTag.start() === 100L)
    assert(newTag.end() === 169L)
  }

  test("rewrite alignment to include an insertion at the start of the read but otherwise all matches") {
    val read = ADAMRecord.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()
      
    val newCigar = CIGAR_CODEC.decode("10I50M")

    val newTag = MdTag.moveAlignment(read, newCigar, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 100L)

    assert(newTag.toString === "50")
    assert(newTag.start() === 100L)
    assert(newTag.end() === 149L)
  }

  test("create new md tag from read vs. reference, perfect match") {
    val read = "ACCATAGA"
    val reference = "ACCATAGA"
    val cigar = CIGAR_CODEC.decode("8M")
    val start = 0L

    val tag = MdTag(read, reference, cigar, start)

    assert(tag.toString === "8")
  }

  test("create new md tag from read vs. reference, perfect alignment match, 1 mismatch") {
    val read = "ACCATAGA"
    val reference = "ACAATAGA"
    val cigar = CIGAR_CODEC.decode("8M")
    val start = 0L

    val tag = MdTag(read, reference, cigar, start)

    assert(tag.toString === "2A5")
    assert(tag.start === 0L)
    assert(tag.end === 7L)
  }

  test("create new md tag from read vs. reference, alignment with deletion") {
    val read = "ACCATAGA"
    val reference = "ACCATTTAGA"
    val cigar = CIGAR_CODEC.decode("5M2D3M")
    val start = 5L

    val tag = MdTag(read, reference, cigar, start)

    assert(tag.toString === "5^TT3")
    assert(tag.start === 5L)
    assert(tag.end === 14L)
  }

  test("create new md tag from read vs. reference, alignment with insert") {
    val read = "ACCCATAGA"
    val reference = "ACCATAGA"
    val cigar = CIGAR_CODEC.decode("3M1I5M")
    val start = 10L

    val tag = MdTag(read, reference, cigar, start)

    assert(tag.toString === "8")
    assert(tag.start === 10L)
    assert(tag.end === 17L)
  }

}
