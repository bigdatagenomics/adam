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
package org.bdgenomics.adam.models

import htsjdk.samtools.TextCigarCodec
import org.bdgenomics.adam.rich.RichAlignment._
import org.bdgenomics.formats.avro.Alignment
import org.scalatest.FunSuite

class MdTagSuite extends FunSuite {

  test("null md tag") {
    MdTag(null, 0L, TextCigarCodec.decode(""))
  }

  test("zero length md tag") {
    MdTag("", 0L, TextCigarCodec.decode(""))
  }

  test("md tag with non-digit initial value") {
    intercept[IllegalArgumentException] {
      MdTag("ACTG0", 0L, TextCigarCodec.decode("4M"))
    }
  }

  test("md tag invalid base") {
    intercept[IllegalArgumentException] {
      MdTag("0ACTZ", 0L, TextCigarCodec.decode("4M"))
    }
  }

  test("md tag, pure insertion") {
    // example
    // Cigar = 101I
    // MD:Z:0
    val tag = MdTag("0", 1L, TextCigarCodec.decode("101I"))
    assert(tag.start === 1L)
    assert(tag.toString === "0")
  }

  test("md tag, pure insertion, test 2") {
    // example
    // Cigar = 101I
    // MD:Z:0
    val tag = MdTag("ACATAC", "", TextCigarCodec.decode("6I"), 1L)
    assert(tag.start === 1L)
    assert(tag.toString === "0")
  }

  test("md tag pure insertion equality") {
    val tag0 = MdTag("0", 1L, TextCigarCodec.decode("6I"))
    val tag1 = MdTag("0", 1L, TextCigarCodec.decode("6I"))
    val tag2 = MdTag("0", 3L, TextCigarCodec.decode("6I"))
    assert(tag0.equals(tag1))
    assert(!tag0.equals(tag2))
  }

  test("md tag equality and hashcode") {
    val md1 = MdTag("0A0", 0L, TextCigarCodec.decode("1M"))
    val md1Dup = MdTag("0A0", 0L, TextCigarCodec.decode("1M"))
    assert(md1 == md1Dup)
    assert(md1.hashCode == md1Dup.hashCode)

    val md2 = MdTag("100C0^C20", 0L, TextCigarCodec.decode("101M1D20M"))
    val md2Dup = MdTag("100C0^C20", 0L, TextCigarCodec.decode("101M1D20M"))
    assert(md2 == md2Dup)
    assert(md2.hashCode == md2Dup.hashCode)

    val md3 = MdTag("22^A79", 0L, TextCigarCodec.decode("22M1D79M"))
    val md3Dup = MdTag("22^A79", 0L, TextCigarCodec.decode("22M1D79M"))
    assert(md3 == md3Dup)
    assert(md3.hashCode == md3Dup.hashCode)
  }

  test("valid md tags") {
    val md1 = MdTag("0A0", 0L, TextCigarCodec.decode("1M"))
    assert(md1.mismatchedBase(0) == Some('A'))
    assert(md1.countOfMismatches === 1)

    val md2 = MdTag("100", 0L, TextCigarCodec.decode("100M"))
    for (i <- 0 until 100) {
      assert(md2.isMatch(i))
    }
    assert(!md2.isMatch(-1))
    assert(md2.countOfMismatches === 0)

    val md3 = MdTag("100C2", 0L, TextCigarCodec.decode("103M"))
    for (i <- 0 until 100) {
      assert(md3.isMatch(i))
    }
    assert(md3.mismatchedBase(100) == Some('C'))
    assert(md3.countOfMismatches === 1)

    for (i <- 101 until 103) {
      assert(md3.isMatch(i))
    }

    val md4 = MdTag("100C0^C20", 0L, TextCigarCodec.decode("101M1D20M"))
    for (i <- 0 until 100) {
      assert(md4.isMatch(i))
    }
    assert(md4.mismatchedBase(100) == Some('C'))
    assert(md4.deletedBase(101) == Some('C'))
    for (i <- 102 until 122) {
      assert(md4.isMatch(i))
    }

    val deletedString = "ACGTACGTACGT"
    val md5 = MdTag("0^" + deletedString + "10", 0L, TextCigarCodec.decode("12D10M"))
    for (i <- 0 until deletedString.length) {
      assert(md5.deletedBase(i) == Some(deletedString charAt i))
    }

    assert(md5.countOfMismatches === 0)

    val md6 = MdTag("22^A79", 0L, TextCigarCodec.decode("22M1D79M"))
    for (i <- 0 until 22) {
      assert(md6.isMatch(i))
    }
    assert(md6.deletedBase(22) == Some('A'))
    for (i <- 23 until 23 + 79) {
      assert(md6.isMatch(i))
    }

    // seen in 1000G, causes errors in 9c05baa2e0e9c59cbf56e241b8ae3a7b87402fa2
    val md7 = MdTag("39r36c23", 0L, TextCigarCodec.decode("100M"))
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

    val mdy = MdTag("34Y18G46", 0L, TextCigarCodec.decode("100M"))
    assert(mdy.mismatchedBase(34) == Some('Y'))
    assert(mdy.countOfMismatches === 2)

  }

  test("get start of read with no mismatches or deletions") {
    val tag = MdTag("60", 1L, TextCigarCodec.decode("60M"))

    assert(tag.start === 1L)
  }

  test("get start of read with no mismatches, but with a deletion at the start") {
    val tag = MdTag("0^AC60", 5L, TextCigarCodec.decode("2D60M"))

    assert(tag.start === 5L)
  }

  test("get start of read with mismatches at the start") {
    val tag = MdTag("0AC60", 10L, TextCigarCodec.decode("62M"))

    assert(tag.start === 10L)
  }

  test("get end of read with no mismatches or deletions") {
    val tag = MdTag("60", 1L, TextCigarCodec.decode("60M"))

    assert(tag.end === 60L)
  }

  test("check that mdtag and rich record return same end") {
    val read = Alignment.newBuilder()
      .setSequence("A" * 60)
      .setStart(1L)
      .setEnd(60L)
      .setCigar("60M")
      .setMismatchingPositions("60")
      .setReadMapped(true)
      .build()

    assert(read.mdTag.get.end === read.getEnd)
  }

  test("get end of read with no mismatches, but a deletion at end") {
    val tag = MdTag("60^AC0", 1L, TextCigarCodec.decode("60M2D"))

    assert(tag.end === 62L)
  }

  test("CIGAR with N operator") {
    val tag = MdTag("5^A2", 1L, TextCigarCodec.decode("5M100N1D2M"))
    for (i <- 1L to 5L)
      assert(tag.isMatch(i))

    for (i <- 107L to 108L)
      assert(tag.isMatch(i))

    assert(tag.end === 108)
    assert(tag.toString === "5^A2")

  }

  test("CIGAR with multiple N operators") {
    val tag = MdTag("20", 1L, TextCigarCodec.decode("5M100N10M100N5M"))

    for (i <- 106L to 115L)
      assert(tag.isMatch(i))

    for (i <- 216L to 220L)
      assert(tag.isMatch(i))

    assert(tag.end === 220)
    assert(tag.toString === "20")
  }

  test("CIGAR with P operators") {
    val tag = MdTag("8", 0L, TextCigarCodec.decode("4=1P4="))
    assert(tag.toString === "8")
    (0 until 8).foreach(locus => assert(tag.isMatch(locus)))

  }

  test("Get correct matches for mdtag with insertion") {
    val tag = MdTag("10", 0L, TextCigarCodec.decode("5M3I5M"))
    assert(tag.end === 9)

    (0 until 9).foreach(locus => assert(tag.isMatch(locus)))

    assert(tag.toString === "10")
  }

  test("Get correct matches for mdtag with mismatches and insertion") {
    val tag = MdTag("2A7", 0L, TextCigarCodec.decode("5M3I5M"))
    assert(tag.end === 9)

    assert(tag.isMatch(0))
    assert(tag.isMatch(1))
    assert(tag.mismatches(2) === 'A')
    (3 until 9).foreach(locus => assert(tag.isMatch(locus)))

    assert(tag.toString === "2A7")
  }

  test("Get correct matches for mdtag with insertion between mismatches") {
    val tag = MdTag("2A4A2", 0L, TextCigarCodec.decode("5M3I5M"))
    assert(tag.end === 9)

    assert(tag.isMatch(0))
    assert(tag.isMatch(1))
    assert(tag.mismatches(2L) === 'A')

    assert(tag.mismatches(7L) === 'A')
    assert(tag.isMatch(8))
    assert(tag.isMatch(9))

    assert(tag.toString === "2A4A2")
  }

  test("Get correct matches for mdtag with intron between mismatches") {
    val tag = MdTag("2A4A2", 0L, TextCigarCodec.decode("5M3N5M"))
    assert(tag.end === 12)

    assert(tag.isMatch(0))
    assert(tag.isMatch(1))
    assert(tag.mismatches(2L) === 'A')
    assert(tag.isMatch(3))
    assert(tag.isMatch(4))

    assert(!tag.isMatch(5))
    assert(!tag.isMatch(6))
    assert(!tag.isMatch(7))

    assert(tag.isMatch(8))
    assert(tag.isMatch(9))

    assert(tag.mismatches(10L) === 'A')
    assert(tag.isMatch(11))
    assert(tag.isMatch(12))

    assert(tag.toString === "2A4A2")
  }

  test("Get correct matches for mdtag with intron and deletion between mismatches") {
    val tag = MdTag("2A4A0^AAA2", 0L, TextCigarCodec.decode("5M3N3M3D2M"))
    assert(tag.end === 15)

    assert(tag.isMatch(0))
    assert(tag.isMatch(1))
    assert(tag.mismatches(2L) === 'A')
    assert(tag.isMatch(3))
    assert(tag.isMatch(4))

    assert(!tag.isMatch(5))
    assert(!tag.isMatch(6))
    assert(!tag.isMatch(7))

    assert(tag.isMatch(8))
    assert(tag.isMatch(9))

    assert(tag.mismatches(10L) === 'A')

    assert(tag.deletions(11L) === 'A')
    assert(tag.deletions(12L) === 'A')
    assert(tag.deletions(13L) === 'A')

    assert(tag.toString === "2A4A0^AAA2")
  }

  test("Throw exception when number of deleted bases in mdtag disagrees with CIGAR") {
    intercept[IllegalArgumentException] {
      MdTag("2A4A0^AAA2", 0L, TextCigarCodec.decode("5M3N3M4D2M"))
    }
  }

  test("Get correct matches for mdtag with mismatch, insertion and deletion") {
    val tag = MdTag("2A3^AAA4", 0L, TextCigarCodec.decode("5M3I1M3D4M"))
    assert(tag.end === 12)

    assert(tag.isMatch(0))
    assert(tag.isMatch(1))
    assert(tag.mismatches(2L) === 'A')

    (3 to 5).foreach(locus => assert(tag.isMatch(locus)))

    assert(tag.deletedBase(6L) === Some('A'))
    assert(tag.deletedBase(7L) === Some('A'))
    assert(tag.deletedBase(8L) === Some('A'))

    (9 to 12).foreach(locus => assert(tag.isMatch(locus)))

    assert(tag.toString === "2A3^AAA4")
  }

  test("Get correct matches for mdtag with mismatches, insertion and deletion") {
    val tag = MdTag("2A3^AAA2A1", 0L, TextCigarCodec.decode("5M3I1M3D4M"))
    assert(tag.end === 12)

    assert(tag.isMatch(0))
    assert(tag.isMatch(1))
    assert(tag.mismatches(2L) === 'A')

    (3 to 5).foreach(locus => assert(tag.isMatch(locus)))

    assert(tag.deletedBase(6L) === Some('A'))
    assert(tag.deletedBase(7L) === Some('A'))
    assert(tag.deletedBase(8L) === Some('A'))

    (9 to 10).foreach(locus => assert(tag.isMatch(locus)))

    assert(tag.mismatches(11L) === 'A')

    assert(tag.toString === "2A3^AAA2A1")
  }

  test("Get correct matches for MDTag with mismatches and deletions") {

    val tag1 = MdTag("40A5^TTT54", 0L, TextCigarCodec.decode("46M3D54M"))
    assert(tag1.hasMismatches === true)
    assert(tag1.end() === 102)
    assert(tag1.isMatch(25) === true)
    assert(tag1.isMatch(39) === true)
    assert(tag1.isMatch(40) === false)
    assert(tag1.isMatch(41) === true)

    assert(tag1.mismatchedBase(40) === Some('A'))
    assert(tag1.toString === "40A5^TTT54")

    val tag2 = MdTag("40A5^TTT0G53", 0L, TextCigarCodec.decode("46M3D54M"))
    assert(tag2.hasMismatches === true)
    assert(tag2.end() === 102)
    assert(tag2.isMatch(25) === true)
    assert(tag2.isMatch(39) === true)
    assert(tag2.isMatch(40) === false)
    assert(tag2.isMatch(41) === true)

    assert(tag2.mismatchedBase(40) === Some('A'))
    assert(tag2.mismatchedBase(49) === Some('G'))
    assert(tag2.isMatch(50) === true)
    assert(tag2.toString === "40A5^TTT0G53")

    val tag3 = MdTag("2^GA5^TC6", 0L, TextCigarCodec.decode("2M2D1M2I2M4I2M2D6M"))
    (0 to 1).foreach(l => assert(tag3.isMatch(l)))
    (2 to 3).foreach(l => assert(!tag3.isMatch(l)))
    (4 to 8).foreach(l => assert(tag3.isMatch(l)))
    (9 to 10).foreach(l => assert(!tag3.isMatch(l)))
    (11 to 16).foreach(l => assert(tag3.isMatch(l)))

    assert(tag3.deletedBase(2) == Some('G'))
    assert(tag3.deletedBase(3) == Some('A'))

    assert(tag3.deletedBase(9) == Some('T'))
    assert(tag3.deletedBase(10) == Some('C'))

    assert(tag3.toString === "2^GA5^TC6")
  }

  test("Get correct matches base from MDTag and CIGAR with N") {
    val tag1 = MdTag("100", 0L, TextCigarCodec.decode("100M"))
    assert(tag1.hasMismatches === false)
    assert(tag1.end === 99)
    assert(tag1.isMatch(25) === true)

    val tag2 = MdTag("100", 0L, TextCigarCodec.decode("50M100N50M"))
    assert(tag2.hasMismatches === false)
    assert(tag2.end() === 199)
    assert(tag2.isMatch(25) === true)
    assert(tag2.isMatch(100) === false)
    assert(tag2.isMatch(175) == true)

  }

  test("get end of read with mismatches and a deletion at end") {
    val tag = MdTag("60^AC0A0C0", 1L, TextCigarCodec.decode("60M2D2M"))

    assert(tag.end === 64L)
  }

  test("get correct string out of mdtag with no mismatches") {
    val tag = MdTag("60", 1L, TextCigarCodec.decode("60M"))

    assert(tag.toString === "60")
  }

  test("get correct string out of mdtag with mismatches at start") {
    val tag = MdTag("0A0C10", 100L, TextCigarCodec.decode("12M"))

    assert(tag.toString === "0A0C10")
  }

  test("get correct string out of mdtag with deletion at end") {
    val tag = MdTag("10^GG0", 200L, TextCigarCodec.decode("10M2D"))

    assert(tag.start === 200L)
    assert(tag.end === 211L)
    assert(tag.toString === "10^GG0")
  }

  test("get correct string out of mdtag with mismatches at end") {
    val tag = MdTag("10G0G0", 200L, TextCigarCodec.decode("12M"))

    assert(tag.start === 200L)
    assert(tag.end === 211L)
    assert(tag.toString === "10G0G0")
  }

  test("get correct string out of complex mdtag") {
    val tag = MdTag("0AT0^GC0", 5123L, TextCigarCodec.decode("2M2D"))

    assert(tag.toString === "0A0T0^GC0")
  }

  test("check complex mdtag") {
    val read = Alignment.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(5)
      .setEnd(75)
      .setMismatchingPositions("29^GGGGGGGGGG10G0G0G0G0G0G0G0G0G0G11")
      .build()

    val tag = read.mdTag.get

    assert((5 until 34).forall(i => tag.isMatch(i)))
    assert((34 until 44).forall(i => tag.deletedBase(i).get == 'G'))
    assert((44 until 54).forall(i => tag.isMatch(i)))
    assert((54 until 64).forall(i => tag.mismatchedBase(i).get == 'G'))
    assert((64 until read.getEnd.toInt).forall(i => tag.isMatch(i)))
    assert(tag.getReference(read) === "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAA")
  }

  test("get gapped reference") {
    val read = Alignment.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(5)
      .setEnd(75)
      .setMismatchingPositions("29^GGGGGGGGGG10G0G0G0G0G0G0G0G0G0G11")
      .build()

    val tag = read.mdTag.get
    assert(tag.getReference(read, withGaps = true) === "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAA")
  }

  test("move a cigar alignment by two for a read") {
    val read = Alignment.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = TextCigarCodec.decode("27M10D33M")

    val newTag = MdTag.moveAlignment(read, newCigar)

    assert(newTag.toString === "27^GGGGGGGGGG10G0G0G0G0G0G0G0G0G0G13")
  }

  test("rewrite alignment to all matches") {
    val read = Alignment.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = TextCigarCodec.decode("60M")

    val newTag = MdTag.moveAlignment(read, newCigar, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 100L)

    assert(newTag.toString === "60")
    assert(newTag.start === 100L)
    assert(newTag.end === 159L)
  }

  test("rewrite alignment to two mismatches followed by all matches") {
    val read = Alignment.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = TextCigarCodec.decode("60M")

    val newTag = MdTag.moveAlignment(read, newCigar, "GGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 100L)

    assert(newTag.toString === "0G0G58")
    assert(newTag.start === 100L)
    assert(newTag.end === 159L)
  }

  test("rewrite alignment to include a deletion but otherwise all matches") {
    val read = Alignment.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = TextCigarCodec.decode("10M10D50M")

    val newTag = MdTag.moveAlignment(read, newCigar, "AAAAAAAAAAGGGGGGGGGGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 100L)

    assert(newTag.toString === "10^GGGGGGGGGG50")
    assert(newTag.start === 100L)
    assert(newTag.end === 169L)
  }

  test("rewrite alignment to include an insertion at the start of the read but otherwise all matches") {
    val read = Alignment.newBuilder()
      .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      .setReadMapped(true)
      .setCigar("29M10D31M")
      .setStart(7)
      .setMismatchingPositions("27G0G0^GGGGGGGGAA8G0G0G0G0G0G0G0G0G0G13")
      .build()

    val newCigar = TextCigarCodec.decode("10I50M")

    val newTag = MdTag.moveAlignment(read, newCigar, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 100L)

    assert(newTag.toString === "50")
    assert(newTag.start === 100L)
    assert(newTag.end === 149L)
  }

  test("create new md tag from read vs. reference, perfect match") {
    val read = "ACCATAGA"
    val reference = "ACCATAGA"
    val cigar = TextCigarCodec.decode("8M")
    val start = 0L

    val tag = MdTag(read, reference, cigar, start)

    assert(tag.toString === "8")
  }

  def testTag(read: String,
              reference: String,
              cigarStr: String,
              start: Long,
              expectedTag: String,
              expectedStart: Long,
              expectedEnd: Long): Unit = {
    val tag = MdTag(read, reference, TextCigarCodec.decode(cigarStr), start)
    assert(tag.toString == expectedTag)
    assert(tag.start == expectedStart)
    assert(tag.end == expectedEnd)
  }

  test("create new md tag from read vs. reference, perfect alignment match, 1 mismatch") {
    testTag("ACCATAGA", "ACAATAGA", "8M", 0, "2A5", 0, 7)
  }

  test("create new md tag from read vs. reference, alignment with deletion") {
    testTag("ACCATAGA", "ACCATTTAGA", "5M2D3M", 5, "5^TT3", 5, 14L)
  }

  test("create new md tag from read vs. reference, alignment with insert") {
    testTag("ACCCATAGA", "ACCATAGA", "3M1I5M", 10, "8", 10, 17)
  }

  test("handle '=' and 'X' operators") {
    testTag("ACCCAAGT", "ACCATAGA", "3=2X2=1X", 0, "3A0T2A0", 0, 7)
  }

  test("CIGAR/MD tag mismatch should cause errors") {
    intercept[MatchError] {
      MdTag("3^C71", 1L, TextCigarCodec.decode("4S1M1D71M"))
    }
  }
}
