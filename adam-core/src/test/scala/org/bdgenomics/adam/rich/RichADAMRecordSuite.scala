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
package org.bdgenomics.adam.rich

import org.scalatest.FunSuite
import org.bdgenomics.formats.avro.{ ADAMContig, ADAMRecord }
import RichADAMRecord._
import org.bdgenomics.adam.models.{ ReferencePosition, TagType, Attribute }

class RichADAMRecordSuite extends FunSuite {

  test("referenceLengthFromCigar") {
    assert(referenceLengthFromCigar("3M") === 3)
    assert(referenceLengthFromCigar("30M") === 30)
    assert(referenceLengthFromCigar("10Y") === 0) // should abort when it hits an illegal operator
    assert(referenceLengthFromCigar("10M1Y") === 10) // same
    assert(referenceLengthFromCigar("10M1I10M") === 20)
    assert(referenceLengthFromCigar("10M1D10M") === 21)
    assert(referenceLengthFromCigar("1S10M1S") === 10)
  }

  test("Unclipped Start") {
    val recordWithoutClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(42).build()
    val recordWithClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("2S8M").setStart(42).build()
    val recordWithHardClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("3H2S5M4S").setStart(42).build()
    assert(recordWithoutClipping.unclippedStart == Some(42L))
    assert(recordWithClipping.unclippedStart == Some(40L))
    assert(recordWithHardClipping.unclippedStart == Some(37L))
  }

  test("Unclipped End") {
    val unmappedRead = ADAMRecord.newBuilder().setReadMapped(false).setStart(0).setCigar("10M").build()
    val recordWithoutClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(10).build()
    val recordWithClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("8M2S").setStart(10).build()
    val recordWithHardClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("6M2S2H").setStart(10).build()
    assert(unmappedRead.unclippedEnd == None)
    assert(recordWithoutClipping.unclippedEnd == Some(20L))
    assert(recordWithClipping.unclippedEnd == Some(20L))
    assert(recordWithHardClipping.unclippedEnd == Some(20L))
  }

  test("Reference End Position") {
    val unmappedRead = ADAMRecord.newBuilder().setReadMapped(false).setStart(0).setCigar("10M").build()
    val recordWithoutClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(10).build()
    val recordWithClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("8M2S").setStart(10).build()
    val recordWithHardClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("6M2S2H").setStart(10).build()
    assert(unmappedRead.end == None)
    assert(recordWithoutClipping.end == Some(20L))
    assert(recordWithClipping.end == Some(18L))
    assert(recordWithHardClipping.end == Some(16L))

  }

  test("Illumina Optics") {
    val nonIlluminaRecord = ADAMRecord.newBuilder().setReadName("THISISNOTILLUMINA").build()
    assert(nonIlluminaRecord.illuminaOptics == None)
    val illuminaRecord = ADAMRecord.newBuilder().setReadName("613F0AAXX100423:4:86:16767:3088").build()
    illuminaRecord.illuminaOptics match {
      case Some(optics) =>
        assert(optics.tile == 86)
        assert(optics.x == 16767)
        assert(optics.y == 3088)
      case None => assert(Some("Failed to parse valid Illumina read name"))
    }
  }

  test("Cigar Clipping Sequence") {
    val contig = ADAMContig.newBuilder.setContigName("chr1").build
    val softClippedRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(100).setCigar("10S90M").setContig(contig).build()
    assert(softClippedRead.referencePositions(0).map(_.pos) == Some(90L))

  }

  test("tags contains optional fields") {
    val contig = ADAMContig.newBuilder.setContigName("chr1").build
    val rec = ADAMRecord.newBuilder().setAttributes("XX:i:3\tYY:Z:foo").setContig(contig).build()
    assert(rec.tags.size === 2)
    assert(rec.tags(0) === Attribute("XX", TagType.Integer, 3))
    assert(rec.tags(1) === Attribute("YY", TagType.String, "foo"))
  }

  test("Reference Positions") {

    val contig = ADAMContig.newBuilder.setContigName("chr1").build

    val hardClippedRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("90M10H").setContig(contig).build()
    assert(hardClippedRead.referencePositions.length == 90)
    assert(hardClippedRead.referencePositions(0).map(_.pos) == Some(1000L))

    val softClippedRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10S90M").setContig(contig).build()
    assert(softClippedRead.referencePositions.length == 100)
    assert(softClippedRead.referencePositions(0).map(_.pos) == Some(990L))
    assert(softClippedRead.referencePositions(10).map(_.pos) == Some(1000L))

    val doubleMatchNonsenseRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M10M").setContig(contig).build()
    Range(0, 20).foreach(i => assert(doubleMatchNonsenseRead.referencePositions(i).map(_.pos) == Some(1000 + i)))

    val deletionRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("5M5D10M").setContig(contig).build()
    assert(deletionRead.referencePositions.length == 15)
    assert(deletionRead.referencePositions(0).map(_.pos) == Some(1000L))
    assert(deletionRead.referencePositions(5).map(_.pos) == Some(1010L))

    val insertionRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M2I10M").setContig(contig).build()
    assert(insertionRead.referencePositions.length == 22)
    assert(insertionRead.referencePositions(0).map(_.pos) == Some(1000L))
    assert(insertionRead.referencePositions(10).map(_.pos) == None)
    assert(insertionRead.referencePositions(12).map(_.pos) == Some(1010L))

    val indelRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M3D10M2I").setContig(contig).build()
    assert(indelRead.referencePositions.length == 22)
    assert(indelRead.referencePositions(0).map(_.pos) == Some(1000L))
    assert(indelRead.referencePositions(10).map(_.pos) == Some(1013L))
    assert(indelRead.referencePositions(20).map(_.pos) == None)

    val hg00096read = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("1S28M1D32M1I15M1D23M").setContig(contig).build()
    assert(hg00096read.referencePositions.length == 100)
    assert(hg00096read.referencePositions(0).map(_.pos) == Some(999L))
    assert(hg00096read.referencePositions(1).map(_.pos) == Some(1000L))
    assert(hg00096read.referencePositions(29).map(_.pos) == Some(1029L))
    assert(hg00096read.referencePositions(61).map(_.pos) == None)
    assert(hg00096read.referencePositions(62).map(_.pos) == Some(1061L))
    assert(hg00096read.referencePositions(78).map(_.pos) == Some(1078L))
    assert(hg00096read.referencePositions(99).map(_.pos) == Some(1099L))

  }

  test("read overlap unmapped read") {
    val unmappedRead = ADAMRecord.newBuilder().setReadMapped(false).setStart(0).setCigar("10M").build()

    val overlaps = unmappedRead.overlapsReferencePosition(ReferencePosition("chr1", 10))
    assert(overlaps == None)
  }

  test("read overlap reference position") {

    val contig = ADAMContig.newBuilder.setContigName("chr1").build
    val record = RichADAMRecord(ADAMRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(10).setContig(contig).build())

    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 10)) == Some(true))

    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 14)) == Some(true))
    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 19)) == Some(true))
    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 20)) == Some(false))
  }

  test("read overlap same position different contig") {

    val contig = ADAMContig.newBuilder.setContigName("chr1").build
    val record = RichADAMRecord(ADAMRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(10).setContig(contig).build())

    assert(record.overlapsReferencePosition(ReferencePosition("chr2", 10)) == Some(false))
  }

}
