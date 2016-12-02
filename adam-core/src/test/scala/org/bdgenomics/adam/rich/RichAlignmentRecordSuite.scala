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

import org.bdgenomics.adam.models.{ ReferencePosition, TagType, Attribute }
import org.bdgenomics.adam.rich.RichAlignmentRecord._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
import org.scalatest.FunSuite

class RichAlignmentRecordSuite extends FunSuite {

  test("Unclipped Start") {
    val recordWithoutClipping = AlignmentRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(42L).setEnd(52L).build()
    val recordWithClipping = AlignmentRecord.newBuilder().setReadMapped(true).setCigar("2S8M").setStart(42L).setEnd(50L).build()
    val recordWithHardClipping = AlignmentRecord.newBuilder().setReadMapped(true).setCigar("3H2S5M4S").setStart(42L).setEnd(47L).build()
    assert(recordWithoutClipping.unclippedStart == 42L)
    assert(recordWithClipping.unclippedStart == 40L)
    assert(recordWithHardClipping.unclippedStart == 37L)
  }

  test("Unclipped End") {
    val unmappedRead = AlignmentRecord.newBuilder().setReadMapped(false).setStart(0L).setCigar("10M").setEnd(10L).build()
    val recordWithoutClipping = AlignmentRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(10L).setEnd(20L).build()
    val recordWithClipping = AlignmentRecord.newBuilder().setReadMapped(true).setCigar("8M2S").setStart(10L).setEnd(18L).build()
    val recordWithHardClipping = AlignmentRecord.newBuilder().setReadMapped(true).setCigar("6M2S2H").setStart(10L).setEnd(16L).build()
    assert(recordWithoutClipping.unclippedEnd == 20L)
    assert(recordWithClipping.unclippedEnd == 20L)
    assert(recordWithHardClipping.unclippedEnd == 20L)
  }

  test("Cigar Clipping Sequence") {
    val contig = Contig.newBuilder.setContigName("chr1").build
    val softClippedRead = AlignmentRecord.newBuilder().setReadMapped(true).setStart(100).setCigar("10S90M").setContigName(contig.getContigName).build()
    assert(softClippedRead.referencePositions(0).map(_.pos) == Some(90L))

  }

  test("tags contains optional fields") {
    val contig = Contig.newBuilder.setContigName("chr1").build
    val rec = AlignmentRecord.newBuilder().setAttributes("XX:i:3\tYY:Z:foo").setContigName(contig.getContigName).build()
    assert(rec.tags.size === 2)
    assert(rec.tags(0) === Attribute("XX", TagType.Integer, 3))
    assert(rec.tags(1) === Attribute("YY", TagType.String, "foo"))
  }

  test("Reference Positions") {

    val contig = Contig.newBuilder.setContigName("chr1").build

    val hardClippedRead = AlignmentRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("90M10H").setContigName(contig.getContigName).build()
    assert(hardClippedRead.referencePositions.length == 90)
    assert(hardClippedRead.referencePositions(0).map(_.pos) == Some(1000L))

    val softClippedRead = AlignmentRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10S90M").setContigName(contig.getContigName).build()
    assert(softClippedRead.referencePositions.length == 100)
    assert(softClippedRead.referencePositions(0).map(_.pos) == Some(990L))
    assert(softClippedRead.referencePositions(10).map(_.pos) == Some(1000L))

    val doubleMatchNonsenseRead = AlignmentRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M10M").setContigName(contig.getContigName).build()
    Range(0, 20).foreach(i => assert(doubleMatchNonsenseRead.referencePositions(i).map(_.pos) == Some(1000 + i)))

    val deletionRead = AlignmentRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("5M5D10M").setContigName(contig.getContigName).build()
    assert(deletionRead.referencePositions.length == 15)
    assert(deletionRead.referencePositions(0).map(_.pos) == Some(1000L))
    assert(deletionRead.referencePositions(5).map(_.pos) == Some(1010L))

    val insertionRead = AlignmentRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M2I10M").setContigName(contig.getContigName).build()
    assert(insertionRead.referencePositions.length == 22)
    assert(insertionRead.referencePositions(0).map(_.pos) == Some(1000L))
    assert(insertionRead.referencePositions(10).map(_.pos) == None)
    assert(insertionRead.referencePositions(12).map(_.pos) == Some(1010L))

    val indelRead = AlignmentRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M3D10M2I").setContigName(contig.getContigName).build()
    assert(indelRead.referencePositions.length == 22)
    assert(indelRead.referencePositions(0).map(_.pos) == Some(1000L))
    assert(indelRead.referencePositions(10).map(_.pos) == Some(1013L))
    assert(indelRead.referencePositions(20).map(_.pos) == None)

    val hg00096read = AlignmentRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("1S28M1D32M1I15M1D23M").setContigName(contig.getContigName).build()
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
    val unmappedRead = AlignmentRecord.newBuilder().setReadMapped(false).setStart(0L).setCigar("10M").setEnd(10L).build()

    val overlaps = unmappedRead.overlapsReferencePosition(ReferencePosition("chr1", 10))
    assert(!overlaps)
  }

  test("read overlap reference position") {

    val contig = Contig.newBuilder.setContigName("chr1").build
    val record = RichAlignmentRecord(AlignmentRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(10L).setEnd(20L).setContigName(contig.getContigName).build())

    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 10)) == true)
    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 14)) == true)
    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 19)) == true)
    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 20)) == false)
  }

  test("read overlap same position different contig") {

    val contig = Contig.newBuilder.setContigName("chr1").build
    val record = RichAlignmentRecord(AlignmentRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(10L).setEnd(20L).setContigName(contig.getContigName).build())

    assert(record.overlapsReferencePosition(ReferencePosition("chr2", 10)) == false)
  }

}
