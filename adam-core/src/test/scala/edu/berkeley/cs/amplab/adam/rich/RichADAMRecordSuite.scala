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
package edu.berkeley.cs.amplab.adam.rich

import org.scalatest.FunSuite
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import RichADAMRecord._
import edu.berkeley.cs.amplab.adam.models.{TagType, Attribute}

class RichADAMRecordSuite extends FunSuite {

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

  test( "Reference End Position")
  {
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
    val softClippedRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(100).setCigar("10S90M").build()
    assert( softClippedRead.referencePositions(0) == Some(90L))


  }

  test("tags contains optional fields") {
    val rec = ADAMRecord.newBuilder().setAttributes("XX:i:3\tYY:Z:foo").build()
    assert(rec.tags.size === 2)
    assert(rec.tags(0) === Attribute("XX", TagType.Integer, 3))
    assert(rec.tags(1) === Attribute("YY", TagType.String, "foo"))
  }

  test("Reference Positions") {

    val hardClippedRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("90M10H").build()
    assert( hardClippedRead.referencePositions.length == 90)
    assert( hardClippedRead.referencePositions(0) == Some(1000L))

    val softClippedRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10S90M").build()
    assert( softClippedRead.referencePositions.length == 100)
    assert( softClippedRead.referencePositions(0) == Some(990L))
    assert( softClippedRead.referencePositions(10) == Some(1000L))

    val doubleMatchNonsenseRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M10M").build()
    Range(0,20).foreach(  i => assert( doubleMatchNonsenseRead.referencePositions(i) == Some(1000 + i) ))

    val deletionRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("5M5D10M").build()
    assert( deletionRead.referencePositions.length == 15)
    assert( deletionRead.referencePositions(0) == Some(1000L))
    assert( deletionRead.referencePositions(5) == Some(1010L))

    val insertionRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M2I10M").build()
    assert( insertionRead.referencePositions.length == 22)
    assert( insertionRead.referencePositions(0) == Some(1000L))
    assert( insertionRead.referencePositions(10) == None)
    assert( insertionRead.referencePositions(12) == Some(1010L))

    val indelRead = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("10M3D10M2I").build()
    assert( indelRead.referencePositions.length == 22)
    assert( indelRead.referencePositions(0) == Some(1000L))
    assert( indelRead.referencePositions(10) == Some(1013L))
    assert( indelRead.referencePositions(20)  == None )

    val hg00096read = ADAMRecord.newBuilder().setReadMapped(true).setStart(1000).setCigar("1S28M1D32M1I15M1D23M").build()
    assert( hg00096read.referencePositions.length == 100)
    assert( hg00096read.referencePositions(0) == Some(999L))
    assert( hg00096read.referencePositions(1) == Some(1000L))
    assert( hg00096read.referencePositions(29)  == Some(1029L) )
    assert( hg00096read.referencePositions(61)  == None )
    assert( hg00096read.referencePositions(62)  == Some(1061L) )
    assert( hg00096read.referencePositions(78)  == Some(1078L) )
    assert( hg00096read.referencePositions(99)  == Some(1099L) )

  }

}
