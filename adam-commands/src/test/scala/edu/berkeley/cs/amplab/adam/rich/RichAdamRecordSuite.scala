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
import RichAdamRecord._

class RichAdamRecordSuite extends FunSuite {

  test("Unclipped Start") {
    val recordWithoutClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(42).build()
    val recordWithClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("2S8M").setStart(42).build()
    val recordWithHardClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("3H2S5M4S").setStart(42).build()
    assert(recordWithoutClipping.unclippedStart.get == 42L)
    assert(recordWithClipping.unclippedStart.get == 40L)
    assert(recordWithHardClipping.unclippedStart.get == 37L)
  }

  test("Unclipped End") {
    val unmappedRead = ADAMRecord.newBuilder().setReadMapped(false).setStart(0).setCigar("10M").build()
    val recordWithoutClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("10M").setStart(10).build()
    val recordWithClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("8M2S").setStart(10).build()
    val recordWithHardClipping = ADAMRecord.newBuilder().setReadMapped(true).setCigar("6M2S2H").setStart(10).build()
    assert(unmappedRead.unclippedEnd == None)
    assert(recordWithoutClipping.unclippedEnd.get == 20L)
    assert(recordWithClipping.unclippedEnd.get == 20L)
    assert(recordWithHardClipping.unclippedEnd.get == 20L)
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

}
