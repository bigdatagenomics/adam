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

import org.bdgenomics.adam.rich.ReferenceMappingContext._
import org.bdgenomics.formats.avro.{ ADAMContig, ADAMRecord }
import org.scalatest.FunSuite

class TrackedLayoutSuite extends FunSuite {

  def rec(contig: String = null, start: Int = 0, cigar: String = null, readMapped: Boolean = true, firstOfPair: Boolean = true): ADAMRecord = {
    val c = ADAMContig.newBuilder().setContigName(contig).build()

    ADAMRecord.newBuilder()
      .setContig(c)
      .setReadMapped(readMapped)
      .setFirstOfPair(firstOfPair)
      .setStart(start)
      .setCigar(cigar)
      .build()
  }

  test("OrderedTrackedLayout lays out no records into zero tracks") {
    val layout = new OrderedTrackedLayout[ADAMRecord](Seq())
    assert(layout.numTracks === 0)
  }

  test("OrderedTrackedLayout lays out ADAMRecords left-to-right in the order they're passed") {
    val (r1, r2, r3, r4) = (
      rec("chr1", 100, "100M"),
      rec("chr1", 150, "100M"),
      rec("chr1", 200, "100M"),
      rec("chr2", 200, "100M"))

    val recs = Seq(r1, r2, r3, r4)
    val layout: OrderedTrackedLayout[ADAMRecord] = new OrderedTrackedLayout(recs)

    // Just making sure... r4 shouldn't overlap r1-3
    val rm: ReferenceMapping[ADAMRecord] = ADAMRecordReferenceMapping
    val refs123 = Seq(r1, r2, r3).map(rm.getReferenceRegion)
    assert(!refs123.exists(rm.getReferenceRegion(r4).overlaps))

    // Now, test the correct track assignments of each record
    assert(layout.trackAssignments.get(r1) === Some(0))
    assert(layout.trackAssignments.get(r2) === Some(1))
    assert(layout.trackAssignments.get(r3) === Some(0))
    assert(layout.trackAssignments.get(r4) === Some(0))
    assert(layout.numTracks === 2)
  }

  test("OrderedTrackedLayout can handle unaligned reads") {
    val (r1, r2, r3, r4) = (
      rec("chr1", 100, "100M"),
      rec("chr1", 150, "100M"),
      rec("chr1", 200, "100M"),
      rec(readMapped = false))

    val recs = Seq(r1, r2, r3, r4)
    val layout: OrderedTrackedLayout[ADAMRecord] = new OrderedTrackedLayout(recs)

    // Just making sure... r4 shouldn't overlap r1-3
    val rm: ReferenceMapping[ADAMRecord] = ADAMRecordReferenceMapping
    val refs123 = Seq(r1, r2, r3).map(rm.getReferenceRegion)

    // Now, test the correct track assignments of each record
    assert(layout.trackAssignments.get(r1) === Some(0))
    assert(layout.trackAssignments.get(r2) === Some(1))
    assert(layout.trackAssignments.get(r3) === Some(0))
    assert(layout.trackAssignments.get(r4) === None)
    assert(layout.numTracks === 2)
  }
}
