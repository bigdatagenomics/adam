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
import org.bdgenomics.adam.rich.RichAlignment._
import org.bdgenomics.formats.avro.{ Alignment, Reference }
import org.scalatest.FunSuite

class RichAlignmentSuite extends FunSuite {

  test("Unclipped Start") {
    val recordWithoutClipping = Alignment.newBuilder().setReadMapped(true).setCigar("10M").setStart(42L).setEnd(52L).build()
    val recordWithClipping = Alignment.newBuilder().setReadMapped(true).setCigar("2S8M").setStart(42L).setEnd(50L).build()
    val recordWithHardClipping = Alignment.newBuilder().setReadMapped(true).setCigar("3H2S5M4S").setStart(42L).setEnd(47L).build()
    assert(recordWithoutClipping.unclippedStart == 42L)
    assert(recordWithClipping.unclippedStart == 40L)
    assert(recordWithHardClipping.unclippedStart == 37L)
  }

  test("Unclipped End") {
    val unmappedRead = Alignment.newBuilder().setReadMapped(false).setStart(0L).setCigar("10M").setEnd(10L).build()
    val recordWithoutClipping = Alignment.newBuilder().setReadMapped(true).setCigar("10M").setStart(10L).setEnd(20L).build()
    val recordWithClipping = Alignment.newBuilder().setReadMapped(true).setCigar("8M2S").setStart(10L).setEnd(18L).build()
    val recordWithHardClipping = Alignment.newBuilder().setReadMapped(true).setCigar("6M2S2H").setStart(10L).setEnd(16L).build()
    assert(recordWithoutClipping.unclippedEnd == 20L)
    assert(recordWithClipping.unclippedEnd == 20L)
    assert(recordWithHardClipping.unclippedEnd == 20L)
  }

  test("tags contains optional fields") {
    val reference = Reference.newBuilder.setName("chr1").build
    val rec = Alignment.newBuilder().setAttributes("XX:i:3\tYY:Z:foo").setReferenceName(reference.getName).build()
    assert(rec.tags.size === 2)
    assert(rec.tags(0) === Attribute("XX", TagType.Integer, 3))
    assert(rec.tags(1) === Attribute("YY", TagType.String, "foo"))
  }

  test("read overlap unmapped read") {
    val unmappedRead = Alignment.newBuilder().setReadMapped(false).setStart(0L).setCigar("10M").setEnd(10L).build()

    val overlaps = unmappedRead.overlapsReferencePosition(ReferencePosition("chr1", 10))
    assert(!overlaps)
  }

  test("read overlap reference position") {

    val reference = Reference.newBuilder.setName("chr1").build
    val record = RichAlignment(Alignment.newBuilder()
      .setReadMapped(true)
      .setCigar("10M")
      .setStart(10L)
      .setEnd(20L)
      .setReferenceName(reference.getName)
      .build())

    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 10)) == true)
    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 14)) == true)
    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 19)) == true)
    assert(record.overlapsReferencePosition(ReferencePosition("chr1", 20)) == false)
  }

  test("read overlap same position different contig") {

    val reference = Reference.newBuilder.setName("chr1").build
    val record = RichAlignment(Alignment.newBuilder()
      .setReadMapped(true)
      .setCigar("10M")
      .setStart(10L)
      .setEnd(20L)
      .setReferenceName(reference.getName)
      .build())

    assert(record.overlapsReferencePosition(ReferencePosition("chr2", 10)) == false)
  }

}
