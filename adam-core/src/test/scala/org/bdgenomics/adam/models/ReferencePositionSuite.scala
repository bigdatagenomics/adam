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

import org.scalatest.FunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig, Genotype, Variant }

class ReferencePositionSuite extends FunSuite {

  test("create reference position from mapped read") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .build

    val read = AlignmentRecord.newBuilder()
      .setContig(contig)
      .setStart(1L)
      .setReadMapped(true)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isDefined)

    val refPos = refPosOpt.get

    assert(refPos.referenceName === "chr1")
    assert(refPos.pos === 1L)
  }

  test("create reference position from unmapped read") {
    val read = AlignmentRecord.newBuilder()
      .setReadMapped(false)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isEmpty)
  }

  test("create reference position from mapped read but contig not specified") {
    val read = AlignmentRecord.newBuilder()
      .setReadMapped(true)
      .setStart(1L)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isEmpty)
  }

  test("create reference position from mapped read but contig is underspecified") {
    val contig = Contig.newBuilder
      // contigName is NOT set
      //.setContigName("chr1")
      .build

    val read = AlignmentRecord.newBuilder()
      .setReadMapped(true)
      .setStart(1L)
      .setContig(contig)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isEmpty)
  }

  test("create reference position from mapped read but start not specified") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .build

    val read = AlignmentRecord.newBuilder()
      .setReadMapped(true)
      .setContig(contig)
      .build()

    val refPosOpt = ReferencePosition(read)

    assert(refPosOpt.isEmpty)
  }

  test("create reference position from variant") {
    val variant = Variant.newBuilder()
      .setContig(Contig.newBuilder.setContigName("chr10").build())
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .setStart(10L)
      .build()

    val refPos = ReferencePosition(variant)

    assert(refPos.referenceName === "chr10")
    assert(refPos.pos === 10L)
  }

  test("create reference position from genotype") {
    val variant = Variant.newBuilder()
      .setStart(100L)
      .setContig(Contig.newBuilder.setContigName("chr10").build())
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .build()
    val genotype = Genotype.newBuilder()
      .setVariant(variant)
      .setSampleId("NA12878")
      .build()

    val refPos = ReferencePosition(genotype)

    assert(refPos.referenceName === "chr10")
    assert(refPos.pos === 100L)
  }

  test("liftOverToReference works with a multi-block alignment on the forward strand") {
    val exons = Seq(ReferenceRegionWithOrientation("1", 100, 200, negativeStrand = false),
      ReferenceRegionWithOrientation("1", 300, 400, negativeStrand = false),
      ReferenceRegionWithOrientation("1", 500, 600, negativeStrand = false))

    val p0 = ReferencePositionWithOrientation.liftOverToReference(0, exons)
    assert(p0.refPos.referenceName === "1")
    assert(p0.refPos.pos === 100)

    val p1 = ReferencePositionWithOrientation.liftOverToReference(50, exons)
    assert(p1.refPos.referenceName === "1")
    assert(p1.refPos.pos === 150)

    val p2 = ReferencePositionWithOrientation.liftOverToReference(150, exons)
    assert(p2.refPos.referenceName === "1")
    assert(p2.refPos.pos === 350)

    val p3 = ReferencePositionWithOrientation.liftOverToReference(250, exons)
    assert(p3.refPos.referenceName === "1")
    assert(p3.refPos.pos === 550)
  }

  test("liftOverToReference works with a multi-block alignment on the reverse strand") {
    val exons = Seq(ReferenceRegionWithOrientation("1", 100, 200, negativeStrand = true),
      ReferenceRegionWithOrientation("1", 300, 400, negativeStrand = true),
      ReferenceRegionWithOrientation("1", 500, 600, negativeStrand = true))

    val p1 = ReferencePositionWithOrientation.liftOverToReference(50, exons)
    assert(p1.refPos.referenceName === "1")
    assert(p1.refPos.pos === 549)

    val p2 = ReferencePositionWithOrientation.liftOverToReference(150, exons)
    assert(p2.refPos.referenceName === "1")
    assert(p2.refPos.pos === 349)

    val p3 = ReferencePositionWithOrientation.liftOverToReference(250, exons)
    assert(p3.refPos.referenceName === "1")
    assert(p3.refPos.pos === 149)
  }

  test("lift over between two transcripts on the forward strand") {
    // create mappings for transcripts
    val t1 = Seq(ReferenceRegionWithOrientation("chr0", 0L, 201L, negativeStrand = false))
    val t2 = Seq(ReferenceRegionWithOrientation("chr0", 50L, 101L, negativeStrand = false),
      ReferenceRegionWithOrientation("chr0", 175L, 201L, negativeStrand = false))

    // check forward strand
    val pos = ReferencePositionWithOrientation.liftOverToReference(60, t1)

    assert(pos.refPos.referenceName === "chr0")
    assert(pos.refPos.pos === 60L)
    assert(!pos.negativeStrand)

    val idx = pos.liftOverFromReference(t2)

    assert(idx === 10L)
  }

  test("lift over between two transcripts on the reverse strand") {
    // create mappings for transcripts
    val t1 = Seq(ReferenceRegionWithOrientation("chr0", 0L, 201L, negativeStrand = true))
    val t2 = Seq(ReferenceRegionWithOrientation("chr0", 175L, 201L, negativeStrand = true),
      ReferenceRegionWithOrientation("chr0", 50L, 101L, negativeStrand = true))

    // check reverse strand
    val idx = ReferencePositionWithOrientation(ReferencePosition("chr0", 190L), negativeStrand = true)
      .liftOverFromReference(t2)

    assert(idx === 11L)

    val pos = ReferencePositionWithOrientation.liftOverToReference(idx, t1)

    assert(pos.refPos.referenceName === "chr0")
    assert(pos.refPos.pos === 189L)
    assert(pos.negativeStrand)
  }
}
