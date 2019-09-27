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
import org.bdgenomics.formats.avro.{ Alignment, Genotype, Variant, Reference }

class ReferencePositionSuite extends FunSuite {

  test("create reference position from mapped read") {
    val reference = Reference.newBuilder
      .setName("chr1")
      .build

    val read = Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(1L)
      .setReadMapped(true)
      .build()

    val refPos = ReferencePosition(read)

    assert(refPos.referenceName === "chr1")
    assert(refPos.pos === 1L)
  }

  test("create reference position from variant") {
    val variant = Variant.newBuilder()
      .setReferenceName("chr10")
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
      .setReferenceName("chr10")
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .build()
    val genotype = Genotype.newBuilder()
      .setVariant(variant)
      .setStart(100L)
      .setEnd(101L)
      .setReferenceName("chr10")
      .setSampleId("NA12878")
      .build()

    val refPos = ReferencePosition(genotype)

    assert(refPos.referenceName === "chr10")
    assert(refPos.pos === 100L)
  }

  test("create reference position from variant starting at vcf 0") {
    val variant = Variant.newBuilder()
      .setReferenceName("chr10")
      .setStart(0L)
      .build()

    val refPos = ReferencePosition(variant)

    assert(refPos.referenceName === "chr10")
    assert(refPos.pos === 0L)
  }

  test("create reference position from genotype starting at vcf 0") {
    val variant = Variant.newBuilder()
      .setStart(0L)
      .setReferenceName("chr10")
      .build()
    val genotype = Genotype.newBuilder()
      .setVariant(variant)
      .setStart(0L)
      .setReferenceName("chr10")
      .build()

    val refPos = ReferencePosition(genotype)

    assert(refPos.referenceName === "chr10")
    assert(refPos.pos === 0L)
  }
}
