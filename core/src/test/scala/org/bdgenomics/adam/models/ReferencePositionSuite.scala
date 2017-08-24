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
      .setContigName(contig.getContigName)
      .setStart(1L)
      .setReadMapped(true)
      .build()

    val refPos = ReferencePosition(read)

    assert(refPos.referenceName === "chr1")
    assert(refPos.pos === 1L)
  }

  test("create reference position from variant") {
    val variant = Variant.newBuilder()
      .setContigName("chr10")
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
      .setContigName("chr10")
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .build()
    val genotype = Genotype.newBuilder()
      .setVariant(variant)
      .setStart(100L)
      .setEnd(101L)
      .setContigName("chr10")
      .setSampleId("NA12878")
      .build()

    val refPos = ReferencePosition(genotype)

    assert(refPos.referenceName === "chr10")
    assert(refPos.pos === 100L)
  }
}
