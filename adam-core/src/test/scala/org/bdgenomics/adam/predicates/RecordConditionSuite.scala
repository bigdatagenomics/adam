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
package org.bdgenomics.adam.predicates

import org.bdgenomics.adam.projections.AlignmentRecordField
import org.bdgenomics.formats.avro._
import org.scalatest.FunSuite

class RecordConditionSuite extends FunSuite {

  test("create record condition from simple field condition") {
    val mappedReadCondition = RecordCondition[AlignmentRecord](
      FieldCondition(AlignmentRecordField.readMapped.toString(), (x: Boolean) => x))

    val mappedRead = AlignmentRecord.newBuilder
      .setReadMapped(true)
      .build
    assert(mappedReadCondition(mappedRead))

    val unmappedRead = AlignmentRecord.newBuilder
      .setReadMapped(false)
      .build

    assert(!mappedReadCondition(unmappedRead))

    val underspecifiedRead = AlignmentRecord.newBuilder
      .setMapq(30)
      .build

    assert(!mappedReadCondition(underspecifiedRead))

  }

  test("create record condition from nested field condition") {
    val v0 = Variant.newBuilder
      .setContig(Contig.newBuilder.setContigName("11").build)
      .setStart(17409571)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .build

    val passFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(true).build()
    val failFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(false).build()

    val passGenotype =
      Genotype.newBuilder().setVariant(v0)
        .setSampleId("ignored")
        .setVariantCallingAnnotations(passFilterAnnotation)
        .build
    val failGenotype = Genotype.newBuilder()
      .setSampleId("ignored")
      .setVariant(v0)
      .setVariantCallingAnnotations(failFilterAnnotation)
      .build

    val isPassing = RecordCondition[Genotype](FieldCondition("variantCallingAnnotations.variantIsPassing", PredicateUtils.isTrue))

    assert(isPassing(passGenotype))
    assert(!isPassing(failGenotype))

  }

  test("create record condition from multiple field conditions") {
    val mappedReadCondition = RecordCondition[AlignmentRecord](
      FieldCondition(AlignmentRecordField.readMapped.toString(), (x: Boolean) => x),
      FieldCondition(AlignmentRecordField.primaryAlignment.toString(), (x: Boolean) => x),
      FieldCondition(AlignmentRecordField.failedVendorQualityChecks.toString(), (x: Boolean) => !x),
      FieldCondition(AlignmentRecordField.duplicateRead.toString(), (x: Boolean) => !x))

    val mappedRead = AlignmentRecord.newBuilder
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setFailedVendorQualityChecks(false)
      .setDuplicateRead(false)
      .build

    assert(mappedReadCondition(mappedRead))

    val mappedDuplicateRead = AlignmentRecord.newBuilder
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setFailedVendorQualityChecks(false)
      .setDuplicateRead(true)
      .build

    assert(!mappedReadCondition(mappedDuplicateRead))

    val mappedSecondaryAlignmentRead = AlignmentRecord.newBuilder
      .setReadMapped(true)
      .setPrimaryAlignment(false)
      .setFailedVendorQualityChecks(false)
      .setDuplicateRead(false)
      .build

    assert(!mappedReadCondition(mappedSecondaryAlignmentRead))

    val unmappedRead = AlignmentRecord.newBuilder
      .setReadMapped(false)
      .build

    assert(!mappedReadCondition(unmappedRead))
  }

  test("create record condition from non-equality field conditions") {
    val highQualityReadCondition = RecordCondition[AlignmentRecord](
      FieldCondition(AlignmentRecordField.readMapped.toString(), PredicateUtils.isTrue),
      FieldCondition(AlignmentRecordField.mapq.toString(), (x: Int) => x > 10))

    val highQualityRead = AlignmentRecord.newBuilder
      .setReadMapped(true)
      .setMapq(30)
      .build

    assert(highQualityReadCondition(highQualityRead))

    val lowQualityRead = AlignmentRecord.newBuilder
      .setReadMapped(true)
      .setMapq(5)
      .build

    assert(!highQualityReadCondition(lowQualityRead))
  }

  test("create record condition OR of record conditions") {

    val sample1Conditon = RecordCondition[AlignmentRecord](
      FieldCondition(AlignmentRecordField.recordGroupSample.toString(), (x: String) => x == "sample1"))

    val sample2Conditon = RecordCondition[AlignmentRecord](
      FieldCondition(AlignmentRecordField.recordGroupSample.toString(), (x: String) => x == "sample2"))

    val sample1ORsample2 = sample1Conditon || sample2Conditon

    val sample1Read = AlignmentRecord.newBuilder
      .setRecordGroupSample("sample1")
      .build

    val sample2Read = AlignmentRecord.newBuilder
      .setRecordGroupSample("sample2")
      .build

    val sample3Read = AlignmentRecord.newBuilder
      .setRecordGroupSample("sample3")
      .build

    assert(sample1ORsample2(sample1Read))
    assert(sample1ORsample2(sample2Read))
    assert(!sample1ORsample2(sample3Read))
  }

  test("high quality adam read condition") {

    val highQualityReadCondition = AlignmentRecordConditions.isHighQuality(10)
    val highQualityRead = AlignmentRecord.newBuilder
      .setReadMapped(true)
      .setMapq(30)
      .build

    assert(highQualityReadCondition(highQualityRead))

    val lowQualityRead = AlignmentRecord.newBuilder
      .setReadMapped(true)
      .setMapq(5)
      .build

    assert(!highQualityReadCondition(lowQualityRead))
  }

  test("passing genotype record condition") {
    val v0 = Variant.newBuilder
      .setContig(Contig.newBuilder.setContigName("11").build)
      .setStart(17409571)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .build

    val passFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(true).build()
    val failFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(false).build()

    val genotypes = Seq[Genotype](
      Genotype.newBuilder().setVariant(v0)
        .setSampleId("ignored")
        .setVariantCallingAnnotations(passFilterAnnotation).build(),
      Genotype.newBuilder()
        .setSampleId("ignored")
        .setVariant(v0)
        .setVariantCallingAnnotations(failFilterAnnotation).build())

    val filtered = genotypes.filter(GenotypeConditions.isPassing.filter)

    assert(filtered.size == 1)
  }

}
