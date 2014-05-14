package org.bdgenomics.adam.predicates

import org.scalatest.FunSuite
import org.bdgenomics.adam.avro._
import org.bdgenomics.adam.projections.ADAMRecordField

class RecordConditionSuite extends FunSuite {

  test("create record condition from simple field condition") {
    val mappedReadCondition = RecordCondition[ADAMRecord](
      FieldCondition(ADAMRecordField.readMapped.toString(), (x: Boolean) => x))

    val mappedRead = ADAMRecord.newBuilder
      .setReadMapped(true)
      .build
    assert(mappedReadCondition(mappedRead))

    val unmappedRead = ADAMRecord.newBuilder
      .setReadMapped(false)
      .build

    assert(!mappedReadCondition(unmappedRead))

    val underspecifiedRead = ADAMRecord.newBuilder
      .setMapq(30)
      .build

    assert(!mappedReadCondition(underspecifiedRead))

  }

  test("create record condition from nested field condition") {
    val v0 = ADAMVariant.newBuilder
      .setContig("11")
      .setPosition(17409571)
      .setReferenceAllele("T")
      .setVariantAllele("C")
      .build

    val passFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(true).build()
    val failFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(false).build()

    val passGenotype =
      ADAMGenotype.newBuilder().setVariant(v0)
        .setSampleId("ignored")
        .setVariantCallingAnnotations(passFilterAnnotation)
        .build
    val failGenotype = ADAMGenotype.newBuilder()
      .setSampleId("ignored")
      .setVariant(v0)
      .setVariantCallingAnnotations(failFilterAnnotation)
      .build

    val isPassing = RecordCondition[ADAMGenotype](FieldCondition("variantCallingAnnotations.variantIsPassing", PredicateUtils.isTrue))

    assert(isPassing(passGenotype))
    assert(!isPassing(failGenotype))

  }

  test("create record condition from multiple field conditions") {
    val mappedReadCondition = RecordCondition[ADAMRecord](
      FieldCondition(ADAMRecordField.readMapped.toString(), (x: Boolean) => x),
      FieldCondition(ADAMRecordField.primaryAlignment.toString(), (x: Boolean) => x),
      FieldCondition(ADAMRecordField.failedVendorQualityChecks.toString(), (x: Boolean) => !x),
      FieldCondition(ADAMRecordField.duplicateRead.toString(), (x: Boolean) => !x))

    val mappedRead = ADAMRecord.newBuilder
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setFailedVendorQualityChecks(false)
      .setDuplicateRead(false)
      .build

    assert(mappedReadCondition(mappedRead))

    val mappedDuplicateRead = ADAMRecord.newBuilder
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setFailedVendorQualityChecks(false)
      .setDuplicateRead(true)
      .build

    assert(!mappedReadCondition(mappedDuplicateRead))

    val mappedSecondaryAlignmentRead = ADAMRecord.newBuilder
      .setReadMapped(true)
      .setPrimaryAlignment(false)
      .setFailedVendorQualityChecks(false)
      .setDuplicateRead(false)
      .build

    assert(!mappedReadCondition(mappedSecondaryAlignmentRead))

    val unmappedRead = ADAMRecord.newBuilder
      .setReadMapped(false)
      .build

    assert(!mappedReadCondition(unmappedRead))
  }

  test("create record condition from non-equality field conditions") {
    val highQualityReadCondition = RecordCondition[ADAMRecord](
      FieldCondition(ADAMRecordField.readMapped.toString(), PredicateUtils.isTrue),
      FieldCondition(ADAMRecordField.mapq.toString(), (x: Int) => x > 10))

    val highQualityRead = ADAMRecord.newBuilder
      .setReadMapped(true)
      .setMapq(30)
      .build

    assert(highQualityReadCondition(highQualityRead))

    val lowQualityRead = ADAMRecord.newBuilder
      .setReadMapped(true)
      .setMapq(5)
      .build

    assert(!highQualityReadCondition(lowQualityRead))
  }

  test("create record condition OR of record conditions") {

    val sample1Conditon = RecordCondition[ADAMRecord](
      FieldCondition(ADAMRecordField.recordGroupSample.toString(), (x: String) => x == "sample1"))

    val sample2Conditon = RecordCondition[ADAMRecord](
      FieldCondition(ADAMRecordField.recordGroupSample.toString(), (x: String) => x == "sample2"))

    val sample1ORsample2 = sample1Conditon || sample2Conditon

    val sample1Read = ADAMRecord.newBuilder
      .setRecordGroupSample("sample1")
      .build

    val sample2Read = ADAMRecord.newBuilder
      .setRecordGroupSample("sample2")
      .build

    val sample3Read = ADAMRecord.newBuilder
      .setRecordGroupSample("sample3")
      .build

    assert(sample1ORsample2(sample1Read))
    assert(sample1ORsample2(sample2Read))
    assert(!sample1ORsample2(sample3Read))
  }

  test("high quality adam read condition") {

    val highQualityReadCondition = ADAMRecordConditions.isHighQuality(10)
    val highQualityRead = ADAMRecord.newBuilder
      .setReadMapped(true)
      .setMapq(30)
      .build

    assert(highQualityReadCondition(highQualityRead))

    val lowQualityRead = ADAMRecord.newBuilder
      .setReadMapped(true)
      .setMapq(5)
      .build

    assert(!highQualityReadCondition(lowQualityRead))
  }

  test("passing genotype record condition") {
    val v0 = ADAMVariant.newBuilder
      .setContig("11")
      .setPosition(17409571)
      .setReferenceAllele("T")
      .setVariantAllele("C")
      .build

    val passFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(true).build()
    val failFilterAnnotation =
      VariantCallingAnnotations.newBuilder().setVariantIsPassing(false).build()

    val genotypes = Seq[ADAMGenotype](
      ADAMGenotype.newBuilder().setVariant(v0)
        .setSampleId("ignored")
        .setVariantCallingAnnotations(passFilterAnnotation).build(),
      ADAMGenotype.newBuilder()
        .setSampleId("ignored")
        .setVariant(v0)
        .setVariantCallingAnnotations(failFilterAnnotation).build())

    val filtered = genotypes.filter(ADAMGenotypeConditions.isPassing.filter)

    assert(filtered.size == 1)
  }

}
