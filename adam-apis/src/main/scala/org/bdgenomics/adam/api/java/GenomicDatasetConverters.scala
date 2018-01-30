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
package org.bdgenomics.adam.api.java

import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.models.{ Coverage, VariantContext }
import org.bdgenomics.adam.rdd.{
  ADAMContext,
  GenomicDataset,
  GenomicDatasetConversion
}
import org.bdgenomics.adam.rdd.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.{ AlignmentRecordDataset, ReadDataset }
import org.bdgenomics.adam.rdd.sequence.{ SequenceDataset, SliceDataset }
import org.bdgenomics.adam.rdd.variant.{
  VariantDataset,
  GenotypeDataset,
  VariantContextDataset
}
import org.bdgenomics.adam.sql.{
  AlignmentRecord => AlignmentRecordProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  Read => ReadProduct,
  Sequence => SequenceProduct,
  Slice => SliceProduct,
  Variant => VariantProduct,
  VariantContext => VariantContextProduct
}
import org.bdgenomics.formats.avro._
import scala.reflect.runtime.universe._

trait ToCoverageDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Coverage, Coverage, CoverageDataset] {

  val yTag: TypeTag[Coverage] = typeTag[Coverage]
}

trait ToFeatureDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Feature, FeatureProduct, FeatureDataset] {

  val yTag: TypeTag[FeatureProduct] = typeTag[FeatureProduct]
}

trait ToFragmentDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Fragment, FragmentProduct, FragmentDataset] {

  val yTag: TypeTag[FragmentProduct] = typeTag[FragmentProduct]
}

trait ToAlignmentRecordDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  val yTag: TypeTag[AlignmentRecordProduct] = typeTag[AlignmentRecordProduct]
}

trait ToGenotypeDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Genotype, GenotypeProduct, GenotypeDataset] {

  val yTag: TypeTag[GenotypeProduct] = typeTag[GenotypeProduct]
}

trait ToReadDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Read, ReadProduct, ReadDataset] {

  val yTag: TypeTag[ReadProduct] = typeTag[ReadProduct]
}

trait ToSequenceDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Sequence, SequenceProduct, SequenceDataset] {

  val yTag: TypeTag[SequenceProduct] = typeTag[SequenceProduct]
}

trait ToSliceDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Slice, SliceProduct, SliceDataset] {

  val yTag: TypeTag[SliceProduct] = typeTag[SliceProduct]
}

trait ToVariantDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Variant, VariantProduct, VariantDataset] {
  val yTag: TypeTag[VariantProduct] = typeTag[VariantProduct]
}

trait ToVariantContextDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, VariantContext, VariantContextProduct, VariantContextDataset] {
  val yTag: TypeTag[VariantContextProduct] = typeTag[VariantContextProduct]
}

final class CoverageToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.coverageToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class CoverageToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.coverageToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.coverageToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.coverageToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class CoverageToReadsDatasetConverter extends ToReadDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[ReadProduct]): ReadDataset = {
    ADAMContext.coverageToReadsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToSequencesDatasetConverter extends ToSequenceDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[SequenceProduct]): SequenceDataset = {
    ADAMContext.coverageToSequencesDatasetConversionFn(v1, v2)
  }
}

final class CoverageToSlicesDatasetConverter extends ToSliceDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[SliceProduct]): SliceDataset = {
    ADAMContext.coverageToSlicesDatasetConversionFn(v1, v2)
  }
}

final class CoverageToVariantsDatasetConverter extends ToVariantDatasetConversion[Coverage, Coverage, CoverageDataset] {
  def call(v1: CoverageDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.coverageToVariantsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToCoverageDatasetConverter extends ToCoverageDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.featuresToCoverageDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.featuresToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.featuresToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.featuresToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToReadsDatasetConverter extends ToReadDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[ReadProduct]): ReadDataset = {
    ADAMContext.featuresToReadsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToSequencesDatasetConverter extends ToSequenceDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[SequenceProduct]): SequenceDataset = {
    ADAMContext.featuresToSequencesDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToSlicesDatasetConverter extends ToSliceDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[SliceProduct]): SliceDataset = {
    ADAMContext.featuresToSlicesDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToVariantsDatasetConverter extends ToVariantDatasetConversion[Feature, FeatureProduct, FeatureDataset] {
  def call(v1: FeatureDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.featuresToVariantsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToCoverageDatasetConverter extends ToCoverageDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.fragmentsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.fragmentsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.fragmentsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.fragmentsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToReadsDatasetConverter extends ToReadDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[ReadProduct]): ReadDataset = {
    ADAMContext.fragmentsToReadsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToSequencesDatasetConverter extends ToSequenceDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[SequenceProduct]): SequenceDataset = {
    ADAMContext.fragmentsToSequencesDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToSlicesDatasetConverter extends ToSliceDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[SliceProduct]): SliceDataset = {
    ADAMContext.fragmentsToSlicesDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToVariantsDatasetConverter extends ToVariantDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {
  def call(v1: FragmentDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.fragmentsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToCoverageDatasetConverter extends ToCoverageDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {
  def call(v1: AlignmentRecordDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.alignmentRecordsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.alignmentRecordsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.alignmentRecordsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.alignmentRecordsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToReadsDatasetConverter extends ToReadDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: Dataset[ReadProduct]): ReadDataset = {
    ADAMContext.alignmentRecordsToReadsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToSequencesDatasetConverter extends ToSequenceDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: Dataset[SequenceProduct]): SequenceDataset = {
    ADAMContext.alignmentRecordsToSequencesDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToSlicesDatasetConverter extends ToSliceDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: Dataset[SliceProduct]): SliceDataset = {
    ADAMContext.alignmentRecordsToSlicesDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToVariantsDatasetConverter extends ToVariantDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {
  def call(v1: AlignmentRecordDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.alignmentRecordsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToCoverageDatasetConverter extends ToCoverageDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {
  def call(v1: GenotypeDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.genotypesToCoverageDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.genotypesToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.genotypesToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.genotypesToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToReadsDatasetConverter extends ToReadDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[ReadProduct]): ReadDataset = {
    ADAMContext.genotypesToReadsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToSequencesDatasetConverter extends ToSequenceDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[SequenceProduct]): SequenceDataset = {
    ADAMContext.genotypesToSequencesDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToSlicesDatasetConverter extends ToSliceDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[SliceProduct]): SliceDataset = {
    ADAMContext.genotypesToSlicesDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToVariantsDatasetConverter extends ToVariantDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.genotypesToVariantsDatasetConversionFn(v1, v2)
  }
}

final class ReadsToCoverageDatasetConverter extends ToCoverageDatasetConversion[Read, ReadProduct, ReadDataset] {

  def call(v1: ReadDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.readsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class ReadsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Read, ReadProduct, ReadDataset] {

  def call(v1: ReadDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.readsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class ReadsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Read, ReadProduct, ReadDataset] {

  def call(v1: ReadDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.readsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class ReadsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Read, ReadProduct, ReadDataset] {

  def call(v1: ReadDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.readsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class ReadsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Read, ReadProduct, ReadDataset] {

  def call(v1: ReadDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.readsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class ReadsToSequencesDatasetConverter extends ToSequenceDatasetConversion[Read, ReadProduct, ReadDataset] {

  def call(v1: ReadDataset, v2: Dataset[SequenceProduct]): SequenceDataset = {
    ADAMContext.readsToSequencesDatasetConversionFn(v1, v2)
  }
}

final class ReadsToSlicesDatasetConverter extends ToSliceDatasetConversion[Read, ReadProduct, ReadDataset] {

  def call(v1: ReadDataset, v2: Dataset[SliceProduct]): SliceDataset = {
    ADAMContext.readsToSlicesDatasetConversionFn(v1, v2)
  }
}

final class ReadsToVariantsDatasetConverter extends ToVariantDatasetConversion[Read, ReadProduct, ReadDataset] {

  def call(v1: ReadDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.readsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class SequencesToCoverageDatasetConverter extends ToCoverageDatasetConversion[Sequence, SequenceProduct, SequenceDataset] {

  def call(v1: SequenceDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.sequencesToCoverageDatasetConversionFn(v1, v2)
  }
}

final class SequencesToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Sequence, SequenceProduct, SequenceDataset] {

  def call(v1: SequenceDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.sequencesToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class SequencesToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Sequence, SequenceProduct, SequenceDataset] {

  def call(v1: SequenceDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.sequencesToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class SequencesToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Sequence, SequenceProduct, SequenceDataset] {

  def call(v1: SequenceDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.sequencesToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class SequencesToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Sequence, SequenceProduct, SequenceDataset] {

  def call(v1: SequenceDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.sequencesToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class SequencesToReadsDatasetConverter extends ToReadDatasetConversion[Sequence, SequenceProduct, SequenceDataset] {

  def call(v1: SequenceDataset, v2: Dataset[ReadProduct]): ReadDataset = {
    ADAMContext.sequencesToReadsDatasetConversionFn(v1, v2)
  }
}

final class SequencesToSlicesDatasetConverter extends ToSliceDatasetConversion[Sequence, SequenceProduct, SequenceDataset] {

  def call(v1: SequenceDataset, v2: Dataset[SliceProduct]): SliceDataset = {
    ADAMContext.sequencesToSlicesDatasetConversionFn(v1, v2)
  }
}

final class SequencesToVariantsDatasetConverter extends ToVariantDatasetConversion[Sequence, SequenceProduct, SequenceDataset] {

  def call(v1: SequenceDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.sequencesToVariantsDatasetConversionFn(v1, v2)
  }
}

final class SlicesToCoverageDatasetConverter extends ToCoverageDatasetConversion[Slice, SliceProduct, SliceDataset] {

  def call(v1: SliceDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.slicesToCoverageDatasetConversionFn(v1, v2)
  }
}

final class SlicesToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Slice, SliceProduct, SliceDataset] {

  def call(v1: SliceDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.slicesToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class SlicesToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Slice, SliceProduct, SliceDataset] {

  def call(v1: SliceDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.slicesToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class SlicesToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Slice, SliceProduct, SliceDataset] {

  def call(v1: SliceDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.slicesToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class SlicesToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Slice, SliceProduct, SliceDataset] {

  def call(v1: SliceDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.slicesToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class SlicesToReadsDatasetConverter extends ToReadDatasetConversion[Slice, SliceProduct, SliceDataset] {

  def call(v1: SliceDataset, v2: Dataset[ReadProduct]): ReadDataset = {
    ADAMContext.slicesToReadsDatasetConversionFn(v1, v2)
  }
}

final class SlicesToSequencesDatasetConverter extends ToSequenceDatasetConversion[Slice, SliceProduct, SliceDataset] {

  def call(v1: SliceDataset, v2: Dataset[SequenceProduct]): SequenceDataset = {
    ADAMContext.slicesToSequencesDatasetConversionFn(v1, v2)
  }
}

final class SlicesToVariantsDatasetConverter extends ToVariantDatasetConversion[Slice, SliceProduct, SliceDataset] {

  def call(v1: SliceDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.slicesToVariantsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToCoverageDatasetConverter extends ToCoverageDatasetConversion[Variant, VariantProduct, VariantDataset] {
  def call(v1: VariantDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.variantsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class VariantsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Variant, VariantProduct, VariantDataset] {

  def call(v1: VariantDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.variantsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class VariantsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Variant, VariantProduct, VariantDataset] {

  def call(v1: VariantDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.variantsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Variant, VariantProduct, VariantDataset] {

  def call(v1: VariantDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.variantsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Variant, VariantProduct, VariantDataset] {

  def call(v1: VariantDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.variantsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class VariantsToReadsDatasetConverter extends ToReadDatasetConversion[Variant, VariantProduct, VariantDataset] {

  def call(v1: VariantDataset, v2: Dataset[ReadProduct]): ReadDataset = {
    ADAMContext.variantsToReadsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToSequencesDatasetConverter extends ToSequenceDatasetConversion[Variant, VariantProduct, VariantDataset] {

  def call(v1: VariantDataset, v2: Dataset[SequenceProduct]): SequenceDataset = {
    ADAMContext.variantsToSequencesDatasetConversionFn(v1, v2)
  }
}

final class VariantsToSlicesDatasetConverter extends ToSliceDatasetConversion[Variant, VariantProduct, VariantDataset] {

  def call(v1: VariantDataset, v2: Dataset[SliceProduct]): SliceDataset = {
    ADAMContext.variantsToSlicesDatasetConversionFn(v1, v2)
  }
}
