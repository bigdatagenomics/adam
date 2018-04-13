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
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentDataset
import org.bdgenomics.adam.rdd.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset
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
  NucleotideContigFragment => NucleotideContigFragmentProduct,
  Variant => VariantProduct,
  VariantContext => VariantContextProduct
}
import org.bdgenomics.formats.avro._
import scala.reflect.runtime.universe._

trait ToContigDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  val yTag: TypeTag[NucleotideContigFragmentProduct] = typeTag[NucleotideContigFragmentProduct]
}

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

trait ToVariantDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Variant, VariantProduct, VariantDataset] {

  val yTag: TypeTag[VariantProduct] = typeTag[VariantProduct]
}

trait ToVariantContextDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, VariantContext, VariantContextProduct, VariantContextDataset] {

  val yTag: TypeTag[VariantContextProduct] = typeTag[VariantContextProduct]
}

final class ContigsToCoverageDatasetConverter extends ToCoverageDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: Dataset[Coverage]): CoverageDataset = {
    ADAMContext.contigsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class ContigsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: Dataset[FeatureProduct]): FeatureDataset = {
    ADAMContext.contigsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class ContigsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: Dataset[FragmentProduct]): FragmentDataset = {
    ADAMContext.contigsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class ContigsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    ADAMContext.contigsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class ContigsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: Dataset[GenotypeProduct]): GenotypeDataset = {
    ADAMContext.contigsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class ContigsToVariantsDatasetConverter extends ToVariantDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.contigsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToContigsDatasetConverter extends ToContigDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    ADAMContext.coverageToContigsDatasetConversionFn(v1, v2)
  }
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

final class CoverageToVariantsDatasetConverter extends ToVariantDatasetConversion[Coverage, Coverage, CoverageDataset] {

  def call(v1: CoverageDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.coverageToVariantsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToContigsDatasetConverter extends ToContigDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    ADAMContext.featuresToContigsDatasetConversionFn(v1, v2)
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

final class FeaturesToVariantsDatasetConverter extends ToVariantDatasetConversion[Feature, FeatureProduct, FeatureDataset] {

  def call(v1: FeatureDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.featuresToVariantsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToContigsDatasetConverter extends ToContigDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    ADAMContext.fragmentsToContigsDatasetConversionFn(v1, v2)
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

final class FragmentsToVariantsDatasetConverter extends ToVariantDatasetConversion[Fragment, FragmentProduct, FragmentDataset] {

  def call(v1: FragmentDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.fragmentsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToContigsDatasetConverter extends ToContigDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    ADAMContext.alignmentRecordsToContigsDatasetConversionFn(v1, v2)
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

final class AlignmentRecordsToVariantsDatasetConverter extends ToVariantDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.alignmentRecordsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToContigsDatasetConverter extends ToContigDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    ADAMContext.genotypesToContigsDatasetConversionFn(v1, v2)
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

final class GenotypesToVariantsDatasetConverter extends ToVariantDatasetConversion[Genotype, GenotypeProduct, GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: Dataset[VariantProduct]): VariantDataset = {
    ADAMContext.genotypesToVariantsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToContigsDatasetConverter extends ToContigDatasetConversion[Variant, VariantProduct, VariantDataset] {

  def call(v1: VariantDataset, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    ADAMContext.variantsToContigsDatasetConversionFn(v1, v2)
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
