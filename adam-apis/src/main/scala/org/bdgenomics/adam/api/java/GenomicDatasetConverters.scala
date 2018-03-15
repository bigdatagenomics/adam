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
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.{ CoverageRDD, FeatureRDD }
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.variant.{
  VariantRDD,
  GenotypeRDD,
  VariantContextRDD
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

trait ToContigDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentRDD] {

  val yTag: TypeTag[NucleotideContigFragmentProduct] = typeTag[NucleotideContigFragmentProduct]
}

trait ToCoverageDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Coverage, Coverage, CoverageRDD] {

  val yTag: TypeTag[Coverage] = typeTag[Coverage]
}

trait ToFeatureDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Feature, FeatureProduct, FeatureRDD] {

  val yTag: TypeTag[FeatureProduct] = typeTag[FeatureProduct]
}

trait ToFragmentDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Fragment, FragmentProduct, FragmentRDD] {

  val yTag: TypeTag[FragmentProduct] = typeTag[FragmentProduct]
}

trait ToAlignmentRecordDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, AlignmentRecord, AlignmentRecordProduct, AlignmentRecordRDD] {

  val yTag: TypeTag[AlignmentRecordProduct] = typeTag[AlignmentRecordProduct]
}

trait ToGenotypeDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Genotype, GenotypeProduct, GenotypeRDD] {

  val yTag: TypeTag[GenotypeProduct] = typeTag[GenotypeProduct]
}

trait ToVariantDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, Variant, VariantProduct, VariantRDD] {

  val yTag: TypeTag[VariantProduct] = typeTag[VariantProduct]
}

trait ToVariantContextDatasetConversion[T, U <: Product, V <: GenomicDataset[T, U, V]] extends GenomicDatasetConversion[T, U, V, VariantContext, VariantContextProduct, VariantContextRDD] {

  val yTag: TypeTag[VariantContextProduct] = typeTag[VariantContextProduct]
}

final class ContigsToCoverageDatasetConverter extends ToCoverageDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.contigsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class ContigsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[FeatureProduct]): FeatureRDD = {
    ADAMContext.contigsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class ContigsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[FragmentProduct]): FragmentRDD = {
    ADAMContext.contigsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class ContigsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    ADAMContext.contigsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class ContigsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[GenotypeProduct]): GenotypeRDD = {
    ADAMContext.contigsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class ContigsToVariantsDatasetConverter extends ToVariantDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[VariantProduct]): VariantRDD = {
    ADAMContext.contigsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToContigsDatasetConverter extends ToContigDatasetConversion[Coverage, Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    ADAMContext.coverageToContigsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Coverage, Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[FeatureProduct]): FeatureRDD = {
    ADAMContext.coverageToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class CoverageToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Coverage, Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[FragmentProduct]): FragmentRDD = {
    ADAMContext.coverageToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Coverage, Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    ADAMContext.coverageToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Coverage, Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[GenotypeProduct]): GenotypeRDD = {
    ADAMContext.coverageToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class CoverageToVariantsDatasetConverter extends ToVariantDatasetConversion[Coverage, Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[VariantProduct]): VariantRDD = {
    ADAMContext.coverageToVariantsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToContigsDatasetConverter extends ToContigDatasetConversion[Feature, FeatureProduct, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    ADAMContext.featuresToContigsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToCoverageDatasetConverter extends ToCoverageDatasetConversion[Feature, FeatureProduct, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.featuresToCoverageDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Feature, FeatureProduct, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[FragmentProduct]): FragmentRDD = {
    ADAMContext.featuresToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Feature, FeatureProduct, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    ADAMContext.featuresToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Feature, FeatureProduct, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[GenotypeProduct]): GenotypeRDD = {
    ADAMContext.featuresToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToVariantsDatasetConverter extends ToVariantDatasetConversion[Feature, FeatureProduct, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[VariantProduct]): VariantRDD = {
    ADAMContext.featuresToVariantsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToContigsDatasetConverter extends ToContigDatasetConversion[Fragment, FragmentProduct, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    ADAMContext.fragmentsToContigsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToCoverageDatasetConverter extends ToCoverageDatasetConversion[Fragment, FragmentProduct, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.fragmentsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Fragment, FragmentProduct, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[FeatureProduct]): FeatureRDD = {
    ADAMContext.fragmentsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Fragment, FragmentProduct, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    ADAMContext.fragmentsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Fragment, FragmentProduct, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[GenotypeProduct]): GenotypeRDD = {
    ADAMContext.fragmentsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToVariantsDatasetConverter extends ToVariantDatasetConversion[Fragment, FragmentProduct, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[VariantProduct]): VariantRDD = {
    ADAMContext.fragmentsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToContigsDatasetConverter extends ToContigDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    ADAMContext.alignmentRecordsToContigsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToCoverageDatasetConverter extends ToCoverageDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.alignmentRecordsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[FeatureProduct]): FeatureRDD = {
    ADAMContext.alignmentRecordsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[FragmentProduct]): FragmentRDD = {
    ADAMContext.alignmentRecordsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[GenotypeProduct]): GenotypeRDD = {
    ADAMContext.alignmentRecordsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToVariantsDatasetConverter extends ToVariantDatasetConversion[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[VariantProduct]): VariantRDD = {
    ADAMContext.alignmentRecordsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToContigsDatasetConverter extends ToContigDatasetConversion[Genotype, GenotypeProduct, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    ADAMContext.genotypesToContigsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToCoverageDatasetConverter extends ToCoverageDatasetConversion[Genotype, GenotypeProduct, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.genotypesToCoverageDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Genotype, GenotypeProduct, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[FeatureProduct]): FeatureRDD = {
    ADAMContext.genotypesToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Genotype, GenotypeProduct, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[FragmentProduct]): FragmentRDD = {
    ADAMContext.genotypesToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Genotype, GenotypeProduct, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    ADAMContext.genotypesToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToVariantsDatasetConverter extends ToVariantDatasetConversion[Genotype, GenotypeProduct, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[VariantProduct]): VariantRDD = {
    ADAMContext.genotypesToVariantsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToContigsDatasetConverter extends ToContigDatasetConversion[Variant, VariantProduct, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    ADAMContext.variantsToContigsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToCoverageDatasetConverter extends ToCoverageDatasetConversion[Variant, VariantProduct, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.variantsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class VariantsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Variant, VariantProduct, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[FeatureProduct]): FeatureRDD = {
    ADAMContext.variantsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class VariantsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Variant, VariantProduct, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[FragmentProduct]): FragmentRDD = {
    ADAMContext.variantsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Variant, VariantProduct, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    ADAMContext.variantsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Variant, VariantProduct, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[GenotypeProduct]): GenotypeRDD = {
    ADAMContext.variantsToGenotypesDatasetConversionFn(v1, v2)
  }
}
