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
import org.bdgenomics.adam.models.Coverage
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
  GenotypeRDD
}
import org.bdgenomics.adam.sql._
import scala.reflect.runtime.universe._

trait ToContigDatasetConversion[T <: Product, U <: GenomicDataset[_, T, U]] extends GenomicDatasetConversion[T, U, NucleotideContigFragment, NucleotideContigFragmentRDD] {

  val xTag: TypeTag[NucleotideContigFragment] = typeTag[NucleotideContigFragment]
}

trait ToCoverageDatasetConversion[T <: Product, U <: GenomicDataset[_, T, U]] extends GenomicDatasetConversion[T, U, Coverage, CoverageRDD] {

  val xTag: TypeTag[Coverage] = typeTag[Coverage]
}

trait ToFeatureDatasetConversion[T <: Product, U <: GenomicDataset[_, T, U]] extends GenomicDatasetConversion[T, U, Feature, FeatureRDD] {

  val xTag: TypeTag[Feature] = typeTag[Feature]
}

trait ToFragmentDatasetConversion[T <: Product, U <: GenomicDataset[_, T, U]] extends GenomicDatasetConversion[T, U, Fragment, FragmentRDD] {

  val xTag: TypeTag[Fragment] = typeTag[Fragment]
}

trait ToAlignmentRecordDatasetConversion[T <: Product, U <: GenomicDataset[_, T, U]] extends GenomicDatasetConversion[T, U, AlignmentRecord, AlignmentRecordRDD] {

  val xTag: TypeTag[AlignmentRecord] = typeTag[AlignmentRecord]
}

trait ToGenotypeDatasetConversion[T <: Product, U <: GenomicDataset[_, T, U]] extends GenomicDatasetConversion[T, U, Genotype, GenotypeRDD] {

  val xTag: TypeTag[Genotype] = typeTag[Genotype]
}

trait ToVariantDatasetConversion[T <: Product, U <: GenomicDataset[_, T, U]] extends GenomicDatasetConversion[T, U, Variant, VariantRDD] {

  val xTag: TypeTag[Variant] = typeTag[Variant]
}

final class ContigsToCoverageDatasetConverter extends ToCoverageDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.contigsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class ContigsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[Feature]): FeatureRDD = {
    ADAMContext.contigsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class ContigsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[Fragment]): FragmentRDD = {
    ADAMContext.contigsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class ContigsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.contigsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class ContigsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[Genotype]): GenotypeRDD = {
    ADAMContext.contigsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class ContigsToVariantsDatasetConverter extends ToVariantDatasetConversion[NucleotideContigFragment, NucleotideContigFragmentRDD] {

  def call(v1: NucleotideContigFragmentRDD, v2: Dataset[Variant]): VariantRDD = {
    ADAMContext.contigsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToContigsDatasetConverter extends ToContigDatasetConversion[Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    ADAMContext.coverageToContigsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[Feature]): FeatureRDD = {
    ADAMContext.coverageToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class CoverageToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[Fragment]): FragmentRDD = {
    ADAMContext.coverageToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.coverageToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class CoverageToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[Genotype]): GenotypeRDD = {
    ADAMContext.coverageToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class CoverageToVariantsDatasetConverter extends ToVariantDatasetConversion[Coverage, CoverageRDD] {

  def call(v1: CoverageRDD, v2: Dataset[Variant]): VariantRDD = {
    ADAMContext.coverageToVariantsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToContigsDatasetConverter extends ToContigDatasetConversion[Feature, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    ADAMContext.featuresToContigsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToCoverageDatasetConverter extends ToCoverageDatasetConversion[Feature, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.featuresToCoverageDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Feature, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[Fragment]): FragmentRDD = {
    ADAMContext.featuresToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Feature, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.featuresToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Feature, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[Genotype]): GenotypeRDD = {
    ADAMContext.featuresToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class FeaturesToVariantsDatasetConverter extends ToVariantDatasetConversion[Feature, FeatureRDD] {

  def call(v1: FeatureRDD, v2: Dataset[Variant]): VariantRDD = {
    ADAMContext.featuresToVariantsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToContigsDatasetConverter extends ToContigDatasetConversion[Fragment, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    ADAMContext.fragmentsToContigsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToCoverageDatasetConverter extends ToCoverageDatasetConversion[Fragment, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.fragmentsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Fragment, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[Feature]): FeatureRDD = {
    ADAMContext.fragmentsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Fragment, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.fragmentsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Fragment, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[Genotype]): GenotypeRDD = {
    ADAMContext.fragmentsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class FragmentsToVariantsDatasetConverter extends ToVariantDatasetConversion[Fragment, FragmentRDD] {

  def call(v1: FragmentRDD, v2: Dataset[Variant]): VariantRDD = {
    ADAMContext.fragmentsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToContigsDatasetConverter extends ToContigDatasetConversion[AlignmentRecord, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    ADAMContext.alignmentRecordsToContigsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToCoverageDatasetConverter extends ToCoverageDatasetConversion[AlignmentRecord, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.alignmentRecordsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[AlignmentRecord, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[Feature]): FeatureRDD = {
    ADAMContext.alignmentRecordsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[AlignmentRecord, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[Fragment]): FragmentRDD = {
    ADAMContext.alignmentRecordsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[AlignmentRecord, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[Genotype]): GenotypeRDD = {
    ADAMContext.alignmentRecordsToGenotypesDatasetConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToVariantsDatasetConverter extends ToVariantDatasetConversion[AlignmentRecord, AlignmentRecordRDD] {

  def call(v1: AlignmentRecordRDD, v2: Dataset[Variant]): VariantRDD = {
    ADAMContext.alignmentRecordsToVariantsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToContigsDatasetConverter extends ToContigDatasetConversion[Genotype, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    ADAMContext.genotypesToContigsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToCoverageDatasetConverter extends ToCoverageDatasetConversion[Genotype, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.genotypesToCoverageDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Genotype, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[Feature]): FeatureRDD = {
    ADAMContext.genotypesToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Genotype, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[Fragment]): FragmentRDD = {
    ADAMContext.genotypesToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Genotype, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.genotypesToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class GenotypesToVariantsDatasetConverter extends ToVariantDatasetConversion[Genotype, GenotypeRDD] {

  def call(v1: GenotypeRDD, v2: Dataset[Variant]): VariantRDD = {
    ADAMContext.genotypesToVariantsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToContigsDatasetConverter extends ToContigDatasetConversion[Variant, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    ADAMContext.variantsToContigsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToCoverageDatasetConverter extends ToCoverageDatasetConversion[Variant, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[Coverage]): CoverageRDD = {
    ADAMContext.variantsToCoverageDatasetConversionFn(v1, v2)
  }
}

final class VariantsToFeaturesDatasetConverter extends ToFeatureDatasetConversion[Variant, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[Feature]): FeatureRDD = {
    ADAMContext.variantsToFeaturesDatasetConversionFn(v1, v2)
  }
}

final class VariantsToFragmentsDatasetConverter extends ToFragmentDatasetConversion[Variant, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[Fragment]): FragmentRDD = {
    ADAMContext.variantsToFragmentsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToAlignmentRecordsDatasetConverter extends ToAlignmentRecordDatasetConversion[Variant, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.variantsToAlignmentRecordsDatasetConversionFn(v1, v2)
  }
}

final class VariantsToGenotypesDatasetConverter extends ToGenotypeDatasetConversion[Variant, VariantRDD] {

  def call(v1: VariantRDD, v2: Dataset[Genotype]): GenotypeRDD = {
    ADAMContext.variantsToGenotypesDatasetConversionFn(v1, v2)
  }
}
