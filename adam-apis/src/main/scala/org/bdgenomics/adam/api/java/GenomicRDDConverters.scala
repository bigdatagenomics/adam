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

import org.apache.spark.api.java.function.Function2
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  Coverage,
  VariantContext
}
import org.bdgenomics.adam.rdd.{ ADAMContext, GenomicRDD }
import org.bdgenomics.adam.rdd.feature.{ CoverageRDD, FeatureRDD }
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.sequence.SliceRDD
import org.bdgenomics.adam.rdd.variant.{
  VariantRDD,
  GenotypeRDD,
  VariantContextRDD
}
import org.bdgenomics.formats.avro._

sealed trait SameTypeConversion[T, U <: GenomicRDD[T, U]] extends Function2[U, RDD[T], U] {

  def call(v1: U, v2: RDD[T]): U = {
    ADAMContext.sameTypeConversionFn(v1, v2)
  }
}

final class SlicesToSlicesConverter extends SameTypeConversion[Slice, SliceRDD] {
}

final class SlicesToCoverageConverter extends Function2[SliceRDD, RDD[Coverage], CoverageRDD] {

  def call(v1: SliceRDD, v2: RDD[Coverage]): CoverageRDD = {
    ADAMContext.slicesToCoverageConversionFn(v1, v2)
  }
}

final class SlicesToFeaturesConverter extends Function2[SliceRDD, RDD[Feature], FeatureRDD] {

  def call(v1: SliceRDD, v2: RDD[Feature]): FeatureRDD = {
    ADAMContext.slicesToFeaturesConversionFn(v1, v2)
  }
}

final class SlicesToFragmentsConverter extends Function2[SliceRDD, RDD[Fragment], FragmentRDD] {

  def call(v1: SliceRDD, v2: RDD[Fragment]): FragmentRDD = {
    ADAMContext.slicesToFragmentsConversionFn(v1, v2)
  }
}

final class SlicesToAlignmentRecordsConverter extends Function2[SliceRDD, RDD[AlignmentRecord], AlignmentRecordRDD] {

  def call(v1: SliceRDD, v2: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.slicesToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class SlicesToGenotypesConverter extends Function2[SliceRDD, RDD[Genotype], GenotypeRDD] {

  def call(v1: SliceRDD, v2: RDD[Genotype]): GenotypeRDD = {
    ADAMContext.slicesToGenotypesConversionFn(v1, v2)
  }
}

final class SlicesToVariantsConverter extends Function2[SliceRDD, RDD[Variant], VariantRDD] {

  def call(v1: SliceRDD, v2: RDD[Variant]): VariantRDD = {
    ADAMContext.slicesToVariantsConversionFn(v1, v2)
  }
}

final class SlicesToVariantContextsConverter extends Function2[SliceRDD, RDD[VariantContext], VariantContextRDD] {

  def call(v1: SliceRDD, v2: RDD[VariantContext]): VariantContextRDD = {
    ADAMContext.slicesToVariantContextConversionFn(v1, v2)
  }
}

final class CoverageToSlicesConverter extends Function2[CoverageRDD, RDD[Slice], SliceRDD] {

  def call(v1: CoverageRDD, v2: RDD[Slice]): SliceRDD = {
    ADAMContext.coverageToSlicesConversionFn(v1, v2)
  }
}

final class CoverageToCoverageConverter extends SameTypeConversion[Coverage, CoverageRDD] {
}

final class CoverageToFeaturesConverter extends Function2[CoverageRDD, RDD[Feature], FeatureRDD] {

  def call(v1: CoverageRDD, v2: RDD[Feature]): FeatureRDD = {
    ADAMContext.coverageToFeaturesConversionFn(v1, v2)
  }
}

final class CoverageToFragmentsConverter extends Function2[CoverageRDD, RDD[Fragment], FragmentRDD] {

  def call(v1: CoverageRDD, v2: RDD[Fragment]): FragmentRDD = {
    ADAMContext.coverageToFragmentsConversionFn(v1, v2)
  }
}

final class CoverageToAlignmentRecordsConverter extends Function2[CoverageRDD, RDD[AlignmentRecord], AlignmentRecordRDD] {

  def call(v1: CoverageRDD, v2: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.coverageToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class CoverageToGenotypesConverter extends Function2[CoverageRDD, RDD[Genotype], GenotypeRDD] {

  def call(v1: CoverageRDD, v2: RDD[Genotype]): GenotypeRDD = {
    ADAMContext.coverageToGenotypesConversionFn(v1, v2)
  }
}

final class CoverageToVariantsConverter extends Function2[CoverageRDD, RDD[Variant], VariantRDD] {

  def call(v1: CoverageRDD, v2: RDD[Variant]): VariantRDD = {
    ADAMContext.coverageToVariantsConversionFn(v1, v2)
  }
}

final class CoverageToVariantContextConverter extends Function2[CoverageRDD, RDD[VariantContext], VariantContextRDD] {

  def call(v1: CoverageRDD, v2: RDD[VariantContext]): VariantContextRDD = {
    ADAMContext.coverageToVariantContextConversionFn(v1, v2)
  }
}

final class FeaturesToSlicesConverter extends Function2[FeatureRDD, RDD[Slice], SliceRDD] {

  def call(v1: FeatureRDD, v2: RDD[Slice]): SliceRDD = {
    ADAMContext.featuresToSlicesConversionFn(v1, v2)
  }
}

final class FeaturesToCoverageConverter extends Function2[FeatureRDD, RDD[Coverage], CoverageRDD] {

  def call(v1: FeatureRDD, v2: RDD[Coverage]): CoverageRDD = {
    ADAMContext.featuresToCoverageConversionFn(v1, v2)
  }
}

final class FeaturesToFeatureConverter extends SameTypeConversion[Feature, FeatureRDD] {
}

final class FeaturesToFragmentsConverter extends Function2[FeatureRDD, RDD[Fragment], FragmentRDD] {

  def call(v1: FeatureRDD, v2: RDD[Fragment]): FragmentRDD = {
    ADAMContext.featuresToFragmentsConversionFn(v1, v2)
  }
}

final class FeaturesToAlignmentRecordsConverter extends Function2[FeatureRDD, RDD[AlignmentRecord], AlignmentRecordRDD] {

  def call(v1: FeatureRDD, v2: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.featuresToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class FeaturesToGenotypesConverter extends Function2[FeatureRDD, RDD[Genotype], GenotypeRDD] {

  def call(v1: FeatureRDD, v2: RDD[Genotype]): GenotypeRDD = {
    ADAMContext.featuresToGenotypesConversionFn(v1, v2)
  }
}

final class FeaturesToVariantsConverter extends Function2[FeatureRDD, RDD[Variant], VariantRDD] {

  def call(v1: FeatureRDD, v2: RDD[Variant]): VariantRDD = {
    ADAMContext.featuresToVariantsConversionFn(v1, v2)
  }
}

final class FeaturesToVariantContextConverter extends Function2[FeatureRDD, RDD[VariantContext], VariantContextRDD] {

  def call(v1: FeatureRDD, v2: RDD[VariantContext]): VariantContextRDD = {
    ADAMContext.featuresToVariantContextConversionFn(v1, v2)
  }
}

final class FragmentsToSlicesConverter extends Function2[FragmentRDD, RDD[Slice], SliceRDD] {

  def call(v1: FragmentRDD, v2: RDD[Slice]): SliceRDD = {
    ADAMContext.fragmentsToSlicesConversionFn(v1, v2)
  }
}

final class FragmentsToCoverageConverter extends Function2[FragmentRDD, RDD[Coverage], CoverageRDD] {

  def call(v1: FragmentRDD, v2: RDD[Coverage]): CoverageRDD = {
    ADAMContext.fragmentsToCoverageConversionFn(v1, v2)
  }
}

final class FragmentsToFeaturesConverter extends Function2[FragmentRDD, RDD[Feature], FeatureRDD] {

  def call(v1: FragmentRDD, v2: RDD[Feature]): FeatureRDD = {
    ADAMContext.fragmentsToFeaturesConversionFn(v1, v2)
  }
}

final class FragmentsToFragmentConverter extends SameTypeConversion[Fragment, FragmentRDD] {
}

final class FragmentsToAlignmentRecordsConverter extends Function2[FragmentRDD, RDD[AlignmentRecord], AlignmentRecordRDD] {

  def call(v1: FragmentRDD, v2: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.fragmentsToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class FragmentsToGenotypesConverter extends Function2[FragmentRDD, RDD[Genotype], GenotypeRDD] {

  def call(v1: FragmentRDD, v2: RDD[Genotype]): GenotypeRDD = {
    ADAMContext.fragmentsToGenotypesConversionFn(v1, v2)
  }
}

final class FragmentsToVariantsConverter extends Function2[FragmentRDD, RDD[Variant], VariantRDD] {

  def call(v1: FragmentRDD, v2: RDD[Variant]): VariantRDD = {
    ADAMContext.fragmentsToVariantsConversionFn(v1, v2)
  }
}

final class FragmentsToVariantContextConverter extends Function2[FragmentRDD, RDD[VariantContext], VariantContextRDD] {

  def call(v1: FragmentRDD, v2: RDD[VariantContext]): VariantContextRDD = {
    ADAMContext.fragmentsToVariantContextConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToSlicesConverter extends Function2[AlignmentRecordRDD, RDD[Slice], SliceRDD] {

  def call(v1: AlignmentRecordRDD, v2: RDD[Slice]): SliceRDD = {
    ADAMContext.alignmentRecordsToSlicesConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToCoverageConverter extends Function2[AlignmentRecordRDD, RDD[Coverage], CoverageRDD] {

  def call(v1: AlignmentRecordRDD, v2: RDD[Coverage]): CoverageRDD = {
    ADAMContext.alignmentRecordsToCoverageConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFeaturesConverter extends Function2[AlignmentRecordRDD, RDD[Feature], FeatureRDD] {

  def call(v1: AlignmentRecordRDD, v2: RDD[Feature]): FeatureRDD = {
    ADAMContext.alignmentRecordsToFeaturesConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFragmentsConverter extends Function2[AlignmentRecordRDD, RDD[Fragment], FragmentRDD] {

  def call(v1: AlignmentRecordRDD, v2: RDD[Fragment]): FragmentRDD = {
    ADAMContext.alignmentRecordsToFragmentsConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToAlignmentRecordsConverter extends SameTypeConversion[AlignmentRecord, AlignmentRecordRDD] {
}

final class AlignmentRecordsToGenotypesConverter extends Function2[AlignmentRecordRDD, RDD[Genotype], GenotypeRDD] {

  def call(v1: AlignmentRecordRDD, v2: RDD[Genotype]): GenotypeRDD = {
    ADAMContext.alignmentRecordsToGenotypesConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToVariantsConverter extends Function2[AlignmentRecordRDD, RDD[Variant], VariantRDD] {

  def call(v1: AlignmentRecordRDD, v2: RDD[Variant]): VariantRDD = {
    ADAMContext.alignmentRecordsToVariantsConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToVariantContextConverter extends Function2[AlignmentRecordRDD, RDD[VariantContext], VariantContextRDD] {

  def call(v1: AlignmentRecordRDD, v2: RDD[VariantContext]): VariantContextRDD = {
    ADAMContext.alignmentRecordsToVariantContextConversionFn(v1, v2)
  }
}

final class GenotypesToSlicesConverter extends Function2[GenotypeRDD, RDD[Slice], SliceRDD] {

  def call(v1: GenotypeRDD, v2: RDD[Slice]): SliceRDD = {
    ADAMContext.genotypesToSlicesConversionFn(v1, v2)
  }
}

final class GenotypesToCoverageConverter extends Function2[GenotypeRDD, RDD[Coverage], CoverageRDD] {

  def call(v1: GenotypeRDD, v2: RDD[Coverage]): CoverageRDD = {
    ADAMContext.genotypesToCoverageConversionFn(v1, v2)
  }
}

final class GenotypesToFeaturesConverter extends Function2[GenotypeRDD, RDD[Feature], FeatureRDD] {

  def call(v1: GenotypeRDD, v2: RDD[Feature]): FeatureRDD = {
    ADAMContext.genotypesToFeaturesConversionFn(v1, v2)
  }
}

final class GenotypesToFragmentsConverter extends Function2[GenotypeRDD, RDD[Fragment], FragmentRDD] {

  def call(v1: GenotypeRDD, v2: RDD[Fragment]): FragmentRDD = {
    ADAMContext.genotypesToFragmentsConversionFn(v1, v2)
  }
}

final class GenotypesToAlignmentRecordsConverter extends Function2[GenotypeRDD, RDD[AlignmentRecord], AlignmentRecordRDD] {

  def call(v1: GenotypeRDD, v2: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.genotypesToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class GenotypesToGenotypesConverter extends SameTypeConversion[Genotype, GenotypeRDD] {
}

final class GenotypesToVariantsConverter extends Function2[GenotypeRDD, RDD[Variant], VariantRDD] {

  def call(v1: GenotypeRDD, v2: RDD[Variant]): VariantRDD = {
    ADAMContext.genotypesToVariantsConversionFn(v1, v2)
  }
}

final class GenotypesToVariantContextConverter extends Function2[GenotypeRDD, RDD[VariantContext], VariantContextRDD] {

  def call(v1: GenotypeRDD, v2: RDD[VariantContext]): VariantContextRDD = {
    ADAMContext.genotypesToVariantContextConversionFn(v1, v2)
  }
}

final class VariantsToSlicesConverter extends Function2[VariantRDD, RDD[Slice], SliceRDD] {

  def call(v1: VariantRDD, v2: RDD[Slice]): SliceRDD = {
    ADAMContext.variantsToSlicesConversionFn(v1, v2)
  }
}

final class VariantsToCoverageConverter extends Function2[VariantRDD, RDD[Coverage], CoverageRDD] {

  def call(v1: VariantRDD, v2: RDD[Coverage]): CoverageRDD = {
    ADAMContext.variantsToCoverageConversionFn(v1, v2)
  }
}

final class VariantsToFeaturesConverter extends Function2[VariantRDD, RDD[Feature], FeatureRDD] {

  def call(v1: VariantRDD, v2: RDD[Feature]): FeatureRDD = {
    ADAMContext.variantsToFeaturesConversionFn(v1, v2)
  }
}

final class VariantsToFragmentsConverter extends Function2[VariantRDD, RDD[Fragment], FragmentRDD] {

  def call(v1: VariantRDD, v2: RDD[Fragment]): FragmentRDD = {
    ADAMContext.variantsToFragmentsConversionFn(v1, v2)
  }
}

final class VariantsToAlignmentRecordsConverter extends Function2[VariantRDD, RDD[AlignmentRecord], AlignmentRecordRDD] {

  def call(v1: VariantRDD, v2: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.variantsToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class VariantsToGenotypesConverter extends Function2[VariantRDD, RDD[Genotype], GenotypeRDD] {

  def call(v1: VariantRDD, v2: RDD[Genotype]): GenotypeRDD = {
    ADAMContext.variantsToGenotypesConversionFn(v1, v2)
  }
}

final class VariantsToVariantsConverter extends SameTypeConversion[Variant, VariantRDD] {
}

final class VariantsToVariantContextConverter extends Function2[VariantRDD, RDD[VariantContext], VariantContextRDD] {

  def call(v1: VariantRDD, v2: RDD[VariantContext]): VariantContextRDD = {
    ADAMContext.variantsToVariantContextConversionFn(v1, v2)
  }
}

final class VariantContextsToSlicesConverter extends Function2[VariantContextRDD, RDD[Slice], SliceRDD] {

  def call(v1: VariantContextRDD, v2: RDD[Slice]): SliceRDD = {
    ADAMContext.variantContextsToSlicesConversionFn(v1, v2)
  }
}

final class VariantContextsToCoverageConverter extends Function2[VariantContextRDD, RDD[Coverage], CoverageRDD] {

  def call(v1: VariantContextRDD, v2: RDD[Coverage]): CoverageRDD = {
    ADAMContext.variantContextsToCoverageConversionFn(v1, v2)
  }
}

final class VariantContextsToFeaturesConverter extends Function2[VariantContextRDD, RDD[Feature], FeatureRDD] {

  def call(v1: VariantContextRDD, v2: RDD[Feature]): FeatureRDD = {
    ADAMContext.variantContextsToFeaturesConversionFn(v1, v2)
  }
}

final class VariantContextsToFragmentsConverter extends Function2[VariantContextRDD, RDD[Fragment], FragmentRDD] {

  def call(v1: VariantContextRDD, v2: RDD[Fragment]): FragmentRDD = {
    ADAMContext.variantContextsToFragmentsConversionFn(v1, v2)
  }
}

final class VariantContextsToAlignmentRecordsConverter extends Function2[VariantContextRDD, RDD[AlignmentRecord], AlignmentRecordRDD] {

  def call(v1: VariantContextRDD, v2: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    ADAMContext.variantContextsToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class VariantContextsToGenotypesConverter extends Function2[VariantContextRDD, RDD[Genotype], GenotypeRDD] {

  def call(v1: VariantContextRDD, v2: RDD[Genotype]): GenotypeRDD = {
    ADAMContext.variantContextsToGenotypesConversionFn(v1, v2)
  }
}

final class VariantContextsToVariantsConverter extends Function2[VariantContextRDD, RDD[Variant], VariantRDD] {

  def call(v1: VariantContextRDD, v2: RDD[Variant]): VariantRDD = {
    ADAMContext.variantContextsToVariantsConversionFn(v1, v2)
  }
}

final class VariantContextsToVariantContextConverter extends SameTypeConversion[VariantContext, VariantContextRDD] {
}
