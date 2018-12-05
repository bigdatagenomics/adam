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
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentDataset
import org.bdgenomics.adam.rdd.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset
import org.bdgenomics.adam.rdd.variant.{
  VariantDataset,
  GenotypeDataset,
  VariantContextDataset
}
import org.bdgenomics.formats.avro._

final class ContigsToContigsConverter extends Function2[NucleotideContigFragmentDataset, RDD[NucleotideContigFragment], NucleotideContigFragmentDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    ADAMContext.contigsToContigsConversionFn(v1, v2)
  }
}

final class ContigsToCoverageConverter extends Function2[NucleotideContigFragmentDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.contigsToCoverageConversionFn(v1, v2)
  }
}

final class ContigsToFeaturesConverter extends Function2[NucleotideContigFragmentDataset, RDD[Feature], FeatureDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.contigsToFeaturesConversionFn(v1, v2)
  }
}

final class ContigsToFragmentsConverter extends Function2[NucleotideContigFragmentDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.contigsToFragmentsConversionFn(v1, v2)
  }
}

final class ContigsToAlignmentRecordsConverter extends Function2[NucleotideContigFragmentDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.contigsToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class ContigsToGenotypesConverter extends Function2[NucleotideContigFragmentDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.contigsToGenotypesConversionFn(v1, v2)
  }
}

final class ContigsToVariantsConverter extends Function2[NucleotideContigFragmentDataset, RDD[Variant], VariantDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.contigsToVariantsConversionFn(v1, v2)
  }
}

final class ContigsToVariantContextsConverter extends Function2[NucleotideContigFragmentDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: NucleotideContigFragmentDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.contigsToVariantContextConversionFn(v1, v2)
  }
}

final class CoverageToContigsConverter extends Function2[CoverageDataset, RDD[NucleotideContigFragment], NucleotideContigFragmentDataset] {

  def call(v1: CoverageDataset, v2: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    ADAMContext.coverageToContigsConversionFn(v1, v2)
  }
}

final class CoverageToCoverageConverter extends Function2[CoverageDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: CoverageDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.coverageToCoverageConversionFn(v1, v2)
  }
}

final class CoverageToFeaturesConverter extends Function2[CoverageDataset, RDD[Feature], FeatureDataset] {

  def call(v1: CoverageDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.coverageToFeaturesConversionFn(v1, v2)
  }
}

final class CoverageToFragmentsConverter extends Function2[CoverageDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: CoverageDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.coverageToFragmentsConversionFn(v1, v2)
  }
}

final class CoverageToAlignmentRecordsConverter extends Function2[CoverageDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: CoverageDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.coverageToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class CoverageToGenotypesConverter extends Function2[CoverageDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: CoverageDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.coverageToGenotypesConversionFn(v1, v2)
  }
}

final class CoverageToVariantsConverter extends Function2[CoverageDataset, RDD[Variant], VariantDataset] {

  def call(v1: CoverageDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.coverageToVariantsConversionFn(v1, v2)
  }
}

final class CoverageToVariantContextConverter extends Function2[CoverageDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: CoverageDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.coverageToVariantContextConversionFn(v1, v2)
  }
}

final class FeaturesToContigsConverter extends Function2[FeatureDataset, RDD[NucleotideContigFragment], NucleotideContigFragmentDataset] {

  def call(v1: FeatureDataset, v2: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    ADAMContext.featuresToContigsConversionFn(v1, v2)
  }
}

final class FeaturesToCoverageConverter extends Function2[FeatureDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: FeatureDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.featuresToCoverageConversionFn(v1, v2)
  }
}

final class FeaturesToFeatureConverter extends Function2[FeatureDataset, RDD[Feature], FeatureDataset] {

  def call(v1: FeatureDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.featuresToFeaturesConversionFn(v1, v2)
  }
}

final class FeaturesToFragmentsConverter extends Function2[FeatureDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: FeatureDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.featuresToFragmentsConversionFn(v1, v2)
  }
}

final class FeaturesToAlignmentRecordsConverter extends Function2[FeatureDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: FeatureDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.featuresToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class FeaturesToGenotypesConverter extends Function2[FeatureDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: FeatureDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.featuresToGenotypesConversionFn(v1, v2)
  }
}

final class FeaturesToVariantsConverter extends Function2[FeatureDataset, RDD[Variant], VariantDataset] {

  def call(v1: FeatureDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.featuresToVariantsConversionFn(v1, v2)
  }
}

final class FeaturesToVariantContextConverter extends Function2[FeatureDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: FeatureDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.featuresToVariantContextConversionFn(v1, v2)
  }
}

final class FragmentsToContigsConverter extends Function2[FragmentDataset, RDD[NucleotideContigFragment], NucleotideContigFragmentDataset] {

  def call(v1: FragmentDataset, v2: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    ADAMContext.fragmentsToContigsConversionFn(v1, v2)
  }
}

final class FragmentsToCoverageConverter extends Function2[FragmentDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: FragmentDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.fragmentsToCoverageConversionFn(v1, v2)
  }
}

final class FragmentsToFeaturesConverter extends Function2[FragmentDataset, RDD[Feature], FeatureDataset] {

  def call(v1: FragmentDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.fragmentsToFeaturesConversionFn(v1, v2)
  }
}

final class FragmentsToFragmentsConverter extends Function2[FragmentDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: FragmentDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.fragmentsToFragmentsConversionFn(v1, v2)
  }
}

final class FragmentsToAlignmentRecordsConverter extends Function2[FragmentDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: FragmentDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.fragmentsToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class FragmentsToGenotypesConverter extends Function2[FragmentDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: FragmentDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.fragmentsToGenotypesConversionFn(v1, v2)
  }
}

final class FragmentsToVariantsConverter extends Function2[FragmentDataset, RDD[Variant], VariantDataset] {

  def call(v1: FragmentDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.fragmentsToVariantsConversionFn(v1, v2)
  }
}

final class FragmentsToVariantContextConverter extends Function2[FragmentDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: FragmentDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.fragmentsToVariantContextConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToContigsConverter extends Function2[AlignmentRecordDataset, RDD[NucleotideContigFragment], NucleotideContigFragmentDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    ADAMContext.alignmentRecordsToContigsConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToCoverageConverter extends Function2[AlignmentRecordDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.alignmentRecordsToCoverageConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFeaturesConverter extends Function2[AlignmentRecordDataset, RDD[Feature], FeatureDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.alignmentRecordsToFeaturesConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToFragmentsConverter extends Function2[AlignmentRecordDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.alignmentRecordsToFragmentsConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToAlignmentRecordsConverter extends Function2[AlignmentRecordDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.alignmentRecordsToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToGenotypesConverter extends Function2[AlignmentRecordDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.alignmentRecordsToGenotypesConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToVariantsConverter extends Function2[AlignmentRecordDataset, RDD[Variant], VariantDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.alignmentRecordsToVariantsConversionFn(v1, v2)
  }
}

final class AlignmentRecordsToVariantContextConverter extends Function2[AlignmentRecordDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.alignmentRecordsToVariantContextConversionFn(v1, v2)
  }
}

final class GenotypesToContigsConverter extends Function2[GenotypeDataset, RDD[NucleotideContigFragment], NucleotideContigFragmentDataset] {

  def call(v1: GenotypeDataset, v2: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    ADAMContext.genotypesToContigsConversionFn(v1, v2)
  }
}

final class GenotypesToCoverageConverter extends Function2[GenotypeDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.genotypesToCoverageConversionFn(v1, v2)
  }
}

final class GenotypesToFeaturesConverter extends Function2[GenotypeDataset, RDD[Feature], FeatureDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.genotypesToFeaturesConversionFn(v1, v2)
  }
}

final class GenotypesToFragmentsConverter extends Function2[GenotypeDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.genotypesToFragmentsConversionFn(v1, v2)
  }
}

final class GenotypesToAlignmentRecordsConverter extends Function2[GenotypeDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: GenotypeDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.genotypesToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class GenotypesToGenotypesConverter extends Function2[GenotypeDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.genotypesToGenotypesConversionFn(v1, v2)
  }
}

final class GenotypesToVariantsConverter extends Function2[GenotypeDataset, RDD[Variant], VariantDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.genotypesToVariantsConversionFn(v1, v2)
  }
}

final class GenotypesToVariantContextConverter extends Function2[GenotypeDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: GenotypeDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.genotypesToVariantContextConversionFn(v1, v2)
  }
}

final class VariantsToContigsConverter extends Function2[VariantDataset, RDD[NucleotideContigFragment], NucleotideContigFragmentDataset] {

  def call(v1: VariantDataset, v2: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    ADAMContext.variantsToContigsConversionFn(v1, v2)
  }
}

final class VariantsToCoverageConverter extends Function2[VariantDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: VariantDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.variantsToCoverageConversionFn(v1, v2)
  }
}

final class VariantsToFeaturesConverter extends Function2[VariantDataset, RDD[Feature], FeatureDataset] {

  def call(v1: VariantDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.variantsToFeaturesConversionFn(v1, v2)
  }
}

final class VariantsToFragmentsConverter extends Function2[VariantDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: VariantDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.variantsToFragmentsConversionFn(v1, v2)
  }
}

final class VariantsToAlignmentRecordsConverter extends Function2[VariantDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: VariantDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.variantsToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class VariantsToGenotypesConverter extends Function2[VariantDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: VariantDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.variantsToGenotypesConversionFn(v1, v2)
  }
}

final class VariantsToVariantsConverter extends Function2[VariantDataset, RDD[Variant], VariantDataset] {

  def call(v1: VariantDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.variantsToVariantsConversionFn(v1, v2)
  }
}

final class VariantsToVariantContextConverter extends Function2[VariantDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: VariantDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.variantsToVariantContextConversionFn(v1, v2)
  }
}

final class VariantContextsToContigsConverter extends Function2[VariantContextDataset, RDD[NucleotideContigFragment], NucleotideContigFragmentDataset] {

  def call(v1: VariantContextDataset, v2: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    ADAMContext.variantContextsToContigsConversionFn(v1, v2)
  }
}

final class VariantContextsToCoverageConverter extends Function2[VariantContextDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.variantContextsToCoverageConversionFn(v1, v2)
  }
}

final class VariantContextsToFeaturesConverter extends Function2[VariantContextDataset, RDD[Feature], FeatureDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.variantContextsToFeaturesConversionFn(v1, v2)
  }
}

final class VariantContextsToFragmentsConverter extends Function2[VariantContextDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.variantContextsToFragmentsConversionFn(v1, v2)
  }
}

final class VariantContextsToAlignmentRecordsConverter extends Function2[VariantContextDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: VariantContextDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.variantContextsToAlignmentRecordsConversionFn(v1, v2)
  }
}

final class VariantContextsToGenotypesConverter extends Function2[VariantContextDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.variantContextsToGenotypesConversionFn(v1, v2)
  }
}

final class VariantContextsToVariantsConverter extends Function2[VariantContextDataset, RDD[Variant], VariantDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.variantContextsToVariantsConversionFn(v1, v2)
  }
}

final class VariantContextsToVariantContextConverter extends Function2[VariantContextDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: VariantContextDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.variantContextsToVariantContextsConversionFn(v1, v2)
  }
}
