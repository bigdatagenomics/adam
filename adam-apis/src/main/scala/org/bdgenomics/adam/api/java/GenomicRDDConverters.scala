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
import org.bdgenomics.adam.ds.ADAMContext
import org.bdgenomics.adam.ds.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.ds.fragment.FragmentDataset
import org.bdgenomics.adam.ds.read.{ AlignmentDataset, ReadDataset }
import org.bdgenomics.adam.ds.sequence.{ SequenceDataset, SliceDataset }
import org.bdgenomics.adam.ds.variant.{
  VariantDataset,
  GenotypeDataset,
  VariantContextDataset
}
import org.bdgenomics.formats.avro._

// coverage conversion functions

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

final class CoverageToAlignmentsConverter extends Function2[CoverageDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: CoverageDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.coverageToAlignmentsConversionFn(v1, v2)
  }
}

final class CoverageToGenotypesConverter extends Function2[CoverageDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: CoverageDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.coverageToGenotypesConversionFn(v1, v2)
  }
}

final class CoverageToReadsConverter extends Function2[CoverageDataset, RDD[Read], ReadDataset] {

  def call(v1: CoverageDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.coverageToReadsConversionFn(v1, v2)
  }
}

final class CoverageToSequencesConverter extends Function2[CoverageDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: CoverageDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.coverageToSequencesConversionFn(v1, v2)
  }
}

final class CoverageToSlicesConverter extends Function2[CoverageDataset, RDD[Slice], SliceDataset] {

  def call(v1: CoverageDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.coverageToSlicesConversionFn(v1, v2)
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

// features conversion functions

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

final class FeaturesToAlignmentsConverter extends Function2[FeatureDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: FeatureDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.featuresToAlignmentsConversionFn(v1, v2)
  }
}

final class FeaturesToGenotypesConverter extends Function2[FeatureDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: FeatureDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.featuresToGenotypesConversionFn(v1, v2)
  }
}

final class FeaturesToReadsConverter extends Function2[FeatureDataset, RDD[Read], ReadDataset] {

  def call(v1: FeatureDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.featuresToReadsConversionFn(v1, v2)
  }
}

final class FeaturesToSequencesConverter extends Function2[FeatureDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: FeatureDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.featuresToSequencesConversionFn(v1, v2)
  }
}

final class FeaturesToSlicesConverter extends Function2[FeatureDataset, RDD[Slice], SliceDataset] {

  def call(v1: FeatureDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.featuresToSlicesConversionFn(v1, v2)
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

// fragments conversion functions

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

final class FragmentsToAlignmentsConverter extends Function2[FragmentDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: FragmentDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.fragmentsToAlignmentsConversionFn(v1, v2)
  }
}

final class FragmentsToGenotypesConverter extends Function2[FragmentDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: FragmentDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.fragmentsToGenotypesConversionFn(v1, v2)
  }
}

final class FragmentsToReadsConverter extends Function2[FragmentDataset, RDD[Read], ReadDataset] {

  def call(v1: FragmentDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.fragmentsToReadsConversionFn(v1, v2)
  }
}

final class FragmentsToSequencesConverter extends Function2[FragmentDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: FragmentDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.fragmentsToSequencesConversionFn(v1, v2)
  }
}

final class FragmentsToSlicesConverter extends Function2[FragmentDataset, RDD[Slice], SliceDataset] {

  def call(v1: FragmentDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.fragmentsToSlicesConversionFn(v1, v2)
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

// alignments conversion functions

final class AlignmentsToCoverageConverter extends Function2[AlignmentDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: AlignmentDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.AlignmentsToCoverageConversionFn(v1, v2)
  }
}

final class AlignmentsToFeaturesConverter extends Function2[AlignmentDataset, RDD[Feature], FeatureDataset] {

  def call(v1: AlignmentDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.AlignmentsToFeaturesConversionFn(v1, v2)
  }
}

final class AlignmentsToFragmentsConverter extends Function2[AlignmentDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: AlignmentDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.AlignmentsToFragmentsConversionFn(v1, v2)
  }
}

final class AlignmentsToAlignmentsConverter extends Function2[AlignmentDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: AlignmentDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.AlignmentsToAlignmentsConversionFn(v1, v2)
  }
}

final class AlignmentsToGenotypesConverter extends Function2[AlignmentDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: AlignmentDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.AlignmentsToGenotypesConversionFn(v1, v2)
  }
}

final class AlignmentsToReadsConverter extends Function2[AlignmentDataset, RDD[Read], ReadDataset] {

  def call(v1: AlignmentDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.AlignmentsToReadsConversionFn(v1, v2)
  }
}

final class AlignmentsToSequencesConverter extends Function2[AlignmentDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: AlignmentDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.AlignmentsToSequencesConversionFn(v1, v2)
  }
}

final class AlignmentsToSlicesConverter extends Function2[AlignmentDataset, RDD[Slice], SliceDataset] {

  def call(v1: AlignmentDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.AlignmentsToSlicesConversionFn(v1, v2)
  }
}

final class AlignmentsToVariantsConverter extends Function2[AlignmentDataset, RDD[Variant], VariantDataset] {
  def call(v1: AlignmentDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.AlignmentsToVariantsConversionFn(v1, v2)
  }
}

final class AlignmentsToVariantContextConverter extends Function2[AlignmentDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: AlignmentDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.AlignmentsToVariantContextConversionFn(v1, v2)
  }
}

// genotypes conversion functions

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

final class GenotypesToAlignmentsConverter extends Function2[GenotypeDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.genotypesToAlignmentsConversionFn(v1, v2)
  }
}

final class GenotypesToGenotypesConverter extends Function2[GenotypeDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.genotypesToGenotypesConversionFn(v1, v2)
  }
}

final class GenotypesToReadsConverter extends Function2[GenotypeDataset, RDD[Read], ReadDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.genotypesToReadsConversionFn(v1, v2)
  }
}

final class GenotypesToSequencesConverter extends Function2[GenotypeDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.genotypesToSequencesConversionFn(v1, v2)
  }
}

final class GenotypesToSlicesConverter extends Function2[GenotypeDataset, RDD[Slice], SliceDataset] {

  def call(v1: GenotypeDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.genotypesToSlicesConversionFn(v1, v2)
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

// reads conversion functions

final class ReadsToCoverageConverter extends Function2[ReadDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: ReadDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.readsToCoverageConversionFn(v1, v2)
  }
}

final class ReadsToFeaturesConverter extends Function2[ReadDataset, RDD[Feature], FeatureDataset] {

  def call(v1: ReadDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.readsToFeaturesConversionFn(v1, v2)
  }
}

final class ReadsToFragmentsConverter extends Function2[ReadDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: ReadDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.readsToFragmentsConversionFn(v1, v2)
  }
}

final class ReadsToAlignmentsConverter extends Function2[ReadDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: ReadDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.readsToAlignmentsConversionFn(v1, v2)
  }
}

final class ReadsToGenotypesConverter extends Function2[ReadDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: ReadDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.readsToGenotypesConversionFn(v1, v2)
  }
}

final class ReadsToReadsConverter extends Function2[ReadDataset, RDD[Read], ReadDataset] {

  def call(v1: ReadDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.readsToReadsConversionFn(v1, v2)
  }
}

final class ReadsToSequencesConverter extends Function2[ReadDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: ReadDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.readsToSequencesConversionFn(v1, v2)
  }
}

final class ReadsToSlicesConverter extends Function2[ReadDataset, RDD[Slice], SliceDataset] {

  def call(v1: ReadDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.readsToSlicesConversionFn(v1, v2)
  }
}

final class ReadsToVariantsConverter extends Function2[ReadDataset, RDD[Variant], VariantDataset] {

  def call(v1: ReadDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.readsToVariantsConversionFn(v1, v2)
  }
}

final class ReadsToVariantContextsConverter extends Function2[ReadDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: ReadDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.readsToVariantContextsConversionFn(v1, v2)
  }
}

// sequence conversion functions

final class SequencesToCoverageConverter extends Function2[SequenceDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: SequenceDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.sequencesToCoverageConversionFn(v1, v2)
  }
}

final class SequencesToFeaturesConverter extends Function2[SequenceDataset, RDD[Feature], FeatureDataset] {

  def call(v1: SequenceDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.sequencesToFeaturesConversionFn(v1, v2)
  }
}

final class SequencesToFragmentsConverter extends Function2[SequenceDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: SequenceDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.sequencesToFragmentsConversionFn(v1, v2)
  }
}

final class SequencesToAlignmentsConverter extends Function2[SequenceDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: SequenceDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.sequencesToAlignmentsConversionFn(v1, v2)
  }
}

final class SequencesToGenotypesConverter extends Function2[SequenceDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: SequenceDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.sequencesToGenotypesConversionFn(v1, v2)
  }
}

final class SequencesToReadsConverter extends Function2[SequenceDataset, RDD[Read], ReadDataset] {

  def call(v1: SequenceDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.sequencesToReadsConversionFn(v1, v2)
  }
}

final class SequencesToSequencesConverter extends Function2[SequenceDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: SequenceDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.sequencesToSequencesConversionFn(v1, v2)
  }
}

final class SequencesToSlicesConverter extends Function2[SequenceDataset, RDD[Slice], SliceDataset] {

  def call(v1: SequenceDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.sequencesToSlicesConversionFn(v1, v2)
  }
}

final class SequencesToVariantsConverter extends Function2[SequenceDataset, RDD[Variant], VariantDataset] {

  def call(v1: SequenceDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.sequencesToVariantsConversionFn(v1, v2)
  }
}

final class SequencesToVariantContextsConverter extends Function2[SequenceDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: SequenceDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.sequencesToVariantContextsConversionFn(v1, v2)
  }
}

// slice conversion functions

final class SlicesToCoverageConverter extends Function2[SliceDataset, RDD[Coverage], CoverageDataset] {

  def call(v1: SliceDataset, v2: RDD[Coverage]): CoverageDataset = {
    ADAMContext.slicesToCoverageConversionFn(v1, v2)
  }
}

final class SlicesToFeaturesConverter extends Function2[SliceDataset, RDD[Feature], FeatureDataset] {

  def call(v1: SliceDataset, v2: RDD[Feature]): FeatureDataset = {
    ADAMContext.slicesToFeaturesConversionFn(v1, v2)
  }
}

final class SlicesToFragmentsConverter extends Function2[SliceDataset, RDD[Fragment], FragmentDataset] {

  def call(v1: SliceDataset, v2: RDD[Fragment]): FragmentDataset = {
    ADAMContext.slicesToFragmentsConversionFn(v1, v2)
  }
}

final class SlicesToAlignmentsConverter extends Function2[SliceDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: SliceDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.slicesToAlignmentsConversionFn(v1, v2)
  }
}

final class SlicesToGenotypesConverter extends Function2[SliceDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: SliceDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.slicesToGenotypesConversionFn(v1, v2)
  }
}

final class SlicesToReadsConverter extends Function2[SliceDataset, RDD[Read], ReadDataset] {

  def call(v1: SliceDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.slicesToReadsConversionFn(v1, v2)
  }
}

final class SlicesToSequencesConverter extends Function2[SliceDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: SliceDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.slicesToSequencesConversionFn(v1, v2)
  }
}

final class SlicesToSlicesConverter extends Function2[SliceDataset, RDD[Slice], SliceDataset] {

  def call(v1: SliceDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.slicesToSlicesConversionFn(v1, v2)
  }
}

final class SlicesToVariantsConverter extends Function2[SliceDataset, RDD[Variant], VariantDataset] {

  def call(v1: SliceDataset, v2: RDD[Variant]): VariantDataset = {
    ADAMContext.slicesToVariantsConversionFn(v1, v2)
  }
}

final class SlicesToVariantContextsConverter extends Function2[SliceDataset, RDD[VariantContext], VariantContextDataset] {

  def call(v1: SliceDataset, v2: RDD[VariantContext]): VariantContextDataset = {
    ADAMContext.slicesToVariantContextsConversionFn(v1, v2)
  }
}

// variants conversion functions

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

final class VariantsToAlignmentsConverter extends Function2[VariantDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: VariantDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.variantsToAlignmentsConversionFn(v1, v2)
  }
}

final class VariantsToGenotypesConverter extends Function2[VariantDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: VariantDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.variantsToGenotypesConversionFn(v1, v2)
  }
}

final class VariantsToReadsConverter extends Function2[VariantDataset, RDD[Read], ReadDataset] {

  def call(v1: VariantDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.variantsToReadsConversionFn(v1, v2)
  }
}

final class VariantsToSequencesConverter extends Function2[VariantDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: VariantDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.variantsToSequencesConversionFn(v1, v2)
  }
}

final class VariantsToSlicesConverter extends Function2[VariantDataset, RDD[Slice], SliceDataset] {

  def call(v1: VariantDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.variantsToSlicesConversionFn(v1, v2)
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

// variant contexts conversion functions

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

final class VariantContextsToAlignmentsConverter extends Function2[VariantContextDataset, RDD[Alignment], AlignmentDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Alignment]): AlignmentDataset = {
    ADAMContext.variantContextsToAlignmentsConversionFn(v1, v2)
  }
}

final class VariantContextsToGenotypesConverter extends Function2[VariantContextDataset, RDD[Genotype], GenotypeDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Genotype]): GenotypeDataset = {
    ADAMContext.variantContextsToGenotypesConversionFn(v1, v2)
  }
}

final class VariantContextsToReadsConverter extends Function2[VariantContextDataset, RDD[Read], ReadDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Read]): ReadDataset = {
    ADAMContext.variantContextsToReadsConversionFn(v1, v2)
  }
}

final class VariantContextsToSequencesConverter extends Function2[VariantContextDataset, RDD[Sequence], SequenceDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Sequence]): SequenceDataset = {
    ADAMContext.variantContextsToSequencesConversionFn(v1, v2)
  }
}

final class VariantContextsToSlicesConverter extends Function2[VariantContextDataset, RDD[Slice], SliceDataset] {

  def call(v1: VariantContextDataset, v2: RDD[Slice]): SliceDataset = {
    ADAMContext.variantContextsToSlicesConversionFn(v1, v2)
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
