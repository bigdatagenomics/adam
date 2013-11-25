/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.projections

/**
 * This enumeration exist in order to reduce typo errors in the code. It needs to be kept
 * in sync with any changes to ADAMGenotype.
 *
 * This enumeration is necessary because Parquet needs the field string names
 * for predicates and projections.
 */
object ADAMGenotypeField extends Enumeration {
  val referenceId,
  referenceName,
  position,
  sampleId,
  ploidy,
  haplotypeNumber,
  alleleVariantType,
  allele,
  isReference,
  referenceAllele,
  expectedAlleleDosage,
  genotypeQuality,
  depth,
  phredLikelihoods,
  phredPosteriorLikelihoods,
  ploidyStateGenotypeLikelihoods,
  haplotypeQuality,
  rmsBaseQuality,
  rmsMappingQuality,
  readsMappedForwardStrand,
  readsMappedMapQ0,
  isPhased,
  isPhaseSwitch,
  phaseSetId,
  phaseQuality,
  svType, 
  svLength,
  svIsPrecise,
  svEnd,
  svConfidenceIntervalStartLow,
  svConfidenceIntervalStartHigh,
  svConfidenceIntervalEndLow,
  svConfidenceIntervalEndHigh = Value
}
