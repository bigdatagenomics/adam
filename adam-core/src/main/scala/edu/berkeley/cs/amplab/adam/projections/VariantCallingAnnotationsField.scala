/*
 * Copyright (c) 2014. Mount Sinai School of Medicine
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

import edu.berkeley.cs.amplab.adam.avro.VariantCallingAnnotations

class VariantCallingAnnotationsField  extends FieldEnumeration(VariantCallingAnnotations.SCHEMA$) {

  val  readDepth,
  downsampled,
  baseQRankSum,
  clippingRankSum,
  haplotypeScore,
  inbreedingCoefficient,
  alleleCountMLE,
  alleleFrequencyMLE,
  rmsMapQ,
  mapq0Reads,
  mqRankSum,
  usedForNegativeTrainingSet,
  usedForPositiveTrainingSet,
  variantQualityByDepth,
  readPositionRankSum,
  vqslod,
  culprit,
  variantCallErrorProbability,
  variantIsPassing,
  variantFilters
  = SchemaValue
}
