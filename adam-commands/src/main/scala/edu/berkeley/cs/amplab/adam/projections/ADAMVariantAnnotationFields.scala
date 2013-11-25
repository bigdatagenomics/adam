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

object ADAMVariantAnnotations extends Enumeration {
  val ADAMVariantDomain = Value

  /* This map serves to connect variant annotations with their respective
   * file extensions.
   */
  val fileExtensions = Map(ADAMVariantDomain -> ".vd")
}
        
/**
 * This enumeration exist in order to reduce typo errors in the code. It needs to be kept
 * in sync with any changes to any ADAM variant annotation objects.
 *
 * This enumeration is necessary because Parquet needs the field string names
 * for predicates and projections.
 */

object ADAMVariantDomainField extends Enumeration {
  val position,
  variant,
  inDbSNP,
  inHM2,
  inHM3,
  in1000G = Value
}
