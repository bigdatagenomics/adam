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
package org.bdgenomics.adam.rich

import org.bdgenomics.formats.avro.ADAMVariant

object RichADAMVariant {
  implicit def variantToRichVariant(variant: ADAMVariant): RichADAMVariant = new RichADAMVariant(variant)
  implicit def richVariantToVariant(variant: RichADAMVariant): ADAMVariant = variant.variant
}

class RichADAMVariant(val variant: ADAMVariant) {
  def isSingleNucleotideVariant() = {
    variant.getReferenceAllele.length == 1 && variant.getVariantAllele.length == 1
  }

  def isMultipleNucleotideVariant() = {
    !isSingleNucleotideVariant && variant.getReferenceAllele.length == variant.getVariantAllele.length
  }

  def isInsertion() = variant.getReferenceAllele.length < variant.getVariantAllele.length

  def isDeletion() = variant.getReferenceAllele.length > variant.getVariantAllele.length

  override def hashCode = variant.hashCode

  override def equals(o: Any) = o match {
    case that: RichADAMVariant => variant.equals(that.variant)
    case _                     => false
  }

}
