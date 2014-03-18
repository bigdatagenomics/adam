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

package edu.berkeley.cs.amplab.adam.rich

import edu.berkeley.cs.amplab.adam.avro.{ADAMContig, ADAMVariant}
import java.util.Arrays

object RichADAMVariant {
  implicit def variantToRichVariant(variant: ADAMVariant): RichADAMVariant = new RichADAMVariant(variant)
  implicit def richVariantToVariant(variant: RichADAMVariant): ADAMVariant = variant.variant
}

class RichADAMVariant(val variant: ADAMVariant) {
 // Only include the contigName in the hash
  val hashObjects = Array[Object](variant.getContig.getContigName, 
    variant.getPosition, variant.getReferenceAllele, variant.getVariantAllele)
  override def hashCode = Arrays.hashCode(hashObjects)

  private def isSameContig(left: ADAMContig, right: ADAMContig): Boolean = {
    left.getContigName == right.getContigName && (
      left.getContigMD5 == null || right.contigMD5 == null || left.getContigMD5 == right.getContigMD5
    )
  }

  override def equals(o: Any) = o match {
    case that: RichADAMVariant => {
      variant.getPosition        == that.variant.getPosition  &&
      isSameContig(variant.getContig, that.variant.getContig) &&
      variant.getReferenceAllele == that.variant.getReferenceAllele &&
      variant.getVariantAllele   == that.variant.getVariantAllele
    }
    case _ => false
  }

  def isSingleNucleotideVariant() = {
    variant.getReferenceAllele.length == 1 && variant.getVariantAllele.length == 1
  }
  
  def isMultipleNucleotideVariant() = {
    !isSingleNucleotideVariant && variant.getReferenceAllele.length == variant.getVariantAllele.length
  }

  def isInsertion() = variant.getReferenceAllele.length < variant.getVariantAllele.length

  def isDeletion() = variant.getReferenceAllele.length > variant.getVariantAllele.length

}
