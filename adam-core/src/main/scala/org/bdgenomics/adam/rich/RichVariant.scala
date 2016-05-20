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

import org.bdgenomics.formats.avro.{ Genotype, Variant }

object RichVariant {
  def genotypeToRichVariant(genotype: Genotype): RichVariant = {
    val variant = Variant.newBuilder(genotype.variant)
      .setContigName(genotype.getContigName)
      .setStart(genotype.getStart)
      .setEnd(genotype.getEnd)
      .build()
    variantToRichVariant(variant)
  }

  implicit def variantToRichVariant(variant: Variant): RichVariant = new RichVariant(variant)
  implicit def richVariantToVariant(variant: RichVariant): Variant = variant.variant
}

class RichVariant(val variant: Variant) {
  def isSingleNucleotideVariant() = {
    variant.getReferenceAllele.length == 1 && variant.getAlternateAllele.length == 1
  }

  def isMultipleNucleotideVariant() = {
    !isSingleNucleotideVariant && variant.getReferenceAllele.length == variant.getAlternateAllele.length
  }

  def isInsertion() = variant.getReferenceAllele.length < variant.getAlternateAllele.length

  def isDeletion() = variant.getReferenceAllele.length > variant.getAlternateAllele.length

  override def hashCode = variant.hashCode

  override def equals(o: Any) = o match {
    case that: RichVariant => variant.equals(that.variant)
    case _                 => false
  }
}
