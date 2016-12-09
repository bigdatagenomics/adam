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
package org.bdgenomics.adam.models

import org.bdgenomics.formats.avro.{ Genotype, Variant, VariantAnnotation }
import org.bdgenomics.adam.rich.RichVariant

/**
 * Singleton object for building VariantContexts.
 */
object VariantContext {

  /**
   * Constructs an VariantContext from an Variant
   *
   * @param v Variant which is used to construct the ReferencePosition
   * @return VariantContext corresponding to the Variant
   */
  def apply(v: Variant): VariantContext = {
    new VariantContext(ReferencePosition(v), RichVariant(v), Iterable.empty)
  }

  /**
   * Constructs an VariantContext from an Variant and Seq[Genotype]
   *  and DatabaseVariantAnnotation
   *
   * @param v Variant which is used to construct the ReferencePosition
   * @param genotypes Seq[Genotype]
   * @return VariantContext corresponding to the Variant
   */
  def apply(v: Variant, genotypes: Iterable[Genotype]): VariantContext = {
    new VariantContext(ReferencePosition(v), RichVariant(v), genotypes)
  }

  /**
   * Builds a variant context off of a set of genotypes. Builds variants from the genotypes.
   *
   * @note Genotypes must be at the same position.
   *
   * @param genotypes List of genotypes to build variant context from.
   * @return A variant context corresponding to the variants and genotypes at this site.
   */
  def buildFromGenotypes(genotypes: Seq[Genotype]): VariantContext = {
    val position = ReferencePosition(genotypes.head)
    require(
      genotypes.map(ReferencePosition(_)).forall(_ == position),
      "Genotypes do not all have the same position."
    )

    val variant = genotypes.head.getVariant

    new VariantContext(position, RichVariant(variant), genotypes)
  }
}

/**
 * A representation of all variation data at a single variant.
 *
 * This class represents an equivalent to a single allele from a VCF line, and
 * is the ADAM equivalent to htsjdk.variant.variantcontext.VariantContext.
 *
 * @param position The locus that the variant is at.
 * @param variant The variant allele that is contained in this VariantContext.
 * @param genotypes An iterable collection of Genotypes where this allele was
 *   called. Equivalent to the per-sample FORMAT fields in a VCF.
 */
class VariantContext(
    val position: ReferencePosition,
    val variant: RichVariant,
    val genotypes: Iterable[Genotype]) extends Serializable {
}
