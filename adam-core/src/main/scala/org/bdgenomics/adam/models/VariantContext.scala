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

import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.rich.RichVariant._
import org.bdgenomics.formats.avro.{ DatabaseVariantAnnotation, Genotype, Variant }

/**
 * Note: VariantContext inherits its name from the Picard VariantContext, and is not related to the SparkContext object.
 * If you're looking for the latter, see [[org.bdgenomics.adam.rdd.variation.VariationContext]]
 */

object VariantContext {

  /**
   * Constructs an VariantContext from locus data. Used in merger process.
   *
   * @param kv Nested tuple containing (locus on reference, (variants at site, genotypes at site,
   *           optional domain annotation at site))
   * @return VariantContext corresponding to the data above.
   */
  def apply(kv: (ReferencePosition, Variant, Iterable[Genotype], Option[DatabaseVariantAnnotation])): VariantContext = {
    new VariantContext(kv._1, kv._2, kv._3, kv._4)
  }

  /**
   * Constructs an VariantContext from an Variant
   *
   * @param v Variant which is used to construct the ReferencePosition
   * @return VariantContext corresponding to the Variant
   */
  def apply(v: Variant): VariantContext = {
    apply((ReferencePosition(v), v, Seq(), None))
  }

  /**
   * Constructs an VariantContext from an Variant and Seq[Genotype]
   *  and DatabaseVariantAnnotation
   *
   * @param v Variant which is used to construct the ReferencePosition
   * @param genotypes Seq[Genotype]
   * @param annotation Option[DatabaseVariantAnnotation]
   * @return VariantContext corresponding to the Variant
   */
  def apply(v: Variant, genotypes: Iterable[Genotype], annotation: Option[DatabaseVariantAnnotation] = None): VariantContext = {
    apply((ReferencePosition(v), v, genotypes, annotation))
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
    assert(genotypes.map(ReferencePosition(_)).forall(_ == position),
      "Genotypes do not all have the same position.")

    val variant = genotypes.head.getVariant

    new VariantContext(position, variant, genotypes, None)
  }
}

class VariantContext(
    val position: ReferencePosition,
    val variant: RichVariant,
    val genotypes: Iterable[Genotype],
    val databases: Option[DatabaseVariantAnnotation] = None) {
  def this(variant: RichVariant, genotypes: Iterable[Genotype], database: Option[DatabaseVariantAnnotation] = None) = {
    this(ReferencePosition(variant), variant, genotypes, database)
  }
}

