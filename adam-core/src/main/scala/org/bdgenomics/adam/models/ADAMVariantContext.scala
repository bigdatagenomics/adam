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

import org.bdgenomics.formats.avro.{ ADAMGenotype, ADAMDatabaseVariantAnnotation, ADAMVariant }
import org.bdgenomics.adam.rich.RichADAMVariant
import org.bdgenomics.adam.rich.RichADAMVariant._

/**
 * Note: ADAMVariantContext inherits its name from the Picard VariantContext, and is not related to the SparkContext object.
 * If you're looking for the latter, see [[org.bdgenomics.adam.rdd.variation.ADAMVariationContext]]
 */

object ADAMVariantContext {

  /**
   * Constructs an ADAMVariantContext from locus data. Used in merger process.
   *
   * @param kv Nested tuple containing (locus on reference, (variants at site, genotypes at site,
   *           optional domain annotation at site))
   * @return ADAMVariantContext corresponding to the data above.
   */
  def apply(kv: (ReferencePosition, ADAMVariant, Iterable[ADAMGenotype], Option[ADAMDatabaseVariantAnnotation])): ADAMVariantContext = {
    new ADAMVariantContext(kv._1, kv._2, kv._3, kv._4)
  }

  /**
   * Constructs an ADAMVariantContext from an ADAMVariant
   *
   * @param v ADAMVariant which is used to construct the ReferencePosition
   * @return ADAMVariantContext corresponding to the ADAMVariant
   */
  def apply(v: ADAMVariant): ADAMVariantContext = {
    apply((ReferencePosition(v), v, Seq(), None))
  }

  /**
   * Constructs an ADAMVariantContext from an ADAMVariant and Seq[ADAMGenotype]
   *  and ADAMDatabaseVariantAnnotation
   *
   * @param v ADAMVariant which is used to construct the ReferencePosition
   * @param genotypes Seq[ADAMGenotype]
   * @param annotation Option[ADAMDatabaseVariantAnnotation]
   * @return ADAMVariantContext corresponding to the ADAMVariant
   */
  def apply(v: ADAMVariant, genotypes: Iterable[ADAMGenotype], annotation: Option[ADAMDatabaseVariantAnnotation] = None): ADAMVariantContext = {
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
  def buildFromGenotypes(genotypes: Seq[ADAMGenotype]): ADAMVariantContext = {
    val position = ReferencePosition(genotypes.head)
    assert(genotypes.map(ReferencePosition(_)).forall(_ == position),
      "Genotypes do not all have the same position.")

    val variant = genotypes.head.getVariant

    new ADAMVariantContext(position, variant, genotypes, None)
  }
}

class ADAMVariantContext(
    val position: ReferencePosition,
    val variant: RichADAMVariant,
    val genotypes: Iterable[ADAMGenotype],
    val databases: Option[ADAMDatabaseVariantAnnotation] = None) {
  def this(variant: RichADAMVariant, genotypes: Iterable[ADAMGenotype], database: Option[ADAMDatabaseVariantAnnotation] = None) = {
    this(ReferencePosition(variant), variant, genotypes, database)
  }
}

