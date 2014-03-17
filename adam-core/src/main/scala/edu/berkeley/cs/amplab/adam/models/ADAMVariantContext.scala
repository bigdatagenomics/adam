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
package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotype, ADAMDatabaseVariantAnnotation, ADAMVariant}
import edu.berkeley.cs.amplab.adam.converters.GenotypesToVariantsConverter
import edu.berkeley.cs.amplab.adam.rich.RichADAMVariant
import edu.berkeley.cs.amplab.adam.rich.RichADAMVariant._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object ADAMVariantContext {

  /**
   * Constructs an ADAMVariantContext from locus data. Used in merger process.
   *
   * @param kv Nested tuple containing (locus on reference, (variants at site, genotypes at site,
   *           optional domain annotation at site))
   * @return ADAMVariantContext corresponding to the data above.
   */
  def apply(kv: (ReferencePosition, ADAMVariant, Seq[ADAMGenotype], Option[ADAMDatabaseVariantAnnotation]))
      : ADAMVariantContext = {
    new ADAMVariantContext(kv._1, kv._2, kv._3, kv._4)
  }

  /**
   * Constructs an ADAMVariantContext from an ADAMVariant
   *
   * @param v ADAMVariant which is used to construct the ReferencePosition
   * @return ADAMVariantContext corresponding to the ADAMVariant
   */
  def apply(v : ADAMVariant) : ADAMVariantContext = {
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
  def apply(v : ADAMVariant, genotypes : Seq[ADAMGenotype], annotation : Option[ADAMDatabaseVariantAnnotation] = None)
      : ADAMVariantContext = {
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

    val variant = genotypes.head.variant

    new ADAMVariantContext(position, variant, genotypes, None)
  }
}

class ADAMVariantContext(
  val position: ReferencePosition,
  val variant: RichADAMVariant,
  val genotypes: Seq[ADAMGenotype],
  val databases: Option[ADAMDatabaseVariantAnnotation] = None) {
  def this(variant : RichADAMVariant, genotypes : Seq[ADAMGenotype], database : Option[ADAMDatabaseVariantAnnotation] = None) = {
    this(ReferencePosition(variant), variant, genotypes, database)
  }
}

