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

import edu.berkeley.cs.amplab.adam.avro.{ADAMVariant, ADAMGenotype, ADAMVariantDomain}
import edu.berkeley.cs.amplab.adam.converters.GenotypesToVariantsConverter
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object ADAMVariantContext {

  /**
   * Merges RDDs containing ADAMVariant, ADAMGenotype, and ADAMVariantDomain records. Variant domain
   * records are an optional variant site annotation and are omitted by default. This merger produces
   * an RDD of ADAMVariantContexts, which unifies all the variation/genotype data for a specific locus.
   *
   * @param variants RDD containing ADAMVariant records (allele data).
   * @param genotypes RDD containing ADAMGenotype records (genotype data).
   * @param variantDomains Option[RDD] containing annotated variant domain data. Provide None if omitted.
   * @return An RDD containing variant domains, which are separated by locus.
   */
  def mergeVariantsAndGenotypes(variants: RDD[ADAMVariant],
                                genotypes: RDD[ADAMGenotype],
                                variantDomains: Option[RDD[ADAMVariantDomain]] = None
                                 ): RDD[ADAMVariantContext] = {
    // group variants and genotypes by locus
    val groupedVariants = variants.keyBy(_.getPosition.toLong).groupByKey
    val groupedGenotypes = genotypes.keyBy(_.getPosition.toLong).groupByKey

    // perform initial merger of variants and genotypes
    val initialMerge = groupedVariants.join(groupedGenotypes)
      .filter(kv => !kv._2._1.isEmpty)

    // merge in variant domain
    val mergeAfterDomain: RDD[(Long, (Seq[ADAMVariant], Seq[ADAMGenotype], Option[ADAMVariantDomain]))] = variantDomains match {
      case Some(o) => {
        // if domains are present, group by locus
        val groupedDomains = o.asInstanceOf[RDD[ADAMVariantDomain]].keyBy(_.getPosition.toLong)

        // join with variant/genotype data and then flatten tuple
        initialMerge.join(groupedDomains)
          .map((kv: (Long, ((Seq[ADAMVariant], Seq[ADAMGenotype]), ADAMVariantDomain))) => {
          (kv._1, (kv._2._1._1, kv._2._1._2, Option(kv._2._2)))
        })
      }
      case None => {
        initialMerge.map((kv: (Long, (Seq[ADAMVariant], Seq[ADAMGenotype]))) => {
          (kv._1, (kv._2._1, kv._2._2, None.asInstanceOf[Option[ADAMVariantDomain]]))
        })
      }
    }

    // map variant contexts
    mergeAfterDomain.map(ADAMVariantContext(_))
  }

  /**
   * Constructs an ADAMVariantContext from locus data. Used in merger process.
   *
   * @param kv Nested tuple containing (locus on reference, (variants at site, genotypes at site,
   *           optional domain annotation at site))
   * @return ADAMVariantContext corresponding to the data above.
   */
  private def apply(kv: (Long, (Seq[ADAMVariant], Seq[ADAMGenotype], Option[ADAMVariantDomain]))
                     ): ADAMVariantContext = {
    new ADAMVariantContext(kv._1, kv._2._1, kv._2._2, kv._2._3)
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
    val position = genotypes.head.getPosition
    assert(genotypes.map(_.getPosition).forall(_ == position), "Genotypes do not all have the same position.")

    val converter = new GenotypesToVariantsConverter(false, false)
    
    val variants = converter.convert(genotypes)

    new ADAMVariantContext(position, variants, genotypes, None)
  }
}

/**
 * Container class for variant contexts. Provides no methods, or anything of the like, just direct access
 * to the data contained within.
 *
 * Of note: this class is meant to organize variants, genotypes, and their annotated data. If annotated data
 * is specific to the locus, but not to a specific variant/genotype, it should exist as field wrapped in an
 * option. If an annotation is specific to a genotype, variant, or the combined genotype of a sample, it should
 * be implemented as a map. This is because the map can be left empty, and because the map provides associative
 * access.
 *
 * Any updates that add annotations should impact three or four files:
 * - adam-format/.../adam.avdl
 * - this file
 * - rdd/AdamContext.scala --> adamVariantLoad function
 * - If there is a corresponding conversion of that data between VCF and ADAM, commands/VariantContextConverter.scala
 */
case class ADAMVariantContext(position: Long,
                              variants: Seq[ADAMVariant],
                              genotypes: Seq[ADAMGenotype],
                              domains: Option[ADAMVariantDomain]) {
}
