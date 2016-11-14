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
   * Produces a new variant context by merging with an optional annotation.
   *
   * If the existing context doesn't have an annotation, pick the new
   * annotation, if present. If both exist, then merge.
   *
   * @param v An existing VariantContext to annotate.
   * @param optAnn An optional annotation to add.
   * @return A new VariantContext, where an annotation has been added/merged.
   */
  def apply(v: VariantContext,
            optAnn: Option[VariantAnnotation]): VariantContext = {

    // if the join yielded one or fewer annotation, pick what we've got. else, merge.
    val ann = (v.annotations, optAnn) match {
      case (None, a)          => a
      case (a, None)          => a
      case (Some(a), Some(b)) => Some(mergeAnnotations(a, b))
    }

    // copy all fields except for the annotation from the input context
    new VariantContext(v.position,
      v.variant,
      v.genotypes,
      ann)
  }

  /**
   * Greedily merges two annotation records by filling empty fields.
   *
   * Merges two records by taking the union of all fields. If a field is
   * populated in both records, it's value will be taken from the left
   * record.
   *
   * @param leftRecord First record to merge. If fields are seen in both
   *   records, the value in this record will win.
   * @param rightRecord Second record to merge. Used to populate missing fields
   *   from the left record.
   * @return Returns the union of these two annotations.
   */
  private[adam] def mergeAnnotations(
    leftRecord: VariantAnnotation,
    rightRecord: VariantAnnotation): VariantAnnotation = {
    val mergedAnnotation = VariantAnnotation.newBuilder(leftRecord)
      .build()
    val numFields = VariantAnnotation.getClassSchema.getFields.size

    def insertField(fieldIdx: Int) =
      {
        val value = rightRecord.get(fieldIdx)
        if (value != null) {
          mergedAnnotation.put(fieldIdx, value)
        }
      }
    (0 until numFields).foreach(insertField(_))

    mergedAnnotation
  }

  /**
   * Constructs an VariantContext from locus data. Used in merger process.
   *
   * @param kv Nested tuple containing (locus on reference, (variants at site, genotypes at site,
   *           optional domain annotation at site))
   * @return VariantContext corresponding to the data above.
   */
  def apply(kv: (ReferencePosition, Variant, Iterable[Genotype], Option[VariantAnnotation])): VariantContext = {
    new VariantContext(kv._1, RichVariant(kv._2), kv._3, kv._4)
  }

  /**
   * Constructs an VariantContext from an Variant
   *
   * @param v Variant which is used to construct the ReferencePosition
   * @return VariantContext corresponding to the Variant
   */
  def apply(v: Variant): VariantContext = {
    new VariantContext(ReferencePosition(v), RichVariant(v), Seq(), None)
  }

  /**
   * Constructs an VariantContext from an Variant and Seq[Genotype]
   *  and DatabaseVariantAnnotation
   *
   * @param v Variant which is used to construct the ReferencePosition
   * @param genotypes Seq[Genotype]
   * @param annotation Option[VariantAnnotation]
   * @return VariantContext corresponding to the Variant
   */
  def apply(v: Variant, genotypes: Iterable[Genotype], annotation: Option[VariantAnnotation] = None): VariantContext = {
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
    assert(
      genotypes.map(ReferencePosition(_)).forall(_ == position),
      "Genotypes do not all have the same position."
    )

    val variant = genotypes.head.getVariant

    new VariantContext(position, RichVariant(variant), genotypes, None)
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
 * @param databases An optional record annotating this variant. Equivalent to
 *   the per-allele split of the INFO field in a VCF.
 */
class VariantContext(
    val position: ReferencePosition,
    val variant: RichVariant,
    val genotypes: Iterable[Genotype],
    val annotations: Option[VariantAnnotation] = None) extends Serializable {
}
