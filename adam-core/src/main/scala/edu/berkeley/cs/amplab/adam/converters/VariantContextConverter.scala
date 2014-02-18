/*
 * Copyright (c) 2013-2014. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.converters

import edu.berkeley.cs.amplab.adam.avro._
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import java.util
import org.apache.spark.Logging
import org.broadinstitute.variant.variantcontext.{Allele, Genotype, VariantContext}
import org.broadinstitute.variant.vcf.VCFConstants
import scala.collection.JavaConverters._

/**
 * This class converts VCF data to and from ADAM. This translation occurs at the abstraction level
 * of the GATK VariantContext which represents VCF data, and at the ADAMVariantContext level, which
 * aggregates ADAM variant/genotype/annotation data together.
 *
 * If an annotation has a corresponding set of fields in the VCF standard, a conversion to/from the
 * GATK VariantContext should be implemented in this class.
 */
private[adam] class VariantContextConverter extends Serializable with Logging {

  private def convertAllele(allele: Allele): ADAMGenotypeAllele = {
    if (allele.isNoCall) ADAMGenotypeAllele.NoCall
    else if (allele.isReference) ADAMGenotypeAllele.Ref
    else ADAMGenotypeAllele.Alt
  }

  implicit def gatkAllelesToADAMAlleles(gatkAlleles : util.List[Allele]) : util.List[ADAMGenotypeAllele] = {
    gatkAlleles.map(convertAllele(_))
  }

  /**
   * Converts a single GATK variant into ADAMVariantContext(s).
   *
   * @param vc GATK Variant context to convert.
   * @return ADAM variant contexts
   */
  def convert(vc: VariantContext): Seq[ADAMVariantContext] = {
    val contig: ADAMContig = ADAMContig.newBuilder()
      .setContigName(vc.getChr).build

    // TODO: Handle multi-allelic sites
    // We need to split the alleles (easy) and split and subset the PLs (harder)/update the genotype
    if (!vc.isBiallelic) {
      return Seq()
    }

    val variant: ADAMVariant = ADAMVariant.newBuilder
      .setContig(contig)
      .setPosition(vc.getStart - 1 /* ADAM is 0-indexed */)
      .setReferenceAllele(vc.getReference.getBaseString)
      .setVariantAllele(vc.getAlternateAllele(0).getBaseString)
      .build

    val genotypes: Seq[ADAMGenotype] = vc.getGenotypes.iterator.asScala.map((g: Genotype) => {
      val genotype: ADAMGenotype.Builder = ADAMGenotype.newBuilder
        .setVariant(variant)
        .setSampleId(g.getSampleName)
        .setAlleles(gatkAllelesToADAMAlleles(g.getAlleles))
        .setIsPhased(g.isPhased)

      if (vc.isFiltered) {
        val annotations : VariantCallingAnnotations.Builder = 
          VariantCallingAnnotations.newBuilder
            .setVariantIsPassing(!vc.isFiltered)
            .setVariantFilters(new util.ArrayList(vc.getFilters))
        genotype.setVariantCallingAnnotations(annotations.build)
      }

      if (g.hasExtendedAttribute(VCFConstants.PHASE_QUALITY_KEY))
        genotype.setPhaseQuality(g.getExtendedAttribute(VCFConstants.PHASE_QUALITY_KEY).asInstanceOf[java.lang.Integer])

      if (g.hasExtendedAttribute(VCFConstants.PHASE_SET_KEY))
        genotype.setPhaseSetId(g.getExtendedAttribute(VCFConstants.PHASE_SET_KEY).asInstanceOf[Integer])

      genotype.build
    }).toSeq

    Seq(ADAMVariantContext(variant, genotypes = genotypes))
  }
}
