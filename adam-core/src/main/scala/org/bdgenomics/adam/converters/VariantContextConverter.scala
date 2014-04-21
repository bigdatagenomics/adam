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
package org.bdgenomics.adam.converters

import org.bdgenomics.adam.avro._
import org.bdgenomics.adam.models.{ ADAMVariantContext, SequenceDictionary }
import org.broadinstitute.variant.variantcontext._
import org.broadinstitute.variant.vcf.VCFConstants
import scala.collection.JavaConversions._

object VariantContextConverter {

  // One conversion method for each way of representing an Allele
  private def convertAllele(vc: VariantContext, allele: Allele): ADAMGenotypeAllele = {
    if (allele.isNoCall) ADAMGenotypeAllele.NoCall
    else if (allele.isReference) ADAMGenotypeAllele.Ref
    else if (!vc.hasAlternateAllele(allele)) ADAMGenotypeAllele.OtherAlt
    else ADAMGenotypeAllele.Alt
  }

  private def convertAllele(allele: CharSequence, isRef: Boolean = false): Seq[Allele] = {
    if (allele == null) Seq() else Seq(Allele.create(allele.toString, isRef))
  }

  private def convertAlleles(v: ADAMVariant): java.util.Collection[Allele] = {
    convertAllele(v.getReferenceAllele, true) ++ convertAllele(v.getVariantAllele)
  }

  private def convertAlleles(g: ADAMGenotype): java.util.List[Allele] = {
    g.getAlleles.map(a => a match {
      case ADAMGenotypeAllele.NoCall => Allele.NO_CALL
      case ADAMGenotypeAllele.Ref | ADAMGenotypeAllele.OtherAlt => Allele.create(g.getVariant.getReferenceAllele.toString, true)
      case ADAMGenotypeAllele.Alt => Allele.create(g.getVariant.getVariantAllele.toString)
    })
  }
}

/**
 * This class converts VCF data to and from ADAM. This translation occurs at the abstraction level
 * of the GATK VariantContext which represents VCF data, and at the ADAMVariantContext level, which
 * aggregates ADAM variant/genotype/annotation data together.
 *
 * If an annotation has a corresponding set of fields in the VCF standard, a conversion to/from the
 * GATK VariantContext should be implemented in this class.
 */
class VariantContextConverter(dict: Option[SequenceDictionary] = None) extends Serializable {

  private lazy val splitFromMultiAllelicField = ADAMGenotype.SCHEMA$.getField("splitFromMultiAllelic")

  /**
   * Converts a single GATK variant into ADAMVariantContext(s).
   *
   * @param vc GATK Variant context to convert.
   * @return ADAM variant contexts
   */
  def convert(vc: VariantContext, extractExternalAnnotations: Boolean = false): Seq[ADAMVariantContext] = {
    // Uncommon case: Multi-allelic sites
    if (!vc.isBiallelic) {
      return vc.getAlternateAlleles.flatMap(allele => {
        val idx = vc.getAlleleIndex(allele)
        assert(idx >= 1, "Unexpected index for alternate allele")

        val vcb = new VariantContextBuilder(vc)

        // Retain alternate allele of interest
        vcb.alleles(List(vc.getReference, allele))

        val gc = GenotypesContext.create // Fixup genotypes
        gc.addAll(vc.getGenotypes.map((g: Genotype) => {
          val gb = new GenotypeBuilder(g)
          gb.phased(true) // Multi-allelic genotypes are locally phased, TODO add phase set
          if (g.hasAD) gb.AD(Array(g.getAD()(0), g.getAD()(idx))) // "Punch out" other alleles in AD
          if (g.hasLikelihoods) {  // Recompute PLs as needed to reflect stripped alleles
            val oldLikelihoods = g.getLikelihoods.getAsVector
            val newLikelihoods = GenotypeLikelihoods.getPLIndecesOfAlleles(0, idx).map(oldLikelihoods(_))
            // Normalize new likelihoods in log-space
            gb.PL(newLikelihoods.map(_ - newLikelihoods.max))
          }
          gb.make()
        }))
        // We purposely retain "invalid" genotype alleles, that will eventually become
        // "OtherAlt" entries, but won't validate against the reduced VariantContext
        vcb.genotypesNoValidation(gc)

        // Recursively convert now bi-allelic VariantContexts, setting any multi-allelic
        // specific fields afterwards
        val adamVCs = convert(vcb.make, extractExternalAnnotations)
        adamVCs.flatMap(_.genotypes).foreach(g => g.put(splitFromMultiAllelicField.pos, true))
        adamVCs
      })
    }

    // Common case: Biallelic site
    val variant: ADAMVariant = createADAMVariant(vc)

    // VCF Genotypes
    val calling_annotations: VariantCallingAnnotations = extractVariantCallingAnnotations(vc)
    val genotypes: Seq[ADAMGenotype] = extractGenotypes(vc, variant, calling_annotations)

    val annotation: Option[ADAMDatabaseVariantAnnotation] =
      if (extractExternalAnnotations)
        Some(extractVariantDatabaseAnnotation(variant, vc))
      else
        None

    return Seq(ADAMVariantContext(variant, genotypes, annotation))
  }

  def convertToAnnotation(vc: VariantContext): ADAMDatabaseVariantAnnotation = {

    val variant = createADAMVariant(vc)
    extractVariantDatabaseAnnotation(variant, vc)

  }

  private def createADAMVariant(vc: VariantContext): ADAMVariant = {
    var contigId = 0;
    // This is really ugly - only temporary until we remove numeric
    // IDs from our representation of contigs.
    try {
      contigId = vc.getID.toInt
    } catch {
      case ex: NumberFormatException => {

      }
    }

    val contig: ADAMContig.Builder = ADAMContig.newBuilder()
      .setContigName(vc.getChr)
      .setContigId(contigId)

    if (dict.isDefined) {
      val sr = (dict.get)(vc.getChr)
      contig.setContigLength(sr.length).setReferenceURL(sr.url).setContigMD5(sr.md5)
    }

    // VCF CHROM, POS, REF and ALT
    ADAMVariant.newBuilder
      .setContig(contig.build)
      .setPosition(vc.getStart - 1 /* ADAM is 0-indexed */ )
      .setReferenceAllele(vc.getReference.getBaseString)
      .setVariantAllele(vc.getAlternateAllele(0).getBaseString)
      .build
  }

  private def extractVariantDatabaseAnnotation(variant: ADAMVariant, vc: VariantContext): ADAMDatabaseVariantAnnotation = {
    val annotation = ADAMDatabaseVariantAnnotation.newBuilder()
      .setVariant(variant)
      .build

    VariantAnnotationConverter.convert(vc, annotation)

  }

  private def extractGenotypes(vc: VariantContext, variant: ADAMVariant, annotations: VariantCallingAnnotations): Seq[ADAMGenotype] = {
    val genotypes: Seq[ADAMGenotype] = vc.getGenotypes.map((g: Genotype) => {
      val genotype: ADAMGenotype.Builder = ADAMGenotype.newBuilder
        .setVariant(variant)
        .setVariantCallingAnnotations(annotations)
        .setSampleId(g.getSampleName)
        .setAlleles(g.getAlleles.map(VariantContextConverter.convertAllele(vc, _)))
        .setIsPhased(g.isPhased)

      if (g.hasGQ) genotype.setGenotypeQuality(g.getGQ)
      if (g.hasDP) genotype.setReadDepth(g.getDP)
      if (g.hasAD) {
        val ad = g.getAD
        assert(ad.length == 2, "Unexpected number of allele depths for bi-allelic variant")
        genotype.setReferenceReadDepth(ad(0)).setAlternateReadDepth(ad(1))
      }
      if (g.hasPL) genotype.setGenotypeLikelihoods(g.getPL.toList.map(p => p: java.lang.Integer))

      val builtGenotype = genotype.build
      for ((v, a) <- VariantAnnotationConverter.VCF2GenotypeAnnotations) {
        // Add extended attributes if present
        val attr = g.getExtendedAttribute(v)
        if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
          builtGenotype.put(a._1, a._2(attr))
        }
      }
      builtGenotype
    }).toSeq
    genotypes
  }

  private def extractVariantCallingAnnotations(vc: VariantContext): VariantCallingAnnotations = {
    val call: VariantCallingAnnotations.Builder = VariantCallingAnnotations.newBuilder

    // VCF QUAL, FILTER and INFO fields
    if (vc.hasLog10PError) {
      call.setVariantCallErrorProbability(vc.getPhredScaledQual.asInstanceOf[Float])
    }

    if (vc.filtersWereApplied && vc.isFiltered) {
      call.setVariantIsPassing(false).setVariantFilters(new java.util.ArrayList(vc.getFilters))
    } else if (vc.filtersWereApplied) {
      call.setVariantIsPassing(true)
    }

    VariantAnnotationConverter.convert(vc, call.build())
  }

  /**
   * Convert an ADAMVariantContext into the equivalent GATK VariantContext
   * @param vc
   * @return GATK VariantContext
   */
  def convert(vc: ADAMVariantContext): VariantContext = {
    val variant: ADAMVariant = vc.variant
    val vcb = new VariantContextBuilder()
      .chr(variant.getContig.getContigName.toString)
      .start(variant.getPosition + 1 /* Recall ADAM is 0-indexed */ )
      .stop(variant.getPosition + variant.getReferenceAllele.length)
      .alleles(VariantContextConverter.convertAlleles(variant))

    vc.databases.flatMap(d => Option(d.getDbSnpId)).foreach(d => vcb.id("rs" + d))

    // TODO: Extract provenance INFO fields
    vcb.genotypes(vc.genotypes.map(g => {
      val gb = new GenotypeBuilder(g.getSampleId.toString, VariantContextConverter.convertAlleles(g))

      Option(g.getIsPhased).foreach(gb.phased(_))
      Option(g.getGenotypeQuality).foreach(gb.GQ(_))
      Option(g.getReadDepth).foreach(gb.DP(_))
      if (g.getReferenceReadDepth != null && g.getAlternateReadDepth != null)
        gb.AD(Array(g.getReferenceReadDepth, g.getAlternateReadDepth))
      if (g.getVariantCallingAnnotations != null) {
        val callAnnotations = g.getVariantCallingAnnotations()
        if (callAnnotations.getVariantFilters != null)
          gb.filters(callAnnotations.getVariantFilters.map(_.toString))
      }

      if (g.getGenotypeLikelihoods.nonEmpty)
        gb.PL(g.getGenotypeLikelihoods.map(p => p: Int).toArray)

      gb.make
    }))

    vcb.make
  }

}
