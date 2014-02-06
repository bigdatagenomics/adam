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
package edu.berkeley.cs.amplab.adam.converters

import scala.collection.JavaConverters._
import org.apache.spark.Logging
import org.broadinstitute.variant.variantcontext.{Allele, Genotype, VariantContext}
import edu.berkeley.cs.amplab.adam.avro._
import edu.berkeley.cs.amplab.adam.models.{SequenceDictionary, ADAMVariantContext}
import edu.berkeley.cs.amplab.adam.util.VcfStringUtils._
import org.broadinstitute.variant.vcf.VCFConstants
import java.util

object VariantContextConverter {
  private def attrAsInt(attr: Object):Object = attr match {
    case a: java.lang.Integer => a
    case a: String => java.lang.Integer.valueOf(a)
  }
  private def attrAsFloat(attr: Object):Object = attr match {
    case a: java.lang.Float => a
    case a: java.lang.Double => a
    case a: String => java.lang.Float.valueOf(a)
  }
  private def attrAsString(attr: Object):Object = attr match {
    case a: String => a
  }
  // Recall that the mere
  private def attrAsBoolean(attr: Object):Object = attr match {
    case a: java.lang.Boolean => a
    case a: String => java.lang.Boolean.valueOf(a)
  }

  private val INFO_MAP: Map[String,(String,Object => Object)] = Map(
    "ClippingRankSum" -> ("clippingRankSum", attrAsFloat _),
    VCFConstants.DEPTH_KEY -> ("readDepth", attrAsInt _),
    VCFConstants.DOWNSAMPLED_KEY -> ("downsampled", attrAsBoolean _),
    VCFConstants.STRAND_BIAS_KEY -> ("fisherStrandBiasPValue", attrAsFloat _),
    "HaplotypeScore" -> ("haplotypeScore", attrAsFloat _),
    "InbreedingCoeff" -> ("inbreedingCoefficient", attrAsFloat _),
    VCFConstants.RMS_MAPPING_QUALITY_KEY -> ("rmsMapQ", attrAsFloat _),
    VCFConstants.MAPPING_QUALITY_ZERO_KEY -> ("mapq0Reads", attrAsInt _),
    "MQRankSum" -> ("mqRankSum", attrAsFloat _),
    "NEGATIVE_TRAIN_SITE" -> ("usedForNegativeTrainingSet", attrAsBoolean _),
    "POSITIVE_TRAIN_SITE" -> ("usedForPositiveTrainingSet", attrAsBoolean _),
    "QD" -> ("variantQualityByDepth", attrAsFloat _),
    "ReadPosRankSum" -> ("readPositionRankSum", attrAsFloat _),
    "VQSLOD" -> ("vqslod", attrAsFloat _),
    "culprit" -> ("culprit", attrAsString _)
  )

  val VCF2VariantCallAnnotations: Map[String,(Int,Object => Object)] = INFO_MAP.mapValues(adam => {
    var field = VariantCallingAnnotations.getClassSchema.getField(adam._1)
    (field.pos, adam._2)
  })

}

/**
 * This class converts VCF data to and from ADAM. This translation occurs at the abstraction level
 * of the GATK VariantContext which represents VCF data, and at the ADAMVariantContext level, which
 * aggregates ADAM variant/genotype/annotation data together.
 *
 * If an annotation has a corresponding set of fields in the VCF standard, a conversion to/from the
 * GATK VariantContext should be implemented in this class.
 */
private[adam] class VariantContextConverter(dict: Option[SequenceDictionary] = None) extends Serializable with Logging {
  import VariantContextConverter._

  initLogging()

  private def convertINFOAttributes(vc: VariantContext): VariantCallingAnnotations = {
    var call : VariantCallingAnnotations = new VariantCallingAnnotations()
    for ((v,a) <- VCF2VariantCallAnnotations) {
      val attr = vc.getAttribute(v)
      if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
        call.put(a._1, a._2(attr))
      }
    }
    call
  }

  private def convertAllele(allele: Allele): ADAMGenotypeAllele = {
    if (allele.isNoCall) ADAMGenotypeAllele.NoCall
    else if (allele.isReference) ADAMGenotypeAllele.Ref
    else ADAMGenotypeAllele.Alt
  }

  /**
   * Converts a single GATK variant into ADAMVariantContext(s).
   *
   * @param vc GATK Variant context to convert.
   * @return ADAM variant contexts
   */
  def convert(vc: VariantContext): Seq[ADAMVariantContext] = {

    val contig: ADAMContig.Builder = ADAMContig.newBuilder().setContigName(vc.getChr)
    if (dict.isDefined) {
      val sr = (dict.get)(vc.getChr)
      contig.setReferenceLength(sr.length).setReferenceURL(sr.url).setReferenceMD5(sr.md5)
    }

    // TODO: Handle multi-allelic sites
    // We need to split the alleles (easy) and split and subset the PLs (harder)/update the genotype
    if (!vc.isBiallelic) {
      return Seq()
    }

    // VCF CHROM, POS, REF and ALT
    val variant: ADAMVariant = ADAMVariant.newBuilder
      .setContig(contig.build)
      .setPosition(vc.getStart - 1 /* ADAM is 0-indexed */)
      .setReferenceAlleles(strToEnumList[Base](
        vc.getReference.getBaseString).asJava)
      .setVariantAlleles(strToEnumList[Base](
        vc.getAlternateAllele(0).getBaseString).asJava)
      .build

    val shared_genotype_builder: ADAMGenotype.Builder = ADAMGenotype.newBuilder
      .setVariant(variant)

    // VCF QUAL, FILTER and INFO fields
    if (vc.hasLog10PError) {
      shared_genotype_builder.setVarProbError(vc.getPhredScaledQual.asInstanceOf[Float])
    }

    if (vc.isFiltered) { // not PASSing
      shared_genotype_builder.setVarIsFiltered(vc.isFiltered).setVarFilters(
        new util.ArrayList(vc.getFilters))
    } else if (vc.filtersWereApplied) { // PASSing
      shared_genotype_builder.setVarIsFiltered(false)
    }
    shared_genotype_builder.setVariantCallingAnnotations(convertINFOAttributes(vc))


    // VCF Genotypes
    val shared_genotype = shared_genotype_builder.build
    val genotypes: Seq[ADAMGenotype] = vc.getGenotypes.iterator.asScala.map((g: Genotype) => {
      val genotype: ADAMGenotype.Builder = ADAMGenotype.newBuilder(shared_genotype)
        .setSampleId(g.getSampleName)
        .setAlleles(g.getAlleles.asScala.map(convertAllele(_)).asJava)

      if (g.hasGQ) genotype.setGenotypeQuality(g.getGQ)
      if (g.hasDP) genotype.setReadDepth(g.getDP)
      if (g.hasAD) {
        val ad = g.getAD
        assert(ad.length == 2, "Unexpected number of allele depths for biallelic variant")
        genotype.setReferenceReadDepth(ad(0)).setAlternateReadDepth(ad(1))
      }
      if (g.isFiltered) {
        // TODO: There has got be a better way to deal with the types here...
        genotype.setGtIsFiltered(true).setGtFilters(
          new util.ArrayList(g.getFilters.split(';').toList.asJava))
      } else {
        genotype.setGtIsFiltered(false)
      }

      genotype.setIsPhased(g.isPhased)
      if (g.hasExtendedAttribute(VCFConstants.PHASE_QUALITY_KEY))
        genotype.setPhaseQuality(g.getExtendedAttribute(VCFConstants.PHASE_QUALITY_KEY).asInstanceOf[Int])
      if (g.hasExtendedAttribute(VCFConstants.PHASE_SET_KEY))
        genotype.setPhaseSetId(g.getExtendedAttribute(VCFConstants.PHASE_SET_KEY).asInstanceOf[String])

      genotype.build
    }).toSeq

    Seq(ADAMVariantContext(variant, genotypes = genotypes))
  }

}
