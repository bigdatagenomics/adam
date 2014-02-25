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

import edu.berkeley.cs.amplab.adam.avro._
import edu.berkeley.cs.amplab.adam.models.{ADAMVariantContext, SequenceDictionary, ReferencePosition}
import edu.berkeley.cs.amplab.adam.util.VcfStringUtils._
import org.broadinstitute.variant.vcf._
import org.broadinstitute.variant.variantcontext._
import org.broadinstitute.variant.vcf.VCFConstants
import scala.collection.JavaConversions._

object VariantContextConverter {
  private def attrAsInt(attr: Object):Object = attr match {
    case a: String => java.lang.Integer.valueOf(a)
    case a: java.lang.Integer => a
    case a: java.lang.Number => java.lang.Integer.valueOf(a.intValue)
  }
  private def attrAsLong(attr: Object):Object = attr match {
    case a: String => java.lang.Integer.valueOf(a)
    case a: java.lang.Long => a
    case a: java.lang.Number => java.lang.Long.valueOf(a.longValue)
  }
  private def attrAsFloat(attr: Object):Object = attr match {
    case a: String => java.lang.Float.valueOf(a)
    case a: java.lang.Float => a
    case a: java.lang.Number => java.lang.Float.valueOf(a.floatValue)
  }
  private def attrAsString(attr: Object):Object = attr match {
    case a: String => a
  }
  // Recall that the mere
  private def attrAsBoolean(attr: Object):Object = attr match {
    case a: java.lang.Boolean => a
    case a: String => java.lang.Boolean.valueOf(a)
  }


  private case class AttrKey(adamKey: String, attrConverter: (Object => Object), hdrLine: VCFCompoundHeaderLine) {
    val vcfKey: String = hdrLine.getID
  }

  private val INFO_KEYS: Seq[AttrKey] = Seq(
    AttrKey("clippingRankSum", attrAsFloat _, new VCFInfoHeaderLine("ClippingRankSum", 1, VCFHeaderLineType.Float, "Z-score From Wilcoxon rank sum test of Alt vs. Ref number of hard clipped bases")),
    AttrKey("readDepth", attrAsInt _, VCFStandardHeaderLines.getInfoLine(VCFConstants.DEPTH_KEY)),
    AttrKey("downsampled", attrAsBoolean _, VCFStandardHeaderLines.getInfoLine(VCFConstants.DOWNSAMPLED_KEY)),
    AttrKey("fisherStrandBiasPValue", attrAsFloat _, VCFStandardHeaderLines.getInfoLine(VCFConstants.STRAND_BIAS_KEY)),
    AttrKey("haplotypeScore", attrAsFloat _, new VCFInfoHeaderLine("HaplotypeScore", 1, VCFHeaderLineType.Float, "Consistency of the site with at most two segregating haplotypes")),
    AttrKey("inbreedingCoefficient", attrAsFloat _, new VCFInfoHeaderLine("InbreedingCoeff", 1, VCFHeaderLineType.Float, "Inbreeding coefficient as estimated from the genotype likelihoods per-sample when compared against the Hardy-Weinberg expectation")),
    AttrKey("rmsMapQ", attrAsFloat _, VCFStandardHeaderLines.getInfoLine(VCFConstants.RMS_MAPPING_QUALITY_KEY)),
    AttrKey("mapq0Reads", attrAsInt _, VCFStandardHeaderLines.getInfoLine(VCFConstants.MAPPING_QUALITY_ZERO_KEY)),
    AttrKey("mqRankSum", attrAsFloat _, new VCFInfoHeaderLine("MQRankSum", 1, VCFHeaderLineType.Float, "Z-score From Wilcoxon rank sum test of Alt vs. Ref read mapping qualities")),
    AttrKey("usedForNegativeTrainingSet", attrAsBoolean _, new VCFInfoHeaderLine("NEGATIVE_TRAIN_SITE", 1, VCFHeaderLineType.Flag, "This variant was used to build the negative training set of bad variants")),
    AttrKey("usedForPositiveTrainingSet", attrAsBoolean _, new VCFInfoHeaderLine("POSITIVE_TRAIN_SITE", 1, VCFHeaderLineType.Flag, "This variant was used to build the positive training set of good variants")),
    AttrKey("variantQualityByDepth", attrAsFloat _, new VCFInfoHeaderLine("QD", 1, VCFHeaderLineType.Float, "Variant Confidence/Quality by Depth")),
    AttrKey("readPositionRankSum", attrAsFloat _, new VCFInfoHeaderLine("ReadPosRankSum", 1, VCFHeaderLineType.Float, "Z-score from Wilcoxon rank sum test of Alt vs. Ref read position bias")),
    AttrKey("vqslod", attrAsFloat _, new VCFInfoHeaderLine("VQSLOD", 1, VCFHeaderLineType.Float, "Log odds ratio of being a true variant versus being false under the trained gaussian mixture model")),
    AttrKey("culprit", attrAsString _, new VCFInfoHeaderLine("culprit", 1, VCFHeaderLineType.String, "The annotation which was the worst performing in the Gaussian mixture model, likely the reason why the variant was filtered out"))
  )

  private val FORMAT_KEYS: Seq[AttrKey] = Seq(
    AttrKey("alleles", null, VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_KEY)),
    AttrKey("gtQuality", null, VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_QUALITY_KEY)),
    AttrKey("readDepth", null, VCFStandardHeaderLines.getFormatLine(VCFConstants.DEPTH_KEY)),
    AttrKey("alleleDepths", null, VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_ALLELE_DEPTHS)),
    AttrKey("gtFilters", null, VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_FILTER_KEY)),
    AttrKey("genotypeLikelihoods", null, VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_PL_KEY)),
    AttrKey("phaseQuality", attrAsInt _, new VCFFormatHeaderLine(VCFConstants.PHASE_QUALITY_KEY, 1, VCFHeaderLineType.Float, "Read-backed phasing quality")),
    AttrKey("phaseSetId", attrAsLong _, new VCFFormatHeaderLine(VCFConstants.PHASE_SET_KEY, 1, VCFHeaderLineType.Integer, "Phase set"))
  )
  
  lazy val infoHeaderLines : Seq[VCFCompoundHeaderLine] = INFO_KEYS.map(_.hdrLine)
  lazy val formatHeaderLines : Seq[VCFCompoundHeaderLine] = FORMAT_KEYS.map(_.hdrLine)

  lazy val VCF2VarCallAnnos : Map[String,(Int,Object => Object)] = INFO_KEYS.map(field => {
    var avro_field = VariantCallingAnnotations.getClassSchema.getField(field.adamKey)
    field.vcfKey -> (avro_field.pos, field.attrConverter)
  })(collection.breakOut)

  lazy val VCF2GTAnnos : Map[String,(Int,Object => Object)] = FORMAT_KEYS.filter(_.attrConverter != null).map(field => {
      var avro_field = ADAMGenotype.getClassSchema.getField(field.adamKey)
      field.vcfKey -> (avro_field.pos, field.attrConverter)
    })(collection.breakOut)



  private def convertAttributes(vc: VariantContext, call : VariantCallingAnnotations): VariantCallingAnnotations = {
    for ((v,a) <- VCF2VarCallAnnos) {
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

  private def convertAllele(allele: CharSequence, isRef: Boolean = false): Seq[Allele] = {
    if (allele == null) Seq() else Seq(Allele.create(allele.toString, isRef))
  }

  private def convertAlleles(v: ADAMVariant): java.util.Collection[Allele] = {
    convertAllele(v.getReferenceAllele, true) ++ convertAllele(v.getVariantAllele)
  }

  private def convertAlleles(g: ADAMGenotype): java.util.List[Allele] = {
    g.getAlleles.map(a => a match {
      case ADAMGenotypeAllele.NoCall => Allele.NO_CALL
      case ADAMGenotypeAllele.Ref => Allele.create(g.getVariant.getReferenceAllele.toString, true)
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
private[adam] class VariantContextConverter(dict: Option[SequenceDictionary] = None) extends Serializable {
  import VariantContextConverter._

  private def convertAllele(allele: Allele): ADAMGenotypeAllele = {
    if (allele.isNoCall) ADAMGenotypeAllele.NoCall
    else if (allele.isReference) ADAMGenotypeAllele.Ref
    else ADAMGenotypeAllele.Alt
  }

  implicit def gatkAllelesToADAMAlleles(gatkAlleles : java.util.List[Allele]) 
      : java.util.List[ADAMGenotypeAllele] = {
    gatkAlleles.map(convertAllele(_))
  }

  /**
   * Converts a single GATK variant into ADAMVariantContext(s).
   *
   * @param vc GATK Variant context to convert.
   * @return ADAM variant contexts
   */
  def convert(vc:VariantContext): Seq[ADAMVariantContext] = {

    var contigId = 0;
    // This is really ugly - only temporary until we remove numeric
    // IDs from our representation of contigs.
    try {
      contigId = vc.getID.toInt
    } catch {
      case ex:NumberFormatException => {
      }
    }

    val contig: ADAMContig.Builder = ADAMContig.newBuilder()
      .setContigName(vc.getChr)
      .setContigId(contigId)

    if (dict.isDefined) {
      val sr = (dict.get)(vc.getChr)
      contig.setContigLength(sr.length).setReferenceURL(sr.url).setContigMD5(sr.md5)
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
      .setReferenceAllele(vc.getReference.getBaseString)
      .setVariantAllele(vc.getAlternateAllele(0).getBaseString)
      .build

    val shared_genotype_builder: ADAMGenotype.Builder = ADAMGenotype.newBuilder
      .setVariant(variant)

    val call : VariantCallingAnnotations.Builder =
      VariantCallingAnnotations.newBuilder

    // VCF QUAL, FILTER and INFO fields
    if (vc.hasLog10PError) {
      call.setVariantCallErrorProbability(vc.getPhredScaledQual.asInstanceOf[Float])
    }

    if (vc.isFiltered) { // not PASSing
      call.setVariantIsPassing(!vc.isFiltered.booleanValue)
        .setVariantFilters(new java.util.ArrayList(vc.getFilters))
    } else { 
      /* otherwise, filters were applied and the variant passed, or no
       * filters were appled */
      call.setVariantIsPassing(true)
    }
    shared_genotype_builder.setVariantCallingAnnotations(convertAttributes(vc, call.build()))

    // VCF Genotypes
    val shared_genotype = shared_genotype_builder.build
    val genotypes: Seq[ADAMGenotype] = vc.getGenotypes.map((g: Genotype) => {
      val genotype: ADAMGenotype.Builder = ADAMGenotype.newBuilder(shared_genotype)
        .setSampleId(g.getSampleName)
        .setAlleles(g.getAlleles.map(convertAllele(_)))
        .setIsPhased(g.isPhased)

      if (g.hasGQ) genotype.setGenotypeQuality(g.getGQ)
      if (g.hasDP) genotype.setReadDepth(g.getDP)
      if (g.hasAD) {
        val ad = g.getAD
        assert(ad.length == 2, "Unexpected number of allele depths for bi-allelic variant")
        genotype.setReferenceReadDepth(ad(0)).setAlternateReadDepth(ad(1))
      }
      if (g.hasPL) genotype.setGenotypeLikelihoods(g.getPL.toList.map(p => p:java.lang.Integer))


      val built_genotype = genotype.build
      for ((v,a) <- VCF2GTAnnos) { // Add extended attributes if present
        val attr = g.getExtendedAttribute(v)
        if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
          built_genotype.put(a._1, a._2(attr))
        }
      }
      built_genotype
    }).toSeq

    Seq(ADAMVariantContext((ReferencePosition(variant), variant, genotypes)))
  }

  /**
   * Convert an ADAMVariantContext into the equivalent GATK VariantContext
   * @param vc
   * @return GATK VariantContext
   */
  def convert(vc:ADAMVariantContext):VariantContext = {
    val variant : ADAMVariant = vc.variant
    val vcb = new VariantContextBuilder()
      .chr(variant.getContig.getContigName.toString)
      .start(variant.getPosition + 1 /* Recall ADAM is 0-indexed */)
      .stop(variant.getPosition + 1 + variant.getReferenceAllele.length - 1)
      .alleles(convertAlleles(variant))

    vc.databases.flatMap(d => Option(d.getDbsnpId)).foreach(d => vcb.id("rs" + d))

    // TODO: Extract provenance INFO fields
    vcb.genotypes(vc.genotypes.map(g => {
      val gb = new GenotypeBuilder(g.getSampleId.toString, convertAlleles(g))

      Option(g.getIsPhased).foreach(gb.phased(_))
      Option(g.getGenotypeQuality).foreach(gb.GQ(_))
      Option(g.getReadDepth).foreach(gb.DP(_))
      if (g.getReferenceReadDepth != null && g.getAlternateReadDepth != null)
        gb.AD(Array(g.getReferenceReadDepth, g.getAlternateReadDepth))
      if (g.variantCallingAnnotations != null) {
        val callAnnotations = g.getVariantCallingAnnotations()
        if (callAnnotations.variantFilters != null)
          gb.filters(callAnnotations.variantFilters.map(_.toString))
      }

      if (g.getGenotypeLikelihoods.nonEmpty)
        gb.PL(g.getGenotypeLikelihoods.map(p => p:Int).toArray)

      gb.make
    }))

    vcb.make
  }

}
