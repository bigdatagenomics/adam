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
/*
* Copyright (c) 2014. Mount Sinai School of Medicine
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

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf._
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.bdgenomics.formats.avro.{ DatabaseVariantAnnotation, Genotype, VariantCallingAnnotations }

object AttrKey {
  def apply(adamKey: String, hdrLine: VCFCompoundHeaderLine): AttrKey = {
    new AttrKey(adamKey, null, hdrLine)
  }
}

case class AttrKey(adamKey: String, attrConverter: (Object => Object), hdrLine: VCFCompoundHeaderLine) {
  val vcfKey: String = hdrLine.getID
}

object VariantAnnotationConverter extends Serializable {

  private def attrAsInt(attr: Object): Object = attr match {
    case a: String            => java.lang.Integer.valueOf(a)
    case a: java.lang.Integer => a
    case a: java.lang.Number  => java.lang.Integer.valueOf(a.intValue)
  }
  private def attrAsLong(attr: Object): Object = attr match {
    case a: String           => java.lang.Long.valueOf(a)
    case a: java.lang.Long   => a
    case a: java.lang.Number => java.lang.Long.valueOf(a.longValue)
  }
  private def attrAsFloat(attr: Object): Object = attr match {
    case a: String           => java.lang.Float.valueOf(a)
    case a: java.lang.Float  => a
    case a: java.lang.Number => java.lang.Float.valueOf(a.floatValue)
  }
  private def attrAsString(attr: Object): Object = attr match {
    case a: String => a
  }
  private def attrAsBoolean(attr: Object): Object = attr match {
    case a: java.lang.Boolean => a
    case a: String            => java.lang.Boolean.valueOf(a)
  }

  val COSMIC_KEYS: List[AttrKey] = List(
    AttrKey("geneSymbol", attrAsString _, new VCFInfoHeaderLine("GENE,", 1, VCFHeaderLineType.String, "Gene name")),
    AttrKey("strand", attrAsString _, new VCFInfoHeaderLine("STRAND,", 1, VCFHeaderLineType.String, "Gene strand")),
    AttrKey("cds", attrAsString _, new VCFInfoHeaderLine("CDS,", 1, VCFHeaderLineType.String, "CDS annotation")),
    AttrKey("cnt", attrAsString _, new VCFInfoHeaderLine("CNT,", 1, VCFHeaderLineType.Integer, "How many samples have this mutation")))

  val DBNSFP_KEYS: List[AttrKey] = List(
    AttrKey("phylop", attrAsFloat _, new VCFInfoHeaderLine("PHYLOP", 1, VCFHeaderLineType.Float, "PhyloP score. The larger the score, the more conserved the site.")),
    AttrKey("siftPred", attrAsString _, new VCFInfoHeaderLine("SIFT_PRED", 1, VCFHeaderLineType.Character, "SIFT Prediction: D (damaging), T (tolerated)")),
    AttrKey("siftScore", attrAsFloat _, new VCFInfoHeaderLine("SIFT_SCORE", 1, VCFHeaderLineType.Float, "SIFT Score")),
    AttrKey("ancestralAllele", attrAsString _, new VCFInfoHeaderLine("AA", 1, VCFHeaderLineType.String, "Ancestral allele")))

  val CLINVAR_KEYS: List[AttrKey] = List(
    AttrKey("dbSnpId", attrAsInt _, new VCFInfoHeaderLine("dbSNP ID", 1, VCFHeaderLineType.Integer, "dbSNP ID")),
    AttrKey("geneSymbol", attrAsString _, new VCFInfoHeaderLine("GENEINFO", 1, VCFHeaderLineType.String, "Pairs each of gene symbol:gene id.  The gene symbol and id are delimited by a colon (:) and each pair is delimited by a vertical bar")))

  val OMIM_KEYS: List[AttrKey] = List(
    AttrKey("omimId", attrAsString _, new VCFInfoHeaderLine("VAR", 1, VCFHeaderLineType.String, "MIM entry with variant mapped to rsID")))

  val INFO_KEYS: Seq[AttrKey] = Seq(
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
    AttrKey("culprit", attrAsString _, new VCFInfoHeaderLine("culprit", 1, VCFHeaderLineType.String, "The annotation which was the worst performing in the Gaussian mixture model, likely the reason why the variant was filtered out")))

  val FORMAT_KEYS: Seq[AttrKey] = Seq(
    AttrKey("alleles", VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_KEY)),
    AttrKey("gtQuality", VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_QUALITY_KEY)),
    AttrKey("readDepth", VCFStandardHeaderLines.getFormatLine(VCFConstants.DEPTH_KEY)),
    AttrKey("alleleDepths", VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_ALLELE_DEPTHS)),
    AttrKey("gtFilters", VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_FILTER_KEY)),
    AttrKey("genotypeLikelihoods", VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_PL_KEY)),
    AttrKey("phaseQuality", attrAsInt _, new VCFFormatHeaderLine(VCFConstants.PHASE_QUALITY_KEY, 1, VCFHeaderLineType.Float, "Read-backed phasing quality")),
    AttrKey("phaseSetId", attrAsInt _, new VCFFormatHeaderLine(VCFConstants.PHASE_SET_KEY, 1, VCFHeaderLineType.Integer, "Phase set")),
    AttrKey("minReadDepth", attrAsInt _, new VCFFormatHeaderLine("MIN_DP", 1, VCFHeaderLineType.Integer, "Minimum DP observed within the GVCF block")),
    AttrKey("strandBiasComponents", attrAsInt _, new VCFFormatHeaderLine("SB", 4, VCFHeaderLineType.Integer, "Per-sample component statistics which comprise the Fisher's Exact Test to detect strand bias.")))

  lazy val infoHeaderLines: Seq[VCFCompoundHeaderLine] = INFO_KEYS.map(_.hdrLine)
  lazy val formatHeaderLines: Seq[VCFCompoundHeaderLine] = FORMAT_KEYS.map(_.hdrLine)

  lazy val VCF2VariantCallingAnnotations: Map[String, (Int, Object => Object)] =
    createFieldMap(INFO_KEYS, VariantCallingAnnotations.getClassSchema)
  lazy val VCF2GenotypeAnnotations: Map[String, (Int, Object => Object)] =
    createFieldMap(FORMAT_KEYS, Genotype.getClassSchema)

  private lazy val EXTERNAL_DATABASE_KEYS: Seq[AttrKey] = OMIM_KEYS ::: CLINVAR_KEYS ::: DBNSFP_KEYS // ::: COSMIC_KEYS
  lazy val VCF2DatabaseAnnotations: Map[String, (Int, Object => Object)] = createFieldMap(EXTERNAL_DATABASE_KEYS, DatabaseVariantAnnotation.getClassSchema)

  private def createFieldMap(keys: Seq[AttrKey], schema: Schema): Map[String, (Int, Object => Object)] = {
    keys.filter(_.attrConverter != null).map(field => {
      val avroField = schema.getField(field.adamKey)
      field.vcfKey -> (avroField.pos, field.attrConverter)
    })(collection.breakOut)
  }

  private def fillRecord[T <% SpecificRecord](fieldMap: Map[String, (Int, Object => Object)], vc: VariantContext, record: T): T = {
    for ((v, a) <- fieldMap) {
      val attr = vc.getAttribute(v)
      if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
        record.put(a._1, a._2(attr))
      }
    }
    record
  }

  private def fillKeys[T <% SpecificRecord](keys: Seq[AttrKey], vc: VariantContext, record: T): T = {
    fillRecord(createFieldMap(keys, record.getSchema), vc, record)
  }

  def convert(vc: VariantContext, annotation: DatabaseVariantAnnotation): DatabaseVariantAnnotation = {
    fillRecord(VCF2DatabaseAnnotations, vc, annotation)
  }

  def convert(vc: VariantContext, call: VariantCallingAnnotations): VariantCallingAnnotations = {
    fillRecord(VCF2VariantCallingAnnotations, vc, call)
  }

  def convert(g: htsjdk.variant.variantcontext.Genotype, genotype: Genotype): Genotype = {
    for ((v, a) <- VariantAnnotationConverter.VCF2GenotypeAnnotations) {
      // Add extended attributes if present
      val attr = g.getExtendedAttribute(v)
      if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
        genotype.put(a._1, a._2(attr))
      }
    }
    genotype
  }

  def mergeAnnotations(leftRecord: DatabaseVariantAnnotation, rightRecord: DatabaseVariantAnnotation): DatabaseVariantAnnotation = {
    val mergedAnnotation = DatabaseVariantAnnotation.newBuilder(leftRecord).build()
    val numFields = DatabaseVariantAnnotation.getClassSchema.getFields.size

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

}
