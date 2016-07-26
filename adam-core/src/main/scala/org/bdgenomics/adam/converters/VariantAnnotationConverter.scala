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
package org.bdgenomics.adam.converters

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf._
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAnnotation, VariantAnnotation }

/**
 * Singleton object for building AttrKey instances.
 */
private object AttrKey {

  /**
   * Helper function for building an AttrKey with a null converter.
   *
   * @param adamKey The key for this annotation.
   * @param hdrLine The header line that represents this annotation.
   * @return Returns an AttrKey instance with a null converter.
   */
  def apply(adamKey: String, hdrLine: VCFCompoundHeaderLine): AttrKey = {
    new AttrKey(adamKey, null, hdrLine)
  }
}

/**
 * Case class representing a VCF info line key.
 *
 * @param adamKey The key that represents this line.
 * @param attrConverter Converter for the value of this annotation.
 * @param hdrLine VCF header line for representing this field.
 */
private case class AttrKey(adamKey: String,
                           attrConverter: (Object => Object),
                           hdrLine: VCFCompoundHeaderLine) {

  /**
   * The VCF key for this info field.
   */
  val vcfKey: String = hdrLine.getID
}

/**
 * Singleton object for converting htsjdk Variant Contexts into Avro.
 */
private[converters] object VariantAnnotationConverter extends Serializable {

  /**
   * Converts an object into a Java Integer.
   *
   * @param attr Attribute to convert.
   * @return Attribute as java.lang.Integer.
   */
  private def attrAsInt(attr: Object): Object = attr match {
    case a: String            => java.lang.Integer.valueOf(a)
    case a: java.lang.Integer => a
    case a: java.lang.Number  => java.lang.Integer.valueOf(a.intValue)
  }

  /**
   * Converts an object into a Java Long.
   *
   * @param attr Attribute to convert.
   * @return Attribute as java.lang.Long.
   */
  private def attrAsLong(attr: Object): Object = attr match {
    case a: String           => java.lang.Long.valueOf(a)
    case a: java.lang.Long   => a
    case a: java.lang.Number => java.lang.Long.valueOf(a.longValue)
  }

  /**
   * Converts an object into a Java Float.
   *
   * @param attr Attribute to convert.
   * @return Attribute as java.lang.Float.
   */
  private def attrAsFloat(attr: Object): Object = attr match {
    case a: String           => java.lang.Float.valueOf(a)
    case a: java.lang.Float  => a
    case a: java.lang.Number => java.lang.Float.valueOf(a.floatValue)
  }

  /**
   * Converts an object into a Java String.
   *
   * @param attr Attribute to convert.
   * @return Attribute as a String.
   */
  private def attrAsString(attr: Object): Object = {
    attr.toString
  }

  /**
   * Converts an object into a boolean.
   *
   * @param attr Attribute to convert.
   * @return Attribute as a boolean.
   */
  private def attrAsBoolean(attr: Object): Object = attr match {
    case a: java.lang.Boolean => a
    case a: String            => java.lang.Boolean.valueOf(a)
  }

  /**
   * Reserved VCF INFO header line keys, from VCF v4.3 specification.
   */
  val RESERVED_INFO_KEYS: Seq[AttrKey] = Seq(
    // TribbleException: Couldn't find a standard VCF header line for field AA
    //AttrKey("ancestralAllele", attrAsString _, VCFStandardHeaderLines.getInfoLine(VCFConstants.ANCESTRAL_ALLELE_KEY)),
    AttrKey("ancestralAllele", attrAsString _, new VCFInfoHeaderLine(VCFConstants.ANCESTRAL_ALLELE_KEY, 1, VCFHeaderLineType.String, "Ancestral allele")),

    AttrKey("alleleCount", attrAsInt _, VCFStandardHeaderLines.getInfoLine(VCFConstants.ALLELE_COUNT_KEY)),

    // TribbleException: Couldn't find a standard VCF header line for field AD
    //AttrKey("readDepth", attrAsInt _, VCFStandardHeaderLines.getInfoLine("AD")), // VCF 4.3 spec, not in htsjdk
    //AttrKey("forwardReadDepth", attrAsInt _, VCFStandardHeaderLines.getInfoLine("ADF")), // VCF 4.3 spec, not in htsjdk
    //AttrKey("reverseReadDepth", attrAsInt _, VCFStandardHeaderLines.getInfoLine("ADR")), // VCF 4.3 spec, not in htsjdk
    AttrKey("readDepth", attrAsInt _, new VCFInfoHeaderLine("AD", 1, VCFHeaderLineType.Integer, "Total read depth for each allele")),
    AttrKey("forwardReadDepth", attrAsInt _, new VCFInfoHeaderLine("ADF", 1, VCFHeaderLineType.Integer, "Read depth for each allele on the forward strand")),
    AttrKey("reverseReadDepth", attrAsInt _, new VCFInfoHeaderLine("ADR", 1, VCFHeaderLineType.Integer, "Read depth for each allele on the reverse strand")),

    AttrKey("alleleFrequency", attrAsString _, VCFStandardHeaderLines.getInfoLine(VCFConstants.ALLELE_FREQUENCY_KEY)),

    // TribbleException: Couldn't find a standard VCF header line for field BQ
    //AttrKey("rmsBaseQuality", attrAsFloat _, VCFStandardHeaderLines.getInfoLine(VCFConstants.RMS_BASE_QUALITY_KEY)),
    AttrKey("rmsBaseQuality", attrAsFloat _, new VCFInfoHeaderLine(VCFConstants.RMS_BASE_QUALITY_KEY, 1, VCFHeaderLineType.Float, "RMS base quality at this position")),

    // TribbleException: Couldn't find a standard VCF header line for field CIGAR
    //AttrKey("cigar", attrAsString _, VCFStandardHeaderLines.getInfoLine(VCFConstants.CIGAR_KEY)),
    AttrKey("cigar", attrAsString _, new VCFInfoHeaderLine(VCFConstants.CIGAR_KEY, 1, VCFHeaderLineType.String, "CIGAR string describing how to align an alternate allele to the reference allele")),

    AttrKey("dbSnp", attrAsBoolean _, VCFStandardHeaderLines.getInfoLine(VCFConstants.DBSNP_KEY)),
    AttrKey("combinedDepth", attrAsInt _, VCFStandardHeaderLines.getInfoLine(VCFConstants.DEPTH_KEY)),

    // TribbleException: Couldn't find a standard VCF header line for field H2
    //AttrKey("hapMap2", attrAsBoolean _, VCFStandardHeaderLines.getInfoLine(VCFConstants.HAPMAP2_KEY)),
    AttrKey("hapMap2", attrAsBoolean _, new VCFInfoHeaderLine(VCFConstants.HAPMAP2_KEY, 1, VCFHeaderLineType.Flag, "Membership in HapMap2")),
    // TribbleException: Couldn't find a standard VCF header line for field H3
    //AttrKey("hapMap3", attrAsBoolean _, VCFStandardHeaderLines.getInfoLine(VCFConstants.HAPMAP3_KEY)),
    AttrKey("hapMap3", attrAsBoolean _, new VCFInfoHeaderLine(VCFConstants.HAPMAP3_KEY, 1, VCFHeaderLineType.Flag, "Membership in HapMap3")),

    AttrKey("rmsMappingQuality", attrAsFloat _, VCFStandardHeaderLines.getInfoLine(VCFConstants.RMS_MAPPING_QUALITY_KEY)),
    AttrKey("mappingQualityZeroReads", attrAsInt _, VCFStandardHeaderLines.getInfoLine(VCFConstants.MAPPING_QUALITY_ZERO_KEY)),

    // TribbleException: Couldn't find a standard VCF header line for field NS
    //AttrKey("samplesWithData", attrAsInt _, VCFStandardHeaderLines.getInfoLine(VCFConstants.SAMPLE_NUMBER_KEY)),
    AttrKey("samplesWithData", attrAsInt _, new VCFInfoHeaderLine(VCFConstants.SAMPLE_NUMBER_KEY, 1, VCFHeaderLineType.Integer, "Number of samples with data")),

    AttrKey("strandBias", attrAsFloat _, VCFStandardHeaderLines.getInfoLine(VCFConstants.STRAND_BIAS_KEY)),
    // TribbleException: Couldn't find a standard VCF header line for field VALIDATED
    //AttrKey("validated", attrAsBoolean _, VCFStandardHeaderLines.getInfoLine(VCFConstants.VALIDATED_KEY)),
    AttrKey("validated", attrAsBoolean _, new VCFInfoHeaderLine(VCFConstants.VALIDATED_KEY, 1, VCFHeaderLineType.Flag, "Validated by follow-up experiment")),

    // TribbleException: Couldn't find a standard VCF header line for field 1000G
    //AttrKey("thousandGenomes", attrAsBoolean _, VCFStandardHeaderLines.getInfoLine(VCFConstants.THOUSAND_GENOMES_KEY))
    AttrKey("thousandGenomes", attrAsBoolean _, new VCFInfoHeaderLine(VCFConstants.THOUSAND_GENOMES_KEY, 1, VCFHeaderLineType.Flag, "Membership in 1000 Genomes"))
  )

  /**
   * Miscellaneous other info keys.
   */
  val INFO_KEYS: Seq[AttrKey] = Seq(
    AttrKey("mqRankSum", attrAsFloat _, new VCFInfoHeaderLine("MQRankSum", 1, VCFHeaderLineType.Float, "Z-score From Wilcoxon rank sum test of Alt vs. Ref read mapping qualities")),
    AttrKey("readPositionRankSum", attrAsFloat _, new VCFInfoHeaderLine("ReadPosRankSum", 1, VCFHeaderLineType.Float, "Z-score from Wilcoxon rank sum test of Alt vs. Ref read position bias")),
    AttrKey("vqslod", attrAsFloat _, new VCFInfoHeaderLine("VQSLOD", 1, VCFHeaderLineType.Float, "Log odds ratio of being a true variant versus being false under the trained gaussian mixture model")),
    AttrKey("culprit", attrAsString _, new VCFInfoHeaderLine("culprit", 1, VCFHeaderLineType.String, "The annotation which was the worst performing in the Gaussian mixture model, likely the reason why the variant was filtered out")),
    AttrKey("fisherStrandBiasPValue", attrAsFloat _, new VCFInfoHeaderLine("FS", 1, VCFHeaderLineType.Float, "Phred-scaled p-value using Fisher's exact test to detect strand bias"))
  )

  /**
   * Format keys seen in VCF.
   */
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
    AttrKey("strandBiasComponents", attrAsInt _, new VCFFormatHeaderLine("SB", 4, VCFHeaderLineType.Integer, "Per-sample component statistics which comprise the Fisher's Exact Test to detect strand bias."))
  )

  /**
   * Mapping between VCF format field names and field IDs in the Genotype schema.
   */
  lazy val VCF2Genotype: Map[String, (Int, Object => Object)] =
    createFieldMap(FORMAT_KEYS, Genotype.getClassSchema)

  /**
   * Mapping between VCF format field names and field IDs in the GenotypeAnnotation schema.
   */
  lazy val VCF2GenotypeAnnotation: Map[String, (Int, Object => Object)] =
    createFieldMap(FORMAT_KEYS, GenotypeAnnotation.getClassSchema)

  /**
   * Mapping betweem VCF info field names and fields IDs in the VariantAnnotation schema.
   */
  lazy val VCF2VariantAnnotation: Map[String, (Int, Object => Object)] =
    createFieldMap((RESERVED_INFO_KEYS ++ INFO_KEYS), VariantAnnotation.getClassSchema)

  /**
   * Creates a mapping between a Seq of attribute keys, and the field ID for
   * that key in Avro.
   *
   * @param keys The ID names for all keys that will map into a given Avro
   *   record.
   * @param schema The schema for that record.
   * @return Returns a map where keys are the VCF ID for the field, and values
   *   are tuples of (Avro schema field ID, conversion method).
   */
  private def createFieldMap(keys: Seq[AttrKey],
                             schema: Schema): Map[String, (Int, Object => Object)] = {
    keys.filter(_.attrConverter != null).map(field => {
      val avroField = schema.getField(field.adamKey)
      field.vcfKey -> ((avroField.pos, field.attrConverter))
    })(collection.breakOut)
  }

  /**
   * Extracts fields from a htsjdk Variant context, and uses a field map
   * to construct an avro record.
   *
   * Given a programmatically defined mapping between a VCF attribute and
   * a field in an Avro specific record, this fills values into said record.
   *
   * @tparam T A class defined using an Avro schema.
   *
   * @see createFieldMap
   *
   * @param fieldMap Mapping between VCF field ID and Avro field IDs.
   * @param vc htsjdk Variant context describing a site.
   * @param record The Avro record to fill in.
   * @return Returns a new Avro record based on the input record with fields
   *   filled in.
   */
  private def fillRecord[T <% SpecificRecord](fieldMap: Map[String, (Int, Object => Object)],
                                              vc: VariantContext,
                                              record: T): T = {
    for ((v, a) <- fieldMap) {
      val attr = vc.getAttribute(v)
      if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
        record.put(a._1, a._2(attr))
      }
    }
    record
  }

  /**
   * Remaps fields from an htsjdk variant context into a genotype annotation.
   *
   * @param vc htsjdk variant context for a site.
   * @param annotation genotype annotation for a sample at a site.
   * @return Returns the genotype annotation with values filled in.
   */
  def convert(vc: VariantContext, annotation: GenotypeAnnotation): GenotypeAnnotation = {
    fillRecord(VCF2GenotypeAnnotation, vc, annotation)
  }

  /**
   * Remaps fields from an htsjdk variant context into a variant annotation.
   *
   * @param vc htsjdk variant context for a site.
   * @param annotation Pre-populated site annotation in Avro.
   * @return Annotation with additional info fields filled in.
   */
  def convert(vc: VariantContext, annotation: VariantAnnotation): VariantAnnotation = {
    fillRecord(VCF2VariantAnnotation, vc, annotation)
  }

  /**
   * Extracts fields from an htsjdk genotype and remaps them into an Avro
   * genotype.
   *
   * @param g htsjdk genotype for a sample at a site.
   * @param genotype A pre-populated Avro genotype for said sample.
   * @return Returns the Avro genotype with additional info remapped from
   *   the htsjdk genotype.
   */
  def convert(g: htsjdk.variant.variantcontext.Genotype,
              genotype: Genotype): Genotype = {
    for ((v, a) <- VariantAnnotationConverter.VCF2GenotypeAnnotation) {
      // Add extended attributes if present
      val attr = g.getExtendedAttribute(v)
      if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
        genotype.put(a._1, a._2(attr))
      }
    }
    genotype
  }
}
