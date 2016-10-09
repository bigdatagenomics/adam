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

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf._
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.bdgenomics.formats.avro.{
  DatabaseVariantAnnotation,
  Genotype,
  VariantCallingAnnotations
}
import scala.collection.JavaConversions._

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
   * Converts a java String of comma delimited integers to a
   * java.util.List of Integer
   *
   * @param attr Attribute to convert.
   * @return Attribute as a java.util.List[Integer]
   */
  private def attrAsIntList(attr: Object): Object = attr match {
    case a: java.lang.String => seqAsJavaList(a.split(",").map(java.lang.Integer.valueOf _))
  }

  /**
   * Keys corresponding to the COSMIC mutation database.
   */
  val COSMIC_KEYS: List[AttrKey] = List(
    AttrKey("geneSymbol", attrAsString _, new VCFInfoHeaderLine("GENE,", 1, VCFHeaderLineType.String, "Gene name")),
    AttrKey("strand", attrAsString _, new VCFInfoHeaderLine("STRAND,", 1, VCFHeaderLineType.String, "Gene strand")),
    AttrKey("cds", attrAsString _, new VCFInfoHeaderLine("CDS,", 1, VCFHeaderLineType.String, "CDS annotation")),
    AttrKey("cnt", attrAsString _, new VCFInfoHeaderLine("CNT,", 1, VCFHeaderLineType.Integer, "How many samples have this mutation"))
  )

  /**
   * Keys corresponding to dbNSFP, which contains functional annotations for
   * non-synomymous SNPs.
   */
  val DBNSFP_KEYS: List[AttrKey] = List(
    AttrKey("phylop", attrAsFloat _, new VCFInfoHeaderLine("PHYLOP", 1, VCFHeaderLineType.Float, "PhyloP score. The larger the score, the more conserved the site.")),
    AttrKey("siftPred", attrAsString _, new VCFInfoHeaderLine("SIFT_PRED", 1, VCFHeaderLineType.Character, "SIFT Prediction: D (damaging), T (tolerated)")),
    AttrKey("siftScore", attrAsFloat _, new VCFInfoHeaderLine("SIFT_SCORE", 1, VCFHeaderLineType.Float, "SIFT Score")),
    AttrKey("ancestralAllele", attrAsString _, new VCFInfoHeaderLine("AA", 1, VCFHeaderLineType.String, "Ancestral allele"))
  )

  /**
   * Keys corresponding to Clinvar, which represents variants seen in clinical
   * medicine.
   */
  val CLINVAR_KEYS: List[AttrKey] = List(
    AttrKey("dbSnpId", attrAsInt _, new VCFInfoHeaderLine("dbSNP ID", 1, VCFHeaderLineType.Integer, "dbSNP ID")),
    AttrKey("geneSymbol", attrAsString _, new VCFInfoHeaderLine("GENEINFO", 1, VCFHeaderLineType.String, "Pairs each of gene symbol:gene id.  The gene symbol and id are delimited by a colon (:) and each pair is delimited by a vertical bar"))
  )

  /**
   * Keys corresponding to the online encyclopedia of Mendelian inheritance in
   * man, a database tracking hereditary disease and variants.
   */
  val OMIM_KEYS: List[AttrKey] = List(
    AttrKey("omimId", attrAsString _, new VCFInfoHeaderLine("VAR", 1, VCFHeaderLineType.String, "MIM entry with variant mapped to rsID"))
  )

  /**
   * Miscellaneous other info keys.
   */
  val INFO_KEYS: Seq[AttrKey] = Seq(
    AttrKey("rmsMapQ", attrAsFloat _, VCFStandardHeaderLines.getInfoLine(VCFConstants.RMS_MAPPING_QUALITY_KEY)),
    AttrKey("mapq0Reads", attrAsInt _, VCFStandardHeaderLines.getInfoLine(VCFConstants.MAPPING_QUALITY_ZERO_KEY)),
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
    AttrKey("strandBiasComponents", attrAsIntList _, new VCFFormatHeaderLine("SB", 4, VCFHeaderLineType.Integer, "Per-sample component statistics which comprise the Fisher's Exact Test to detect strand bias."))
  )

  /**
   * Mapping betweem VCF info field names and fields IDs in the
   * VariantCallingAnnotations schema.
   */
  lazy val VCF2VariantCallingAnnotations: Map[String, (Int, Object => Object)] =
    createFieldMap(INFO_KEYS, VariantCallingAnnotations.getClassSchema)

  /**
   * Mapping between VCF format field names and field IDs in the Genotype schema.
   */
  lazy val VCF2GenotypeAnnotations: Map[String, (Int, Object => Object)] =
    createFieldMap(FORMAT_KEYS, Genotype.getClassSchema)

  /**
   * All external database keys, concatenated.
   */
  private lazy val EXTERNAL_DATABASE_KEYS: Seq[AttrKey] = (OMIM_KEYS :::
    CLINVAR_KEYS :::
    DBNSFP_KEYS) // ::: COSMIC_KEYS

  /**
   * Mapping between VCF info field names and DatabaseVariantAnnotation schema
   * field IDs for all database specific fields.
   */
  lazy val VCF2DatabaseAnnotations: Map[String, (Int, Object => Object)] = createFieldMap(EXTERNAL_DATABASE_KEYS,
    DatabaseVariantAnnotation.getClassSchema)

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
   * Remaps fields from an htsjdk variant context into a site annotation.
   *
   * @param vc htsjdk variant context for a site.
   * @param annotation Pre-populated site annotation in Avro.
   * @return Annotation with additional info fields filled in.
   */
  def convert(vc: VariantContext,
              annotation: DatabaseVariantAnnotation): DatabaseVariantAnnotation = {
    fillRecord(VCF2DatabaseAnnotations, vc, annotation)
  }

  /**
   * Remaps fields from an htsjdk variant context into variant calling
   * annotations.
   *
   * @param vc htsjdk variant context for a site.
   * @param call Call specific annotations for a sample at a site.
   * @return Returns the genotype annotations with values filled in.
   */
  def convert(vc: VariantContext,
              call: VariantCallingAnnotations): VariantCallingAnnotations = {
    fillRecord(VCF2VariantCallingAnnotations, vc, call)
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
    for ((v, a) <- VariantAnnotationConverter.VCF2GenotypeAnnotations) {
      // Add extended attributes if present
      val attr = g.getExtendedAttribute(v)
      if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
        genotype.put(a._1, a._2(attr))
      }
    }
    genotype
  }
}
