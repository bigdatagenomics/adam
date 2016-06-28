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

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFConstants
import htsjdk.variant.variantcontext.VariantContext
import org.bdgenomics.formats.avro.VariantAnnotationMessage._
import org.bdgenomics.formats.avro.{
  TranscriptEffect,
  Variant,
  VariantAnnotation,
  VariantAnnotationMessage
}
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConverters._

object VariantAnnotations extends Serializable with Logging {
  private val MESSAGES: Map[String, VariantAnnotationMessage] = Map(
    // name -> enum
    ERROR_CHROMOSOME_NOT_FOUND.name() -> ERROR_CHROMOSOME_NOT_FOUND,
    ERROR_OUT_OF_CHROMOSOME_RANGE.name() -> ERROR_OUT_OF_CHROMOSOME_RANGE,
    WARNING_REF_DOES_NOT_MATCH_GENOME.name() -> WARNING_REF_DOES_NOT_MATCH_GENOME,
    WARNING_SEQUENCE_NOT_AVAILABLE.name() -> WARNING_SEQUENCE_NOT_AVAILABLE,
    WARNING_TRANSCRIPT_INCOMPLETE.name() -> WARNING_TRANSCRIPT_INCOMPLETE,
    WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS.name() -> WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS,
    WARNING_TRANSCRIPT_NO_START_CODON.name() -> WARNING_TRANSCRIPT_NO_START_CODON,
    INFO_REALIGN_3_PRIME.name() -> INFO_REALIGN_3_PRIME,
    INFO_COMPOUND_ANNOTATION.name() -> INFO_COMPOUND_ANNOTATION,
    INFO_NON_REFERENCE_ANNOTATION.name() -> INFO_NON_REFERENCE_ANNOTATION,
    // message code -> enum
    "E1" -> ERROR_CHROMOSOME_NOT_FOUND,
    "E2" -> ERROR_OUT_OF_CHROMOSOME_RANGE,
    "W1" -> WARNING_REF_DOES_NOT_MATCH_GENOME,
    "W2" -> WARNING_SEQUENCE_NOT_AVAILABLE,
    "W3" -> WARNING_TRANSCRIPT_INCOMPLETE,
    "W4" -> WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS,
    "W5" -> WARNING_TRANSCRIPT_NO_START_CODON,
    "I1" -> INFO_REALIGN_3_PRIME,
    "I2" -> INFO_COMPOUND_ANNOTATION,
    "I3" -> INFO_NON_REFERENCE_ANNOTATION
  )

  private def parseEffects(s: String): List[String] = {
    s.split("&").toList
  }

  private def parseMessages(s: String): List[VariantAnnotationMessage] = {
    // todo: haven't seen a delimiter here, assuming it is also '&'
    s.split("&").map(MESSAGES.get(_)).toList.flatten
  }

  private def parseFraction(s: String): (Option[Integer], Option[Integer]) = {
    if ("".equals(s)) {
      return (None, None)
    }
    val tokens = s.split("/")
    tokens.length match {
      case 0 => (None, None)
      case 1 => (Some(Integer.parseInt(tokens(0))), None)
      case _ => (Some(Integer.parseInt(tokens(0))), Some(Integer.parseInt(tokens(1))))
    }
  }

  def setIfNotEmpty(s: String, setFn: String => Unit) {
    Option(s).filter(_.nonEmpty).foreach(setFn)
  }

  private[converters] def parseTranscriptEffect(
    s: String,
    stringency: ValidationStringency): Seq[TranscriptEffect] = {

    val fields = s.split("\\|", -1)
    if (fields.length < 16) {
      if (stringency == ValidationStringency.STRICT)
        throw new IllegalArgumentException(s"Invalid VCF ANN attribute, expected at least 16 fields, found ${fields.length}: $s")
      else
        return Seq()
    }

    // simple and ignored fields
    val (alternateAllele, annotationImpact, geneName, geneId, featureType,
      featureId, biotype, transcriptHgvs, proteinHgvs, distance) =
      (fields(0), fields(2), fields(3), fields(4), fields(5),
        fields(6), fields(7), fields(9), fields(10), fields(14))

    // multivalued fields
    val effects = parseEffects(fields(1))
    val (rank, total) = parseFraction(fields(8))
    val (cdnaPosition, cdnaLength) = parseFraction(fields(11))
    val (cdsPosition, cdsLength) = parseFraction(fields(12))
    val (proteinPosition, proteinLength) = parseFraction(fields(13))
    val messages = parseMessages(fields(15))

    val te = TranscriptEffect.newBuilder()
    setIfNotEmpty(alternateAllele, te.setAlternateAllele(_))
    if (!effects.isEmpty) te.setEffects(effects.asJava)
    setIfNotEmpty(geneName, te.setGeneName(_))
    setIfNotEmpty(geneId, te.setGeneId(_))
    setIfNotEmpty(featureType, te.setFeatureType(_))
    setIfNotEmpty(featureId, te.setFeatureId(_))
    setIfNotEmpty(biotype, te.setBiotype(_))
    rank.foreach(te.setRank(_))
    total.foreach(te.setTotal(_))
    setIfNotEmpty(transcriptHgvs, te.setTranscriptHgvs(_))
    setIfNotEmpty(proteinHgvs, te.setProteinHgvs(_))
    cdnaPosition.foreach(te.setCdnaPosition(_))
    cdnaLength.foreach(te.setCdnaLength(_))
    cdsPosition.foreach(te.setCdsPosition(_))
    cdsLength.foreach(te.setCdsLength(_))
    proteinPosition.foreach(te.setProteinPosition(_))
    proteinLength.foreach(te.setProteinLength(_))
    setIfNotEmpty(distance, (s: String) => te.setDistance(Integer.parseInt(s)))
    if (!messages.isEmpty) te.setMessages(messages.asJava)

    Seq(te.build())
  }

  private[converters] def parseAnn(
    s: String,
    stringency: ValidationStringency): List[TranscriptEffect] = {

    s.split(",").map(parseTranscriptEffect(_, stringency)).flatten.toList
  }

  def createVariantAnnotation(
    variant: Variant,
    vc: VariantContext,
    stringency: ValidationStringency = ValidationStringency.STRICT): VariantAnnotation = {

    val va = VariantAnnotation.newBuilder()
      .setVariant(variant)

    val attr = vc.getAttributeAsString("ANN", null)
    if (attr != null && attr != VCFConstants.MISSING_VALUE_v4) {
      va.setTranscriptEffects(parseAnn(attr, stringency).asJava)
    }

    va.build()
  }
}
