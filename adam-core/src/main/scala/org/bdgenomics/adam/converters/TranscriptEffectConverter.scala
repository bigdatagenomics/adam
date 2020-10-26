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

import grizzled.slf4j.Logging
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFConstants
import htsjdk.variant.variantcontext.VariantContext
import org.bdgenomics.formats.avro.VariantAnnotationMessage._
import org.bdgenomics.formats.avro.{
  TranscriptEffect,
  Variant,
  VariantAnnotationMessage
}
import scala.collection.JavaConverters._

/**
 * Convert between htsjdk VCF INFO reserved key "ANN" values and TranscriptEffects.
 */
private[adam] object TranscriptEffectConverter extends Serializable with Logging {

  /**
   * Look up variant annotation messages by name and message code.
   */
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

  /**
   * Split effects by <code>&amp;</code> character.
   *
   * @param s effects to split
   * @return effects split by <code>&amp;</code> character
   */
  private def parseEffects(s: String): List[String] = {
    s.split("&").toList
  }

  /**
   * Split variant effect messages by <code>&amp;</code> character.
   *
   * @param s variant effect messages to split
   * @return variant effect messages split by <code>&amp;</code> character
   */
  private def parseMessages(s: String): List[VariantAnnotationMessage] = {
    // todo: haven't seen a delimiter here, assuming it is also '&'
    s.split("&").map(MESSAGES.get(_)).toList.flatten
  }

  /**
   * Ensembl VEP incorrectly supplies an interval instead of a position
   * for some attributes; in these cases use the interval start as the position.
   *
   * @param s position or interval
   * @return position or interval start
   */
  private def positionOrIntervalStart(s: String): Int = {
    val tokens = s.split("-")
    Integer.parseInt(tokens(0))
  }

  /**
   * Split a single or fractional value into optional numerator and denominator values.
   *
   * @param s single or fractional value to split
   * @return single or fractional value split into optional numerator and denominator values
   */
  private def parseFraction(s: String): (Option[Integer], Option[Integer]) = {
    if ("".equals(s)) {
      return (None, None)
    }
    val tokens = s.split("/")
    tokens.length match {
      case 0 => (None, None)
      case 1 => (Some(positionOrIntervalStart(tokens(0))), None)
      case _ => (Some(positionOrIntervalStart(tokens(0))), Some(Integer.parseInt(tokens(1))))
    }
  }

  /**
   * Set a value via a function if the value is not empty.
   *
   * @param s value to set
   * @param setFn function to call if the value is not empty
   */
  private def setIfNotEmpty(s: String, setFn: String => Unit) {
    Option(s).filter(_.nonEmpty).foreach(setFn)
  }

  /**
   * Parse zero or one transcript effects from the specified string value.
   *
   * @param s value to parse
   * @param stringency validation stringency
   * @return zero or one transcript effects parsed from the specified string value
   */
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
    if (effects.nonEmpty) te.setEffects(effects.asJava)
    // note: annotationImpact is output by SnpEff but is not part of the VCF ANN specification
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
    if (messages.nonEmpty) te.setMessages(messages.asJava)

    Seq(te.build())
  }

  /**
   * Parse the VCF INFO reserved key "ANN" value into zero or more TranscriptEffects.
   *
   * @param ann VCF INFO reserved key "ANN" value
   * @param stringency validation stringency
   * @return the VCF INFO reserved key "ANN" value parsed into zero or more TranscriptEffects
   */
  private[converters] def parseAnn(
    ann: Iterable[String],
    stringency: ValidationStringency): List[TranscriptEffect] = {

    ann.map(parseTranscriptEffect(_, stringency)).flatten.toList
  }

  /**
   * Convert the htsjdk VCF INFO reserved key "ANN" value into zero or more TranscriptEffects,
   * matching on alternate allele.
   *
   * @param variant variant
   * @param vc htsjdk variant context
   * @param stringency validation stringency, defaults to strict
   * @return the htsjdk VCF INFO reserved key "ANN" value converted into zero or more
   *    TranscriptEffects, matching on alternate allele, and wrapped in an option
   */
  def convertToTranscriptEffects(
    variant: Variant,
    vc: VariantContext,
    stringency: ValidationStringency = ValidationStringency.STRICT): Option[List[TranscriptEffect]] = {

    def parseAndFilter(attr: Iterable[String]): Option[List[TranscriptEffect]] = {
      if (attr.isEmpty) {
        None
      } else {
        val filtered = parseAnn(attr, stringency)
          .filter(_.getAlternateAllele == variant.getAlternateAllele)
        if (filtered.isEmpty) {
          None
        } else {
          Some(filtered)
        }
      }
    }

    val attrOpt = Option(vc.getAttributeAsList("ANN"))
    try {
      attrOpt.flatMap(attr => parseAndFilter(attr.asScala.map(_.asInstanceOf[java.lang.String])))
    } catch {
      case t: Throwable => {
        if (stringency == ValidationStringency.STRICT) {
          throw t
        } else if (stringency == ValidationStringency.LENIENT) {
          warn("Could not convert VCF INFO reserved key ANN value to TranscriptEffect, caught %s.".format(t))
        }
        None
      }
    }
  }

  /**
   * Convert the specified transcript effects into a string suitable for a VCF INFO reserved
   * key "ANN" value.
   *
   * @param effects zero or more transcript effects
   * @return the specified transcript effects converted into a string suitable for a VCF INFO
   *    reserved key "ANN" value
   */
  def convertToVcfInfoAnnValue(effects: Iterable[TranscriptEffect]): String = {
    def toFraction(numerator: java.lang.Integer, denominator: java.lang.Integer): String = {
      val numOpt = Option(numerator)
      val denomOpt = Option(denominator)

      (numOpt, denomOpt) match {
        case (None, None) => {
          ""
        }
        case (Some(n), None) => {
          "%d".format(n)
        }
        case (None, Some(d)) => {
          warn("Incorrect fractional value ?/%d, missing numerator".format(d))
          ""
        }
        case (Some(n), Some(d)) => {
          "%d/%d".format(n, d)
        }
      }
    }

    def toAnn(te: TranscriptEffect): String = {
      Seq(
        Option(te.getAlternateAllele).getOrElse(""), // 0
        te.getEffects.asScala.mkString("&"), // 1
        "", // annotationImpact
        Option(te.getGeneName).getOrElse(""), // 3
        Option(te.getGeneId).getOrElse(""),
        Option(te.getFeatureType).getOrElse(""),
        Option(te.getFeatureId).getOrElse(""),
        Option(te.getBiotype).getOrElse(""),
        toFraction(te.getRank, te.getTotal), // 8
        Option(te.getTranscriptHgvs).getOrElse(""), // 9
        Option(te.getProteinHgvs).getOrElse(""), // 10
        toFraction(te.getCdnaPosition, te.getCdnaLength), // 11
        toFraction(te.getCdsPosition, te.getCdsLength), // 12
        toFraction(te.getProteinPosition, te.getProteinLength), // 13
        Option(te.getDistance).getOrElse(""),
        te.getMessages.asScala.mkString("&")
      ).mkString("|")
    }

    effects.map(toAnn).mkString(",")
  }
}
