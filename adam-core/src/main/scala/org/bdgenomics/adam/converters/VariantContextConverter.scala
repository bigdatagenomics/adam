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

import com.google.common.base.Splitter
import com.google.common.collect.ImmutableList
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{
  Allele,
  Genotype => HtsjdkGenotype,
  GenotypeBuilder,
  GenotypesContext,
  GenotypeLikelihoods,
  VariantContext => HtsjdkVariantContext,
  VariantContextBuilder
}
import htsjdk.variant.vcf.{
  VCFCompoundHeaderLine,
  VCFConstants,
  VCFFormatHeaderLine,
  VCFHeader,
  VCFHeaderLine,
  VCFHeaderLineCount,
  VCFHeaderLineType,
  VCFInfoHeaderLine
}
import java.util.Collections
import org.apache.hadoop.conf.Configuration
import org.bdgenomics.utils.misc.{ Logging, MathUtils }
import org.bdgenomics.adam.models.{
  SequenceDictionary,
  VariantContext => ADAMVariantContext
}
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro._
import org.slf4j.Logger
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.{ Buffer, HashMap }

/**
 * Object for converting between htsjdk and ADAM VariantContexts.
 *
 * Handles Variant, Genotype, Allele, and various genotype annotation
 * conversions. Does not handle Variant annotations. Genotype annotations are
 * annotations in the VCF GT field while Variant annotations are annotations
 * contained in the VCF INFO field.
 */
object VariantContextConverter {

  /**
   * If set to true, this property will ensure that the variant.annotation field
   * in the Genotype record is populated after conversion from an htsjdk
   * VariantContext. By default, this property is false.
   */
  val nestAnnotationInGenotypesProperty = "org.bdgenomics.adam.converters.VariantContextConverter.NEST_ANN_IN_GENOTYPES"

  /**
   * Sets the value of the nest annotation in genotypes property.
   *
   * @param conf Hadoop configuration to set the property in.
   * @param populateNestedAnn If true, the nested field is populated.
   */
  def setNestAnnotationInGenotypesProperty(conf: Configuration,
                                           populateNestedAnn: Boolean) {
    conf.setBoolean(nestAnnotationInGenotypesProperty, populateNestedAnn)
  }

  /**
   * Gets the value of the nest annotation in genotypes property.
   *
   * @param conf Hadoop configuration to set the property in.
   * @return Returns whether or not to nest the variant annotation under each
   *   genotype record.
   */
  private[adam] def getNestAnnotationInGenotypesProperty(
    conf: Configuration): Boolean = {
    conf.getBoolean(nestAnnotationInGenotypesProperty, false)
  }

  /**
   * Representation for an unknown non-ref/symbolic allele in VCF.
   */
  private val NON_REF_ALLELE = Allele.create("<NON_REF>", false /* !Reference */ )

  /**
   * The index in the Avro genotype record for the splitFromMultiAllelec field.
   *
   * This field is true if the VCF site was not biallelic.
   */
  private lazy val splitFromMultiAllelicField = Genotype.SCHEMA$.getField("splitFromMultiAllelic")

  /**
   * One conversion method for each way of representing an Allele
   *
   * An htsjdk Allele can represent a reference or alternate allele call, or a
   * site where no call could be made. If the allele is an alternate allele, we
   * check to see if this matches the primary alt allele at the site before
   * deciding to tag it as a primary alt (Alt) or a secondary alt (OtherAlt).
   *
   * @param vc The underlying VariantContext for the site.
   * @param allele The allele we are converting.
   * @return The Avro representation for this allele.
   */
  private def convertAllele(vc: HtsjdkVariantContext, allele: Allele): GenotypeAllele = {
    if (allele.isNoCall) GenotypeAllele.NO_CALL
    else if (allele.isReference) GenotypeAllele.REF
    else if (allele == NON_REF_ALLELE || !vc.hasAlternateAllele(allele)) GenotypeAllele.OTHER_ALT
    else GenotypeAllele.ALT
  }

  /**
   * Converts an allele string from an avro Variant into an htsjdk Allele.
   *
   * @param allele String representation of the allele. If null, we return an
   *   empty option (None).
   * @param isRef True if this allele is the reference allele. Default is false.
   * @return If the allele is defined, returns a wrapped allele. Else, returns
   *   a None.
   */
  private def convertAlleleOpt(allele: String, isRef: Boolean = false): Option[Allele] = {
    if (allele == null) {
      None
    } else {
      Some(Allele.create(allele, isRef))
    }
  }

  private val OPT_NON_REF = Some(Allele.create("<NON_REF>", false))

  private def optNonRef(v: Variant, hasNonRefModel: Boolean): Option[Allele] = {
    if (hasNonRefModel || v.getAlternateAllele == null) {
      OPT_NON_REF
    } else {
      None
    }
  }

  /**
   * Converts the alleles in a variant into a Java collection of htsjdk alleles.
   *
   * @param v Avro model of the variant at a site.
   * @param hasNonRefModel Does this site have non-reference model likelihoods
   *   attached?
   * @return Returns a Java collection representing the reference allele and any
   *   alternate allele at the site.
   */
  private def convertAlleles(v: Variant, hasNonRefModel: Boolean): java.util.Collection[Allele] = {
    val asSeq = Seq(convertAlleleOpt(v.getReferenceAllele, true),
      convertAlleleOpt(v.getAlternateAllele),
      optNonRef(v, hasNonRefModel)).flatten

    asSeq
  }

  /**
   * Emits a list of htsjdk alleles for the alleles present at a genotyped site.
   *
   * Given an avro description of a Genotype, returns the variants called at the
   * site as a Java List of htsjdk alleles. This maps over all of the called
   * alleles at the site and returns their htsjdk representation.
   *
   * @param g The genotype call at a site for a single sample.
   * @return Returns the called alleles at this site.
   */
  private def convertAlleles(g: Genotype): java.util.List[Allele] = {
    var alleles = g.getAlleles
    if (alleles == null) return Collections.emptyList[Allele]
    else g.getAlleles.map {
      case GenotypeAllele.NO_CALL | GenotypeAllele.OTHER_ALT => Allele.NO_CALL
      case GenotypeAllele.REF                                => Allele.create(g.getVariant.getReferenceAllele, true)
      case GenotypeAllele.ALT                                => Allele.create(g.getVariant.getAlternateAllele)
    }
  }

  private[adam] def cleanAndMixInSupportedLines(
    headerLines: Seq[VCFHeaderLine],
    stringency: ValidationStringency,
    log: Logger): Seq[VCFHeaderLine] = {

    // dedupe
    val deduped = headerLines.distinct

    def auditLine(line: VCFCompoundHeaderLine,
                  defaultLine: VCFCompoundHeaderLine,
                  replaceFn: (String, VCFCompoundHeaderLine) => VCFCompoundHeaderLine): Option[VCFCompoundHeaderLine] = {
      if (line.getType != defaultLine.getType) {
        val msg = "Field type for provided header line (%s) does not match supported line (%s)".format(
          line, defaultLine)
        if (stringency == ValidationStringency.STRICT) {
          throw new IllegalArgumentException(msg)
        } else {
          if (stringency == ValidationStringency.LENIENT) {
            log.warn(msg)
          }
          Some(replaceFn("BAD_%s".format(line.getID), line))
        }
      } else {
        None
      }
    }

    // remove our supported header lines
    deduped.flatMap(line => line match {
      case fl: VCFFormatHeaderLine => {
        val key = fl.getID
        DefaultHeaderLines.formatHeaderLines
          .find(_.getID == key)
          .fold(Some(fl).asInstanceOf[Option[VCFCompoundHeaderLine]])(defaultLine => {
            auditLine(fl, defaultLine, (newId, oldLine) => {
              if (oldLine.getCountType == VCFHeaderLineCount.INTEGER) {
                new VCFFormatHeaderLine(newId,
                  oldLine.getCount,
                  oldLine.getType,
                  oldLine.getDescription)
              } else {
                new VCFFormatHeaderLine(newId,
                  oldLine.getCountType,
                  oldLine.getType,
                  oldLine.getDescription)
              }
            })
          })
      }
      case il: VCFInfoHeaderLine => {
        val key = il.getID
        DefaultHeaderLines.infoHeaderLines
          .find(_.getID == key)
          .fold(Some(il).asInstanceOf[Option[VCFCompoundHeaderLine]])(defaultLine => {
            auditLine(il, defaultLine, (newId, oldLine) => {
              if (oldLine.getCountType == VCFHeaderLineCount.INTEGER) {
                new VCFInfoHeaderLine(newId,
                  oldLine.getCount,
                  oldLine.getType,
                  oldLine.getDescription)
              } else {
                new VCFInfoHeaderLine(newId,
                  oldLine.getCountType,
                  oldLine.getType,
                  oldLine.getDescription)
              }
            })
          })
      }
      case l => {
        Some(l)
      }
    }) ++ DefaultHeaderLines.allHeaderLines
  }

  private[adam] def headerLines(header: VCFHeader): Seq[VCFHeaderLine] = {
    (header.getFilterLines ++
      header.getFormatHeaderLines ++
      header.getInfoHeaderLines ++
      header.getOtherHeaderLines).toSeq
  }

  def apply(headerLines: Seq[VCFHeaderLine],
            stringency: ValidationStringency,
            conf: Configuration): VariantContextConverter = {
    new VariantContextConverter(headerLines,
      stringency,
      getNestAnnotationInGenotypesProperty(conf))
  }
}

/**
 * This class converts VCF data to and from ADAM. This translation occurs at the
 * abstraction level of the htsjdk VariantContext which represents VCF data, and
 * at the ADAMVariantContext level, which aggregates ADAM
 * variant/genotype/annotation data together.
 *
 * If a genotype annotation has a corresponding set of fields in the VCF standard,
 * a conversion to/from the htsjdk VariantContext should be implemented in this
 * class.
 */
class VariantContextConverter(
    headerLines: Seq[VCFHeaderLine],
    stringency: ValidationStringency,
    setNestedAnnotationInGenotype: Boolean) extends Serializable with Logging {
  import VariantContextConverter._

  // format fns gatk --> bdg, extract fns bdg --> gatk
  private val variantFormatFn = makeVariantFormatFn(headerLines, stringency)
  private val variantExtractFn = makeVariantExtractFn(headerLines)
  private val genotypeFormatFn = makeGenotypeFormatFn(headerLines)
  private val genotypeExtractFn = makeGenotypeExtractFn(headerLines)

  /**
   * Converts a Scala float to a Java float.
   *
   * @param f Scala floating point value.
   * @return Java floating point value.
   */
  private def jFloat(f: Float): java.lang.Float = f

  /**
   * Converts a Scala double to a Java double.
   *
   * @param f Scala double precision floating point value.
   * @return Java double precision floating point value.
   */
  private def jDouble(f: Double): java.lang.Double = f

  private def genotypeVariant(coreVariant: Variant,
                              fullVariant: Variant): Variant = {
    if (setNestedAnnotationInGenotype) {
      fullVariant
    } else {
      coreVariant
    }
  }

  /**
   * Converts a GATK variant context into one or more ADAM variant context(s).
   *
   * @param vc GATK variant context to convert.
   * @return The specified GATK variant context converted into one or more ADAM variant context(s)
   */
  def convert(
    vc: HtsjdkVariantContext): Seq[ADAMVariantContext] = {

    try {
      vc.getAlternateAlleles.toList match {
        case List(NON_REF_ALLELE) | List() => {
          val (coreVariant, variant) = variantFormatFn(vc, None, 0, false)
          val v = genotypeVariant(coreVariant, variant)
          val genotypes = vc.getGenotypes.map(g => {
            genotypeFormatFn(g, v, NON_REF_ALLELE, 0, Some(1), false)
          })
          return Seq(ADAMVariantContext(variant, genotypes))
        }
        case List(allele) => {
          require(
            allele.isNonReference,
            "Assertion failed when converting: " + vc.toString
          )
          val (coreVariant, variant) = variantFormatFn(vc, Some(allele.getDisplayString), 0, false)
          val v = genotypeVariant(coreVariant, variant)
          val genotypes = vc.getGenotypes.map(g => {
            genotypeFormatFn(g, v, allele, 1, None, false)
          })
          return Seq(ADAMVariantContext(variant, genotypes))
        }
        case List(allele, NON_REF_ALLELE) => {
          require(
            allele.isNonReference,
            "Assertion failed when converting: " + vc.toString
          )
          val (coreVariant, variant) = variantFormatFn(vc, Some(allele.getDisplayString), 0, false)
          val v = genotypeVariant(coreVariant, variant)
          val genotypes = vc.getGenotypes.map(g => {
            genotypeFormatFn(g, v, allele, 1, Some(2), false)
          })
          return Seq(ADAMVariantContext(variant, genotypes))
        }
        case _ => {
          val vcb = new VariantContextBuilder(vc)

          // is the last allele the non-ref allele?
          val alleles = vc.getAlternateAlleles.toSeq
          val referenceModelIndex = if (alleles.nonEmpty && alleles.last == NON_REF_ALLELE) {
            Some(alleles.length)
          } else {
            None
          }
          val altAlleles = if (referenceModelIndex.isDefined) {
            alleles.dropRight(1)
          } else {
            alleles
          }

          return altAlleles.map(allele => {
            val idx = vc.getAlleleIndex(allele)
            require(idx >= 1, "Unexpected index for alternate allele: " + vc.toString)

            // variant annotations only contain values for alternate alleles so
            // we need to subtract one from real index
            val variantIdx = idx - 1
            val (coreVariant, variant) = variantFormatFn(vc,
              Some(allele.getDisplayString),
              variantIdx,
              true)
            val v = genotypeVariant(coreVariant, variant)
            val genotypes = vc.getGenotypes.map(g => {
              genotypeFormatFn(g, v, allele, idx, referenceModelIndex, true)
            })
            ADAMVariantContext(variant, genotypes)
          })
        }
      }
    } catch {
      case t: Throwable => {
        if (stringency == ValidationStringency.STRICT) {
          throw t
        } else {
          if (stringency == ValidationStringency.LENIENT) {
            log.warn("Caught exception %s when converting %s.".format(t, vc))
          }
          Seq.empty
        }
      }
    }
  }

  /**
   * Split the htsjdk variant context ID field into an array of names.
   *
   * @param vc htsjdk variant context
   * @return Returns an Option wrapping an array of names split from the htsjdk
   *    variant context ID field
   */
  private def splitIds(vc: HtsjdkVariantContext): Option[java.util.List[String]] = {
    if (vc.hasID()) {
      Some(ImmutableList.copyOf(vc.getID().split(VCFConstants.ID_FIELD_SEPARATOR)))
    } else {
      None
    }
  }

  /**
   * Join the array of variant names into a string for the htsjdk variant context ID field.
   *
   * @param variant variant
   * @return Returns an Option wrapping a string for the htsjdk variant context ID field joined
   *    from the array of variant names
   */
  private def joinNames(variant: Variant): Option[String] = {
    if (variant.getNames != null && variant.getNames.length > 0) {
      Some(variant.getNames.mkString(VCFConstants.ID_FIELD_SEPARATOR))
    } else {
      None
    }
  }

  // htsjdk --> variant format functions

  private[converters] def formatNames(
    vc: HtsjdkVariantContext,
    vb: Variant.Builder): Variant.Builder = {

    splitIds(vc).fold(vb)(vb.setNames(_))
  }

  private[converters] def formatQuality(
    vc: HtsjdkVariantContext,
    vb: Variant.Builder): Variant.Builder = {

    if (vc.hasLog10PError) {
      vb.setQuality(vc.getPhredScaledQual)
    } else {
      vb
    }
  }

  private[converters] def formatFilters(
    vc: HtsjdkVariantContext,
    vb: Variant.Builder): Variant.Builder = {

    vb.setFiltersApplied(vc.filtersWereApplied)
    if (vc.filtersWereApplied) {
      vb.setFiltersPassed(!vc.isFiltered)
    }
    if (vc.isFiltered) {
      vb.setFiltersFailed(new java.util.ArrayList(vc.getFilters))
    }
    vb
  }

  private val variantFormatFns: Iterable[(HtsjdkVariantContext, Variant.Builder) => Variant.Builder] = Iterable(
    formatNames(_, _),
    formatQuality(_, _),
    formatFilters(_, _)
  )

  // variant --> htsjdk extract functions

  private[converters] def extractNames(
    v: Variant,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    joinNames(v).fold(vcb.noID())(vcb.id(_))
  }

  private[converters] def extractQuality(
    v: Variant,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    if (v.getQuality != null) {
      vcb.log10PError(-v.getQuality / 10.0)
    } else {
      vcb
    }
  }

  private[converters] def extractFilters(
    v: Variant,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(v.getFiltersApplied)
      .filter(ft => ft)
      .map(applied => {
        Option(v.getFiltersPassed).map(passed => {
          if (passed) {
            vcb.passFilters
          } else {
            val failedFilters = v.getFiltersFailed
            require(failedFilters.nonEmpty,
              "Variant marked as filtered, but no failed filters listed in %s.".format(v))
            vcb.filters(failedFilters.toSet)
          }
        }).getOrElse({
          throw new IllegalArgumentException("Filters were applied but filters passed is null in %s.".format(v))
        })
      }).getOrElse(vcb.unfiltered)
  }

  private val variantExtractFns: Iterable[(Variant, VariantContextBuilder) => VariantContextBuilder] = Iterable(
    extractNames(_, _),
    extractQuality(_, _),
    extractFilters(_, _)
  )

  // htsjdk --> variant annotation format functions

  private[converters] def formatAncestralAllele(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    Option(vc.getAttributeAsString("AA", null))
      .fold(vab)(vab.setAncestralAllele(_))
  }

  private[converters] def formatDbSnp(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    Option(vc.getAttribute("DB").asInstanceOf[java.lang.Boolean])
      .fold(vab)(vab.setDbSnp(_))
  }

  private[converters] def formatHapMap2(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    Option(vc.getAttribute("H2").asInstanceOf[java.lang.Boolean])
      .fold(vab)(vab.setHapMap2(_))
  }

  private[converters] def formatHapMap3(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    Option(vc.getAttribute("H3").asInstanceOf[java.lang.Boolean])
      .fold(vab)(vab.setHapMap3(_))
  }

  private[converters] def formatValidated(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    Option(vc.getAttribute("VALIDATED").asInstanceOf[java.lang.Boolean])
      .fold(vab)(vab.setValidated(_))
  }

  private[converters] def formatThousandGenomes(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    Option(vc.getAttribute("1000G").asInstanceOf[java.lang.Boolean])
      .fold(vab)(vab.setThousandGenomes(_))
  }

  private[converters] def formatSomatic(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    // default somatic to false if unspecified
    Option(vc.getAttribute("SOMATIC").asInstanceOf[java.lang.Boolean])
      .fold(vab.setSomatic(false))(vab.setSomatic(_))
  }

  private[converters] def formatAlleleCount(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    val ac = vc.getAttributeAsList("AC")
    if (ac.size > index) {
      vab.setAlleleCount(toInt(ac.get(index)))
    }
    vab
  }

  private[converters] def formatAlleleFrequency(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    val af = vc.getAttributeAsList("AF")
    if (af.size > index) {
      vab.setAlleleFrequency(toFloat(af.get(index)))
    }
    vab
  }

  private[converters] def formatCigar(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    val cigar = vc.getAttributeAsList("CIGAR")
    if (cigar.size > index) {
      vab.setCigar(asString(cigar.get(index)))
    }
    vab
  }

  private[converters] def formatReadDepth(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    val ad = vc.getAttributeAsList("AD")
    if (ad.size > (index + 1)) {
      vab.setReferenceReadDepth(toInt(ad.get(0)))
      vab.setReadDepth(toInt(ad.get(index + 1)))
    }
    vab
  }

  private[converters] def formatForwardReadDepth(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    val adf = vc.getAttributeAsList("ADF")
    if (adf.size > (index + 1)) {
      vab.setReferenceForwardReadDepth(toInt(adf.get(0)))
      vab.setForwardReadDepth(toInt(adf.get(index + 1)))
    }
    vab
  }

  private[converters] def formatReverseReadDepth(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    val adr = vc.getAttributeAsList("ADR")
    if (adr.size > (index + 1)) {
      vab.setReferenceReverseReadDepth(toInt(adr.get(0)))
      vab.setReverseReadDepth(toInt(adr.get(index + 1)))
    }
    vab
  }

  private[converters] def formatTranscriptEffects(
    vc: HtsjdkVariantContext,
    vab: VariantAnnotation.Builder,
    v: Variant,
    index: Int): VariantAnnotation.Builder = {

    TranscriptEffectConverter.convertToTranscriptEffects(v, vc)
      .fold(vab)(vab.setTranscriptEffects(_))
  }

  private val variantAnnotationFormatFns: Iterable[(HtsjdkVariantContext, VariantAnnotation.Builder, Variant, Int) => VariantAnnotation.Builder] = Iterable(
    formatAncestralAllele(_, _, _, _),
    formatDbSnp(_, _, _, _),
    formatHapMap2(_, _, _, _),
    formatHapMap3(_, _, _, _),
    formatValidated(_, _, _, _),
    formatThousandGenomes(_, _, _, _),
    formatSomatic(_, _, _, _),
    formatAlleleCount(_, _, _, _),
    formatAlleleFrequency(_, _, _, _),
    formatCigar(_, _, _, _),
    formatReadDepth(_, _, _, _),
    formatForwardReadDepth(_, _, _, _),
    formatReverseReadDepth(_, _, _, _),
    formatTranscriptEffects(_, _, _, _)
  )

  // variant annotation --> htsjdk extract functions

  private[converters] def extractAncestralAllele(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getAncestralAllele).fold(vcb)(vcb.attribute("AA", _))
  }

  private[converters] def extractDbSnp(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getDbSnp).fold(vcb)(vcb.attribute("DB", _))
  }

  private[converters] def extractHapMap2(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getHapMap2).fold(vcb)(vcb.attribute("H2", _))
  }

  private[converters] def extractHapMap3(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getHapMap3).fold(vcb)(vcb.attribute("H3", _))
  }

  private[converters] def extractValidated(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getValidated).fold(vcb)(vcb.attribute("VALIDATED", _))
  }

  private[converters] def extractThousandGenomes(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getThousandGenomes).fold(vcb)(vcb.attribute("1000G", _))
  }

  private[converters] def extractSomatic(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getSomatic).fold(vcb)(vcb.attribute("SOMATIC", _))
  }

  private[converters] def extractAlleleCount(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getAlleleCount).fold(vcb)(i => vcb.attribute("AC", i.toString))
  }

  private[converters] def extractAlleleFrequency(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getAlleleFrequency).fold(vcb)(f => vcb.attribute("AF", f.toString))
  }

  private[converters] def extractCigar(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    Option(va.getCigar).fold(vcb)(vcb.attribute("CIGAR", _))
  }

  private[converters] def extractReadDepth(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    (Option(va.getReferenceReadDepth), Option(va.getReadDepth)) match {
      case (Some(ref), Some(alt)) => vcb.attribute("AD", ImmutableList.of(ref.toString, alt.toString))
      case (None, Some(_))        => throw new IllegalArgumentException("Read depth specified without reference read depth")
      case (Some(_), None)        => throw new IllegalArgumentException("Reference read depth specified without read depth")
      case _                      =>
    }
    vcb
  }

  private[converters] def extractForwardReadDepth(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    (Option(va.getReferenceForwardReadDepth), Option(va.getForwardReadDepth)) match {
      case (Some(ref), Some(alt)) => vcb.attribute("ADF", ImmutableList.of(ref.toString, alt.toString))
      case (None, Some(_))        => throw new IllegalArgumentException("Forward read depth specified without reference forward read depth")
      case (Some(_), None)        => throw new IllegalArgumentException("Reference forward read depth specified without forward read depth")
      case _                      =>
    }
    vcb
  }

  private[converters] def extractReverseReadDepth(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    (Option(va.getReferenceReverseReadDepth), Option(va.getReverseReadDepth)) match {
      case (Some(ref), Some(alt)) => vcb.attribute("ADR", ImmutableList.of(ref.toString, alt.toString))
      case (None, Some(_))        => throw new IllegalArgumentException("Reverse read depth specified without reference reverse read depth")
      case (Some(_), None)        => throw new IllegalArgumentException("Reference reverse read depth specified without reverse read depth")
      case _                      =>
    }
    vcb
  }

  private[converters] def extractTranscriptEffects(
    va: VariantAnnotation,
    vcb: VariantContextBuilder): VariantContextBuilder = {

    if (!va.getTranscriptEffects.isEmpty) {
      vcb.attribute("ANN", TranscriptEffectConverter.convertToVcfInfoAnnValue(va.getTranscriptEffects))
    }
    vcb
  }

  private val variantAnnotationExtractFns: Iterable[(VariantAnnotation, VariantContextBuilder) => VariantContextBuilder] = Iterable(
    extractAncestralAllele(_, _),
    extractDbSnp(_, _),
    extractHapMap2(_, _),
    extractHapMap3(_, _),
    extractValidated(_, _),
    extractThousandGenomes(_, _),
    extractSomatic(_, _),
    extractAlleleCount(_, _),
    extractAlleleFrequency(_, _),
    extractCigar(_, _),
    extractReadDepth(_, _),
    extractForwardReadDepth(_, _),
    extractReverseReadDepth(_, _),
    extractTranscriptEffects(_, _)
  )

  // htsjdk --> genotype format functions

  private[converters] def formatAllelicDepth(g: HtsjdkGenotype,
                                             gb: Genotype.Builder,
                                             gIdx: Int,
                                             gIndices: Array[Int]): Genotype.Builder = {

    // AD is an array type field
    if (g.hasAD && gIdx < g.getAD.size) {
      val ad = g.getAD
      gb.setReferenceReadDepth(ad(0))
        .setAlternateReadDepth(ad(gIdx))
    } else {
      gb
    }
  }

  private[converters] def formatReadDepth(g: HtsjdkGenotype,
                                          gb: Genotype.Builder,
                                          gIdx: Int,
                                          gIndices: Array[Int]): Genotype.Builder = {
    if (g.hasDP) {
      gb.setReadDepth(g.getDP)
    } else {
      gb
    }
  }

  private[converters] def formatMinReadDepth(g: HtsjdkGenotype,
                                             gb: Genotype.Builder,
                                             gIdx: Int,
                                             gIndices: Array[Int]): Genotype.Builder = {
    Option(g.getExtendedAttribute("MIN_DP", null))
      .map(attr => {
        tryAndCatchStringCast(attr, attribute => {
          gb.setMinReadDepth(attribute.asInstanceOf[java.lang.Integer])
        }, attribute => {
          gb.setMinReadDepth(attribute.toInt)
        })
      }).getOrElse(gb)
  }

  private[converters] def formatGenotypeQuality(g: HtsjdkGenotype,
                                                gb: Genotype.Builder,
                                                gIdx: Int,
                                                gIndices: Array[Int]): Genotype.Builder = {
    if (g.hasGQ) {
      gb.setGenotypeQuality(g.getGQ)
    } else {
      gb
    }
  }

  private[converters] def formatGenotypeLikelihoods(g: HtsjdkGenotype,
                                                    gb: Genotype.Builder,
                                                    gIdx: Int,
                                                    gIndices: Array[Int]): Genotype.Builder = {
    if (g.hasPL) {
      val pl = g.getPL
      try {
        val likelihoods = gIndices.map(idx => {
          jDouble(PhredUtils.phredToLogProbability(pl(idx)))
        }).toList
        gb.setGenotypeLikelihoods(likelihoods)
      } catch {
        case _: ArrayIndexOutOfBoundsException => {
          log.warn("Ran into Array Out of Bounds when accessing indices %s of genotype %s.".format(gIndices.mkString(","), g))
          gb
        }
      }
    } else {
      gb
    }
  }

  private[converters] def formatNonRefGenotypeLikelihoods(g: HtsjdkGenotype,
                                                          gb: Genotype.Builder,
                                                          gIndices: Array[Int]): Genotype.Builder = {
    if (g.hasPL) {
      val pl = g.getPL
      gb.setNonReferenceLikelihoods(gIndices.map(idx => {
        jDouble(PhredUtils.phredToLogProbability(pl(idx)))
      }).toList)
    } else {
      gb
    }
  }

  private def tryAndCatchStringCast[T](attr: java.lang.Object,
                                       tryFn: (java.lang.Object) => T,
                                       catchFn: (String) => T): T = {
    try {
      tryFn(attr)
    } catch {
      case cce: ClassCastException => {

        // is this a string? if so, parse...
        if (attr.getClass.isAssignableFrom(classOf[String])) {
          catchFn(attr.asInstanceOf[String])
        } else {
          throw cce
        }
      }
      case t: Throwable => throw t
    }
  }

  private[converters] def formatStrandBiasComponents(g: HtsjdkGenotype,
                                                     gb: Genotype.Builder,
                                                     gIdx: Int,
                                                     gIndices: Array[Int]): Genotype.Builder = {
    Option(g.getExtendedAttribute("SB"))
      .map(attr => {
        tryAndCatchStringCast(
          attr,
          attribute => {
            gb.setStrandBiasComponents(attribute.asInstanceOf[Array[java.lang.Integer]].toSeq)
          }, attribute => {
            val components = attribute.split(",")
            require(components.size == 4,
              "Strand bias components must have 4 entries. Saw %s in %s.".format(
                attr, g))

            gb.setStrandBiasComponents(components.map(e => e.toInt: java.lang.Integer)
              .toSeq)
          })
      }).getOrElse(gb)
  }

  private[converters] def formatPhaseInfo(g: HtsjdkGenotype,
                                          gb: Genotype.Builder,
                                          gIdx: Int,
                                          gIndices: Array[Int]): Genotype.Builder = {
    if (g.isPhased) {
      gb.setPhased(true)

      Option(g.getExtendedAttribute(VCFConstants.PHASE_SET_KEY))
        .map(attr => {
          tryAndCatchStringCast(attr, attribute => {
            gb.setPhaseSetId(attribute.asInstanceOf[java.lang.Integer])
          }, attribute => {
            gb.setPhaseSetId(attribute.toInt)
          })
        })

      Option(g.getExtendedAttribute(VCFConstants.PHASE_QUALITY_KEY))
        .map(attr => {
          tryAndCatchStringCast(attr, attribute => {
            gb.setPhaseQuality(attribute.asInstanceOf[java.lang.Integer])
          }, attribute => {
            gb.setPhaseQuality(attribute.toInt)
          })
        })
    }
    gb
  }

  private val genotypeFormatFns: Iterable[(HtsjdkGenotype, Genotype.Builder, Int, Array[Int]) => Genotype.Builder] = Iterable(
    formatAllelicDepth(_, _, _, _),
    formatReadDepth(_, _, _, _),
    formatMinReadDepth(_, _, _, _),
    formatGenotypeQuality(_, _, _, _),
    formatGenotypeLikelihoods(_, _, _, _),
    formatStrandBiasComponents(_, _, _, _),
    formatPhaseInfo(_, _, _, _)
  )

  // genotype --> htsjdk extract functions

  private[converters] def extractAllelicDepth(g: Genotype,
                                              gb: GenotypeBuilder): GenotypeBuilder = {
    (Option(g.getReferenceReadDepth), Option(g.getAlternateReadDepth)) match {
      case (Some(ref), Some(alt)) => gb.AD(Array(ref, alt))
      case (Some(_), None) => {
        throw new IllegalArgumentException("Had reference depth but no alternate depth in %s.".format(g))
      }
      case (None, Some(_)) => {
        throw new IllegalArgumentException("Had alternate depth but no reference depth in %s.".format(g))
      }
      case _ => gb.noAD
    }
  }

  private[converters] def extractReadDepth(g: Genotype,
                                           gb: GenotypeBuilder): GenotypeBuilder = {
    Option(g.getReadDepth).fold(gb.noDP)(dp => gb.DP(dp))
  }

  private[converters] def extractMinReadDepth(g: Genotype,
                                              gb: GenotypeBuilder): GenotypeBuilder = {
    Option(g.getMinReadDepth).fold(gb)(minDp => gb.attribute("MIN_DP", minDp))
  }

  private[converters] def extractGenotypeQuality(g: Genotype,
                                                 gb: GenotypeBuilder): GenotypeBuilder = {
    Option(g.getGenotypeQuality).fold(gb.noGQ)(gq => gb.GQ(gq))
  }

  private[converters] def numPls(copyNumber: Int): Int = {

    @tailrec def plCalculator(cn: Int, sum: Int = 0): Int = {
      if (cn == 0) {
        sum + 1
      } else {
        plCalculator(cn - 1, sum + cn + 1)
      }
    }

    plCalculator(copyNumber)
  }

  private[converters] def nonRefPls(gls: java.util.List[java.lang.Double],
                                    nls: java.util.List[java.lang.Double]): Array[Int] = {
    require(gls.length == nls.length,
      "Genotype likelihoods (%s) and non-reference likelihoods (%s) disagree on copy number".format(
        gls.mkString(","), nls.mkString(",")))
    val copyNumber = gls.length - 1
    val elements = numPls(copyNumber)

    val array = Array.fill(elements) { PhredUtils.minValue }

    (0 to copyNumber).foreach(idx => {
      array(idx) = PhredUtils.logProbabilityToPhred(gls.get(idx))
    })

    var cnIdx = copyNumber + 1
    (1 to copyNumber).foreach(idx => {
      array(cnIdx + 1) = PhredUtils.logProbabilityToPhred(nls.get(idx))
      cnIdx += (copyNumber - idx)
    })

    array
  }

  private[converters] def extractGenotypeLikelihoods(g: Genotype,
                                                     gb: GenotypeBuilder): GenotypeBuilder = {
    val gls = g.getGenotypeLikelihoods
    val nls = g.getNonReferenceLikelihoods

    if (gls.isEmpty) {
      if (g.getVariant == null ||
        g.getVariant.getAlternateAllele != null ||
        nls.isEmpty) {
        if (nls.nonEmpty) {
          log.warn("Expected empty non-reference likelihoods for genotype with empty likelihoods (%s).".format(g))
        }
        gb.noPL
      } else {
        gb.PL(nls.map(l => PhredUtils.logProbabilityToPhred(l))
          .toArray)
      }
    } else if (nls.isEmpty) {
      gb.PL(gls.map(l => PhredUtils.logProbabilityToPhred(l))
        .toArray)
    } else {
      gb.PL(nonRefPls(gls, nls))
    }
  }

  private[converters] def extractStrandBiasComponents(g: Genotype,
                                                      gb: GenotypeBuilder): GenotypeBuilder = {

    val components = g.getStrandBiasComponents

    if (components.isEmpty) {
      gb
    } else {
      require(components.size == 4,
        "Illegal strand bias components length. Must be empty or 4. In:\n%s".format(g))
      gb.attribute("SB", components.map(i => i: Int).toArray)
    }
  }

  private[converters] def extractPhaseInfo(g: Genotype,
                                           gb: GenotypeBuilder): GenotypeBuilder = {
    Option(g.getPhased)
      .filter(p => p)
      .map(p => {
        val setFns: Iterable[Option[(GenotypeBuilder => GenotypeBuilder)]] = Iterable(
          Option(g.getPhaseSetId).map(ps => {
            (b: GenotypeBuilder) => b.attribute("PS", ps)
          }),
          Option(g.getPhaseQuality).map(pq => {
            (b: GenotypeBuilder) => b.attribute("PQ", pq)
          }))

        setFns.flatten
          .foldLeft(gb.phased(true))((b, fn) => fn(b))
      }).getOrElse(gb.phased(false))
  }

  private val genotypeExtractFns: Iterable[(Genotype, GenotypeBuilder) => GenotypeBuilder] = Iterable(
    extractAllelicDepth(_, _),
    extractReadDepth(_, _),
    extractMinReadDepth(_, _),
    extractGenotypeQuality(_, _),
    extractGenotypeLikelihoods(_, _),
    extractStrandBiasComponents(_, _),
    extractPhaseInfo(_, _)
  )

  // htsjdk --> genotype annotation format functions

  private[converters] def formatFilters(g: HtsjdkGenotype,
                                        vcab: VariantCallingAnnotations.Builder,
                                        idx: Int,
                                        indices: Array[Int]): VariantCallingAnnotations.Builder = {
    // see https://github.com/samtools/htsjdk/issues/741
    val gtFiltersWereApplied = true
    if (gtFiltersWereApplied) {
      val filtersWereApplied = vcab.setFiltersApplied(true)
      if (g.isFiltered) {
        filtersWereApplied.setFiltersPassed(false)
          .setFiltersFailed(g.getFilters.split(";").toList)
      } else {
        filtersWereApplied.setFiltersPassed(true)
      }
    } else {
      vcab.setFiltersApplied(false)
    }
  }

  private[converters] def formatFisherStrandBias(g: HtsjdkGenotype,
                                                 vcab: VariantCallingAnnotations.Builder,
                                                 idx: Int,
                                                 indices: Array[Int]): VariantCallingAnnotations.Builder = {
    Option(g.getExtendedAttribute("FS"))
      .map(attr => {
        tryAndCatchStringCast(attr, attribute => {
          vcab.setFisherStrandBiasPValue(attribute.asInstanceOf[java.lang.Float])
        }, attribute => {
          vcab.setFisherStrandBiasPValue(toFloat(attribute))
        })
      }).getOrElse(vcab)
  }

  private[converters] def formatRmsMapQ(g: HtsjdkGenotype,
                                        vcab: VariantCallingAnnotations.Builder,
                                        idx: Int,
                                        indices: Array[Int]): VariantCallingAnnotations.Builder = {
    Option(g.getExtendedAttribute("MQ"))
      .map(attr => {
        tryAndCatchStringCast(attr, attribute => {
          vcab.setRmsMapQ(attribute.asInstanceOf[java.lang.Float])
        }, attribute => {
          vcab.setRmsMapQ(toFloat(attribute))
        })
      }).getOrElse(vcab)
  }

  private[converters] def formatMapQ0(g: HtsjdkGenotype,
                                      vcab: VariantCallingAnnotations.Builder,
                                      idx: Int,
                                      indices: Array[Int]): VariantCallingAnnotations.Builder = {
    Option(g.getExtendedAttribute("MQ0"))
      .map(attr => {
        tryAndCatchStringCast(attr, attribute => {
          vcab.setMapq0Reads(attribute.asInstanceOf[java.lang.Integer])
        }, attribute => {
          vcab.setMapq0Reads(attribute.toInt)
        })
      }).getOrElse(vcab)
  }

  private val genotypeAnnotationFormatFns: Iterable[(HtsjdkGenotype, VariantCallingAnnotations.Builder, Int, Array[Int]) => VariantCallingAnnotations.Builder] = Iterable(
    formatFilters(_, _, _, _),
    formatFisherStrandBias(_, _, _, _),
    formatRmsMapQ(_, _, _, _),
    formatMapQ0(_, _, _, _)
  )

  // genotype annotation --> htsjdk extract functions

  private[converters] def extractFilters(vca: VariantCallingAnnotations,
                                         gb: GenotypeBuilder): GenotypeBuilder = {
    Option(vca.getFiltersApplied)
      .filter(ft => ft)
      .map(applied => {
        Option(vca.getFiltersPassed).map(passed => {
          if (passed) {
            gb.filters("PASS")
          } else {
            val failedFilters = vca.getFiltersFailed
            require(failedFilters.nonEmpty,
              "Genotype marked as filtered, but no failed filters listed in %s.".format(vca))
            gb.filters(failedFilters.mkString(";"))
          }
        }).getOrElse({
          throw new IllegalArgumentException("Filters were applied but filters passed is null in %s.".format(vca))
        })
      }).getOrElse(gb.unfiltered())
  }

  private[converters] def extractFisherStrandBias(vca: VariantCallingAnnotations,
                                                  gb: GenotypeBuilder): GenotypeBuilder = {
    Option(vca.getFisherStrandBiasPValue).map(fs => {
      gb.attribute("FS", fs)
    }).getOrElse(gb)
  }

  private[converters] def extractRmsMapQ(vca: VariantCallingAnnotations,
                                         gb: GenotypeBuilder): GenotypeBuilder = {
    Option(vca.getRmsMapQ).map(mq => {
      gb.attribute("MQ", mq)
    }).getOrElse(gb)
  }

  private[converters] def extractMapQ0(vca: VariantCallingAnnotations,
                                       gb: GenotypeBuilder): GenotypeBuilder = {
    Option(vca.getMapq0Reads).map(mq0 => {
      gb.attribute("MQ0", mq0)
    }).getOrElse(gb)
  }

  private val genotypeAnnotationExtractFns: Iterable[(VariantCallingAnnotations, GenotypeBuilder) => GenotypeBuilder] = Iterable(
    extractFilters(_, _),
    extractFisherStrandBias(_, _),
    extractRmsMapQ(_, _),
    extractMapQ0(_, _)
  )

  // safe type conversions

  private def toBoolean(obj: java.lang.Object): Boolean = {
    tryAndCatchStringCast(obj, o => {
      o.asInstanceOf[java.lang.Boolean]
    }, o => o.toBoolean)
  }

  private def toInt(obj: java.lang.Object): Int = {
    tryAndCatchStringCast(obj, o => {
      o.asInstanceOf[java.lang.Integer]
    }, o => o.toInt)
  }

  private def toChar(obj: java.lang.Object): Char = {
    tryAndCatchStringCast(obj, o => {
      o.asInstanceOf[java.lang.Character]
    }, o => {
      require(o.length == 1, "Expected character to have length 1.")
      o(0)
    })
  }

  private def toFloat(obj: java.lang.Object): Float = {
    tryAndCatchStringCast(obj, o => {
      o.asInstanceOf[java.lang.Float]
    }, o => {
      if (o == "+Inf") {
        Float.PositiveInfinity
      } else if (o == "-Inf") {
        Float.NegativeInfinity
      } else if (o == "nan") {
        Float.NaN
      } else {
        o.toFloat
      }
    })
  }

  // don't shadow toString
  private def asString(obj: java.lang.Object): String = {
    obj.asInstanceOf[java.lang.String]
  }

  private def splitAndCheckForEmptyArray(s: String): Array[String] = {
    val array = s.split(",")
    if (array.forall(_ == ".")) {
      Array.empty
    } else {
      require(array.forall(_ != "."),
        "Array must either be fully defined or fully undefined.")
      array
    }
  }

  private def toIntArray(obj: java.lang.Object): Array[Int] = {
    tryAndCatchStringCast(obj, o => {
      o.asInstanceOf[Array[java.lang.Integer]]
        .map(i => i: Int)
    }, o => {
      splitAndCheckForEmptyArray(o).map(_.toInt)
    })
  }

  private def toCharArray(obj: java.lang.Object): Array[Char] = {
    tryAndCatchStringCast(obj, o => {
      o.asInstanceOf[Array[java.lang.Character]]
        .map(c => c: Char)
    }, o => {
      splitAndCheckForEmptyArray(o).map(s => {
        require(s.length == 1, "Expected character to have length 1.")
        s(0)
      })
    })
  }

  private def toFloatArray(obj: java.lang.Object): Array[Float] = {
    tryAndCatchStringCast(obj, o => {
      o.asInstanceOf[Array[java.lang.Float]]
        .map(f => f: Float)
    }, o => {
      splitAndCheckForEmptyArray(o).map(toFloat(_))
    })
  }

  private def toStringArray(obj: java.lang.Object): Array[String] = {
    tryAndCatchStringCast(obj, o => {
      o.asInstanceOf[Array[java.lang.String]]
        .map(s => s: String)
    }, o => splitAndCheckForEmptyArray(o))
  }

  private def filterArray[T](array: Array[T],
                             indices: List[Int]): List[T] = {
    if (indices.isEmpty) {
      array.toList
    } else if (indices.max < array.size) {
      indices.map(idx => array(idx))
    } else {
      List.empty
    }
  }

  private def arrayFieldExtractor(g: HtsjdkGenotype,
                                  id: String,
                                  toFn: (java.lang.Object => Array[String]),
                                  indices: List[Int]): Option[(String, List[String])] = {
    Option(g.getExtendedAttribute(id))
      .map(toFn)
      .filter(_.nonEmpty)
      .map(filterArray(_, indices))
      .map(v => (id, v))
  }

  private def fromArrayExtractor(g: HtsjdkGenotype,
                                 id: String,
                                 toFn: (java.lang.Object => Array[String]),
                                 idx: Int): Option[(String, String)] = {
    Option(g.getExtendedAttribute(id))
      .map(toFn)
      .filter(_.nonEmpty)
      .map(array => (id, array(idx)))
  }

  private def fieldExtractor(g: HtsjdkGenotype,
                             id: String,
                             toFn: (java.lang.Object => Any)): Option[(String, Any)] = {
    Option(g.getExtendedAttribute(id))
      .map(toFn)
      .map(attr => (id, attr))
  }

  private def arrayFieldExtractor(vc: HtsjdkVariantContext,
                                  id: String,
                                  toFn: (java.lang.Object => Array[String]),
                                  indices: List[Int]): Option[(String, List[String])] = {
    Option(vc.getAttribute(id))
      .map(toFn)
      .filter(_.nonEmpty)
      .map(filterArray(_, indices))
      .map(v => (id, v))
  }

  private def fromArrayExtractor(vc: HtsjdkVariantContext,
                                 id: String,
                                 toFn: (java.lang.Object => Array[String]),
                                 idx: Int): Option[(String, String)] = {
    Option(vc.getAttribute(id))
      .map(toFn)
      .filter(_.nonEmpty)
      .map(array => (id, array(idx)))
  }

  private def variantContextFieldExtractor(vc: HtsjdkVariantContext,
                                           id: String,
                                           toFn: (java.lang.Object => Any)): Option[(String, Any)] = {
    Option(vc.getAttribute(id))
      .filter(attr => attr match {
        case s: String => s != "."
        case _         => true
      }).map(toFn)
      .map(attr => (id, attr))
  }

  private def lineToVariantContextExtractor(
    headerLine: VCFInfoHeaderLine): Option[(HtsjdkVariantContext, Int, Array[Int]) => Option[(String, String)]] = {
    val id = headerLine.getID

    if (headerLine.isFixedCount && headerLine.getCount == 0 && headerLine.getType == VCFHeaderLineType.Flag) {
      Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
        {
          variantContextFieldExtractor(vc, id, toBoolean).map(kv => (kv._1, kv._2.toString))
        })
    } else if (headerLine.isFixedCount && headerLine.getCount == 1) {
      headerLine.getType match {
        // Flag header line types should be Number=0, but we'll allow Number=1
        case VCFHeaderLineType.Flag => {
          Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
            {
              variantContextFieldExtractor(vc, id, toBoolean).map(kv => (kv._1, kv._2.toString))
            })
        }
        case VCFHeaderLineType.Character => {
          Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
            {
              variantContextFieldExtractor(vc, id, toChar).map(kv => (kv._1, kv._2.toString))
            })
        }
        case VCFHeaderLineType.Float => {
          Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
            {
              variantContextFieldExtractor(vc, id, toFloat).map(kv => (kv._1, kv._2.toString))
            })
        }
        case VCFHeaderLineType.Integer => {
          Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
            {
              variantContextFieldExtractor(vc, id, toInt).map(kv => (kv._1, kv._2.toString))
            })
        }
        case VCFHeaderLineType.String => {
          Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
            {
              variantContextFieldExtractor(vc, id, asString).map(kv => (kv._1, kv._2.toString))
            })
        }
      }
    } else {
      def objToArray(obj: java.lang.Object): Array[String] = {
        try {
          val l: java.util.List[String] = obj.asInstanceOf[java.util.List[String]]
          // java.util.List has a conflicing toString. Get implicit to buffer first
          val sL: Buffer[String] = l
          sL.toArray
        } catch {
          case cce: ClassCastException => {
            // is this a string? if so, split
            if (obj.getClass.isAssignableFrom(classOf[String])) {
              obj.asInstanceOf[String].split(",")
            } else {
              throw cce
            }
          }
          case t: Throwable => {
            throw t
          }
        }
      }

      val toFn: (java.lang.Object => Array[String]) = headerLine.getType match {
        case VCFHeaderLineType.Flag => {
          throw new IllegalArgumentException("Multivalued flags are not supported for INFO lines: %s".format(
            headerLine))
        }
        case _ => {
          objToArray(_)
        }
      }

      (headerLine.isFixedCount, headerLine.getCountType) match {
        case (false, VCFHeaderLineCount.A) => {
          Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
            {
              fromArrayExtractor(vc, id, toFn, idx)
                .map(kv => (kv._1, kv._2.toString))
            })
        }
        case (false, VCFHeaderLineCount.R) => {
          Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
            {
              arrayFieldExtractor(vc, id, toFn, List(0, idx + 1))
                .map(kv => (kv._1, kv._2.mkString(",")))
            })
        }
        case (false, VCFHeaderLineCount.G) => {
          if (stringency == ValidationStringency.LENIENT) {
            log.warn("Ignoring INFO field with Number=G described in header row: %s".format(headerLine))
            None
          } else
            throw new IllegalArgumentException("Number=G INFO lines are not supported in split-allelic model: %s".format(
              headerLine))
        }
        case _ => {
          Some((vc: HtsjdkVariantContext, idx: Int, indices: Array[Int]) =>
            {
              arrayFieldExtractor(vc, id, toFn, List.empty)
                .map(kv => (kv._1, kv._2.mkString(",")))
            })
        }
      }
    }
  }

  private def lineToGenotypeExtractor(
    headerLine: VCFFormatHeaderLine): ((HtsjdkGenotype, Int, Array[Int]) => Option[(String, String)]) = {
    val id = headerLine.getID

    if (headerLine.isFixedCount && headerLine.getCount == 1) {
      headerLine.getType match {
        case VCFHeaderLineType.Flag => {
          throw new IllegalArgumentException("Flag is not supported for Format lines: %s".format(
            headerLine))
        }
        case VCFHeaderLineType.Character => {
          (g: HtsjdkGenotype, idx: Int, indices: Array[Int]) =>
            {
              fieldExtractor(g, id, toChar).map(kv => (kv._1, kv._2.toString))
            }
        }
        case VCFHeaderLineType.Float => {
          (g: HtsjdkGenotype, idx: Int, indices: Array[Int]) =>
            {
              fieldExtractor(g, id, toFloat).map(kv => (kv._1, kv._2.toString))
            }
        }
        case VCFHeaderLineType.Integer => {
          (g: HtsjdkGenotype, idx: Int, indices: Array[Int]) =>
            {
              fieldExtractor(g, id, toInt).map(kv => (kv._1, kv._2.toString))
            }
        }
        case VCFHeaderLineType.String => {
          (g: HtsjdkGenotype, idx: Int, indices: Array[Int]) =>
            {
              fieldExtractor(g, id, asString).map(kv => (kv._1, kv._2.toString))
            }
        }
      }
    } else {
      val toFn: (java.lang.Object => Array[String]) = headerLine.getType match {
        case VCFHeaderLineType.Flag => {
          throw new IllegalArgumentException("Flag is not supported for Format lines: %s".format(
            headerLine))
        }
        case VCFHeaderLineType.Character => {
          toCharArray(_).map(c => c.toString)
        }
        case VCFHeaderLineType.Float => {
          toFloatArray(_).map(f => f.toString)
        }
        case VCFHeaderLineType.Integer => {
          toIntArray(_).map(i => i.toString)
        }
        case VCFHeaderLineType.String => {
          toStringArray(_)
        }
      }

      (headerLine.isFixedCount, headerLine.getCountType) match {
        case (false, VCFHeaderLineCount.A) => {
          (g: HtsjdkGenotype, idx: Int, indices: Array[Int]) =>
            {
              fromArrayExtractor(g, id, toFn, idx - 1)
                .map(kv => (kv._1, kv._2.toString))
            }
        }
        case (false, VCFHeaderLineCount.R) => {
          (g: HtsjdkGenotype, idx: Int, indices: Array[Int]) =>
            {
              arrayFieldExtractor(g, id, toFn, List(0, idx))
                .map(kv => (kv._1, kv._2.mkString(",")))
            }
        }
        case (false, VCFHeaderLineCount.G) => {
          (g: HtsjdkGenotype, idx: Int, indices: Array[Int]) =>
            {
              arrayFieldExtractor(g, id, toFn, indices.toList)
                .map(kv => (kv._1, kv._2.mkString(",")))
            }
        }
        case _ => {
          (g: HtsjdkGenotype, idx: Int, indices: Array[Int]) =>
            {
              arrayFieldExtractor(g, id, toFn, List.empty)
                .map(kv => (kv._1, kv._2.mkString(",")))
            }
        }
      }
    }
  }

  private def makeVariantFormatFn(
    headerLines: Seq[VCFHeaderLine],
    stringency: ValidationStringency = ValidationStringency.STRICT): (HtsjdkVariantContext, Option[String], Int, Boolean) => (Variant, Variant) = {

    val attributeFns: Iterable[(HtsjdkVariantContext, Int, Array[Int]) => Option[(String, String)]] = headerLines
      .flatMap(hl => hl match {
        case il: VCFInfoHeaderLine => {
          // get the id of this line
          val key = il.getID

          // filter out the lines that we already support
          if (DefaultHeaderLines.infoHeaderLines
            .find(_.getID == key)
            .isEmpty) {
            try {
              lineToVariantContextExtractor(il)
            } catch {
              case t: Throwable => {
                if (stringency == ValidationStringency.STRICT) {
                  throw t
                } else {
                  if (stringency == ValidationStringency.LENIENT) {
                    log.warn("Saw invalid info field %s. Ignoring...".format(t))
                  }
                  None
                }
              }
            }
          } else {
            None
          }
        }
        case _ => None
      })

    def convert(vc: HtsjdkVariantContext,
                alt: Option[String],
                alleleIdx: Int,
                wasSplit: Boolean): (Variant, Variant) = {

      // create the builder
      val variantBuilder = Variant.newBuilder
        .setReferenceName(vc.getChr)
        .setStart(vc.getStart - 1)
        .setEnd(vc.getEnd)
        .setReferenceAllele(vc.getReference.getBaseString)

      // was this split?
      if (wasSplit) {
        variantBuilder.setSplitFromMultiAllelic(true)
      }

      alt.foreach(variantBuilder.setAlternateAllele(_))

      // bind the conversion functions and fold
      val boundFns: Iterable[Variant.Builder => Variant.Builder] = variantFormatFns
        .map(fn => {
          fn(vc, _: Variant.Builder)
        })
      val converted = boundFns.foldLeft(variantBuilder)((vb: Variant.Builder, fn) => {
        try {
          fn(vb)
        } catch {
          case t: Throwable => {
            if (stringency == ValidationStringency.STRICT) {
              throw t
            } else {
              if (stringency == ValidationStringency.LENIENT) {
                log.warn("Converting variant field from %s with function %s line %s failed: %s".format(vc, fn, t))
              }
              vb
            }
          }
        }
      })

      val variant = variantBuilder.build
      val variantAnnotationBuilder = VariantAnnotation.newBuilder

      val boundAnnotationFns: Iterable[VariantAnnotation.Builder => VariantAnnotation.Builder] = variantAnnotationFormatFns
        .map(fn => {
          fn(vc, _: VariantAnnotation.Builder, variant, alleleIdx)
        })
      val convertedAnnotation = boundAnnotationFns.foldLeft(variantAnnotationBuilder)(
        (vab: VariantAnnotation.Builder, fn) => {
          try {
            fn(vab)
          } catch {
            case t: Throwable => {
              if (stringency == ValidationStringency.STRICT) {
                throw t
              } else {
                if (stringency == ValidationStringency.LENIENT) {
                  log.warn("Generating annotation tag from line %s with function %sfailed: %s".format(vab, fn, t))
                }
                vab
              }
            }
          }
        })

      val indices = Array.empty[Int]

      // pull out the attribute map and process
      val attrMap = attributeFns.flatMap(fn => fn(vc, alleleIdx, indices))
        .toMap

      // if the map has kv pairs, attach it
      val convertedAnnotationWithAttrs = if (attrMap.isEmpty) {
        convertedAnnotation
      } else {
        convertedAnnotation.setAttributes(attrMap)
      }

      variantBuilder.setAnnotation(convertedAnnotationWithAttrs.build)
      (variant, variantBuilder.build)
    }

    convert(_, _, _, _)
  }

  private def makeGenotypeFormatFn(
    headerLines: Seq[VCFHeaderLine]): (HtsjdkGenotype, Variant, Allele, Int, Option[Int], Boolean) => Genotype = {

    val attributeFns: Iterable[(HtsjdkGenotype, Int, Array[Int]) => Option[(String, String)]] = headerLines
      .flatMap(hl => hl match {
        case fl: VCFFormatHeaderLine => {

          // get the id of this line
          val key = fl.getID

          // filter out the lines that we already support
          if (DefaultHeaderLines.formatHeaderLines
            .find(_.getID == key)
            .isEmpty) {

            Some(lineToGenotypeExtractor(fl))
          } else {
            None
          }
        }
        case _ => None
      })

    def convert(g: HtsjdkGenotype,
                variant: Variant,
                allele: Allele,
                alleleIdx: Int,
                nonRefIndex: Option[Int],
                wasSplit: Boolean): Genotype = {

      // create the builder
      val builder = Genotype.newBuilder()
        .setVariant(variant)
        .setReferenceName(variant.getReferenceName)
        .setStart(variant.getStart)
        .setEnd(variant.getEnd)
        .setSampleId(g.getSampleName)
        .setAlleles(g.getAlleles.map(gtAllele => {
          if (gtAllele.isReference) {
            GenotypeAllele.REF
          } else if (gtAllele.isNoCall) {
            GenotypeAllele.NO_CALL
          } else if (gtAllele.equals(allele, true)) {
            GenotypeAllele.ALT
          } else {
            GenotypeAllele.OTHER_ALT
          }
        }))

      // was this split?
      if (wasSplit) {
        builder.setSplitFromMultiAllelic(true)
      }

      // get array indices
      val indices = if (alleleIdx > 0) {
        GenotypeLikelihoods.getPLIndecesOfAlleles(0, alleleIdx)
      } else {
        Array.empty[Int]
      }

      // bind the conversion functions and fold
      val boundFns: Iterable[Genotype.Builder => Genotype.Builder] = genotypeFormatFns
        .map(fn => {
          fn(g, _: Genotype.Builder, alleleIdx, indices)
        })
      val convertedCore = boundFns.foldLeft(builder)((gb: Genotype.Builder, fn) => {
        try {
          fn(gb)
        } catch {
          case t: Throwable => {
            if (stringency == ValidationStringency.STRICT) {
              throw t
            } else {
              if (stringency == ValidationStringency.LENIENT) {
                log.warn("Converting genotype field in %s with function %s failed: %s".format(
                  g, fn, t))
              }
              gb
            }
          }
        }
      })

      // if we have a non-ref allele, fold and build
      val coreWithOptNonRefs = nonRefIndex.fold(convertedCore)(nonRefAllele => {

        // non-ref pl indices
        val nrIndices = GenotypeLikelihoods.getPLIndecesOfAlleles(0, nonRefAllele)

        formatNonRefGenotypeLikelihoods(g, convertedCore, nrIndices)
      })

      val vcAnns = VariantCallingAnnotations.newBuilder

      // bind the annotation conversion functions and fold
      val boundAnnotationFns: Iterable[VariantCallingAnnotations.Builder => VariantCallingAnnotations.Builder] = genotypeAnnotationFormatFns
        .map(fn => {
          fn(g, _: VariantCallingAnnotations.Builder, alleleIdx, indices)
        })
      val convertedAnnotations = boundAnnotationFns.foldLeft(vcAnns)(
        (vcab: VariantCallingAnnotations.Builder, fn) => {
          try {
            fn(vcab)
          } catch {
            case t: Throwable => {
              if (stringency == ValidationStringency.STRICT) {
                throw t
              } else {
                if (stringency == ValidationStringency.LENIENT) {
                  log.warn("Converting genotype annotation field in %s with function %s failed: %s".format(
                    g, fn, t))
                }
                vcab
              }
            }
          }
        })

      // pull out the attribute map and process
      val attrMap = attributeFns.flatMap(fn => fn(g, alleleIdx, indices))
        .toMap

      // if the map has kv pairs, attach it
      val convertedAnnotationsWithAttrs = if (attrMap.isEmpty) {
        convertedAnnotations
      } else {
        convertedAnnotations.setAttributes(attrMap)
      }

      // build the annotations and attach
      val gtWithAnnotations = coreWithOptNonRefs
        .setVariantCallingAnnotations(convertedAnnotationsWithAttrs.build)

      // build and return
      gtWithAnnotations.build()
    }

    convert(_, _, _, _, _, _)
  }

  private def extractorFromInfoLine(
    headerLine: VCFInfoHeaderLine): (Map[String, String]) => Option[(String, java.lang.Object)] = {

    val id = headerLine.getID

    def toCharAndKey(s: String): (String, java.lang.Object) = {

      require(s.length == 1,
        "Expected character field: %s.".format(id))
      val javaChar: java.lang.Character = s(0)

      (id, javaChar.asInstanceOf[java.lang.Object])
    }

    def toFloatAndKey(s: String): (String, java.lang.Object) = {
      val javaFloat: java.lang.Float = s.toFloat

      (id, javaFloat.asInstanceOf[java.lang.Object])
    }

    def toBooleanAndKey(s: String): (String, java.lang.Object) = {
      val javaBoolean: java.lang.Boolean = s.toBoolean

      (id, javaBoolean.asInstanceOf[java.lang.Object])
    }

    def toIntAndKey(s: String): (String, java.lang.Object) = {
      val javaInteger: java.lang.Integer = s.toInt

      (id, javaInteger.asInstanceOf[java.lang.Object])
    }

    if (headerLine.isFixedCount && headerLine.getCount == 0 && headerLine.getType == VCFHeaderLineType.Flag) {
      (m: Map[String, String]) =>
        {
          m.get(id).map(toBooleanAndKey)
        }
    } else if (headerLine.isFixedCount && headerLine.getCount == 1) {
      headerLine.getType match {
        // Flag header line types should be Number=0, but we'll allow Number=1
        case VCFHeaderLineType.Flag => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(toBooleanAndKey)
            }
        }
        case VCFHeaderLineType.Character => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(toCharAndKey)
            }
        }
        case VCFHeaderLineType.Float => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(toFloatAndKey)
            }
        }
        case VCFHeaderLineType.Integer => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(toIntAndKey)
            }
        }
        case VCFHeaderLineType.String => {
          (m: Map[String, String]) =>
            {
              // don't need to force to the java type, as String in scala is
              // an alias for java.lang.String
              m.get(id).map(v => (id, v.asInstanceOf[java.lang.Object]))
            }
        }
      }
    } else {

      headerLine.getType match {
        case VCFHeaderLineType.Flag => {
          throw new IllegalArgumentException("Multivalue flags are not supported for INFO lines: %s".format(
            headerLine))
        }
        case VCFHeaderLineType.Character => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(v => {
                (id, v.split(",")
                  .map(c => {
                    require(c.length == 1,
                      "Expected character field: %s in %s.".format(id,
                        m))
                    c(0): java.lang.Character
                  }).asInstanceOf[java.lang.Object])
              })
            }
        }
        case VCFHeaderLineType.Float => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(v => {
                (id, v.split(",")
                  .map(f => {
                    f.toFloat: java.lang.Float
                  }).asInstanceOf[java.lang.Object])
              })
            }
        }
        case VCFHeaderLineType.Integer => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(v => {
                (id, v.split(",")
                  .map(i => {
                    i.toInt: java.lang.Integer
                  }).asInstanceOf[java.lang.Object])
              })
            }
        }
        case VCFHeaderLineType.String => {
          (m: Map[String, String]) =>
            {
              // don't need to force to the java type, as String in scala is
              // an alias for java.lang.String
              m.get(id).map(v => (id, v.split(",").asInstanceOf[java.lang.Object]))
            }
        }
      }
    }
  }

  private def extractorFromFormatLine(
    headerLine: VCFFormatHeaderLine): (Map[String, String]) => Option[(String, java.lang.Object)] = {

    val id = headerLine.getID

    def toCharAndKey(s: String): (String, java.lang.Object) = {

      require(s.length == 1,
        "Expected character field: %s.".format(id))
      val javaChar: java.lang.Character = s(0)

      (id, javaChar.asInstanceOf[java.lang.Object])
    }

    def toFloatAndKey(s: String): (String, java.lang.Object) = {
      val javaFloat: java.lang.Float = s.toFloat

      (id, javaFloat.asInstanceOf[java.lang.Object])
    }

    def toIntAndKey(s: String): (String, java.lang.Object) = {
      val javaInteger: java.lang.Integer = s.toInt

      (id, javaInteger.asInstanceOf[java.lang.Object])
    }

    if (headerLine.isFixedCount && headerLine.getCount == 1) {
      headerLine.getType match {
        case VCFHeaderLineType.Flag => {
          throw new IllegalArgumentException("Flag is not supported for Format lines: %s".format(
            headerLine))
        }
        case VCFHeaderLineType.Character => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(toCharAndKey)
            }
        }
        case VCFHeaderLineType.Float => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(toFloatAndKey)
            }
        }
        case VCFHeaderLineType.Integer => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(toIntAndKey)
            }
        }
        case VCFHeaderLineType.String => {
          (m: Map[String, String]) =>
            {
              // don't need to force to the java type, as String in scala is
              // an alias for java.lang.String
              m.get(id).map(v => (id, v.asInstanceOf[java.lang.Object]))
            }
        }
      }
    } else {

      headerLine.getType match {
        case VCFHeaderLineType.Flag => {
          throw new IllegalArgumentException("Flag is not supported for Format lines: %s".format(
            headerLine))
        }
        case VCFHeaderLineType.Character => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(v => {
                (id, v.split(",")
                  .map(c => {
                    require(c.length == 1,
                      "Expected character field: %s in %s.".format(id,
                        m))
                    c(0): java.lang.Character
                  }).asInstanceOf[java.lang.Object])
              })
            }
        }
        case VCFHeaderLineType.Float => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(v => {
                (id, v.split(",")
                  .map(f => {
                    f.toFloat: java.lang.Float
                  }).asInstanceOf[java.lang.Object])
              })
            }
        }
        case VCFHeaderLineType.Integer => {
          (m: Map[String, String]) =>
            {
              m.get(id).map(v => {
                (id, v.split(",")
                  .map(i => {
                    i.toInt: java.lang.Integer
                  }).asInstanceOf[java.lang.Object])
              })
            }
        }
        case VCFHeaderLineType.String => {
          (m: Map[String, String]) =>
            {
              // don't need to force to the java type, as String in scala is
              // an alias for java.lang.String
              m.get(id).map(v => (id, v.split(",").asInstanceOf[java.lang.Object]))
            }
        }
      }
    }
  }

  private def makeVariantExtractFn(
    headerLines: Seq[VCFHeaderLine]): (ADAMVariantContext) => HtsjdkVariantContext = {

    val attributeFns: Iterable[(Map[String, String]) => Option[(String, java.lang.Object)]] = headerLines
      .flatMap(hl => hl match {
        case il: VCFInfoHeaderLine => {

          // get the id of this line
          val key = il.getID

          // filter out the lines that we already support
          if (DefaultHeaderLines.infoHeaderLines
            .find(_.getID == key)
            .isDefined) {

            None
          } else {
            try {
              Some(extractorFromInfoLine(il))
            } catch {
              case t: Throwable => {
                if (stringency == ValidationStringency.STRICT) {
                  throw t
                } else {
                  if (stringency == ValidationStringency.LENIENT) {
                    log.warn("Generating field extractor from header line %s failed: %s".format(il, t))
                  }
                  None
                }
              }
            }
          }
        }
        case _ => None
      })

    def convert(vc: ADAMVariantContext): HtsjdkVariantContext = {
      val v = vc.variant.variant
      val hasNonRefAlleles = vc.genotypes
        .exists(_.getNonReferenceLikelihoods.length != 0)
      val builder = new VariantContextBuilder()
        .chr(v.getReferenceName)
        .start(v.getStart + 1)
        .stop(v.getEnd)
        .alleles(VariantContextConverter.convertAlleles(v, hasNonRefAlleles))

      // bind the conversion functions and fold
      val convertedWithVariants = variantExtractFns.foldLeft(builder)(
        (vcb: VariantContextBuilder, fn) => {
          try {
            fn(v, vcb)
          } catch {
            case t: Throwable => {
              if (stringency == ValidationStringency.STRICT) {
                throw t
              } else {
                if (stringency == ValidationStringency.LENIENT) {
                  log.warn("Applying extraction function %s to %s failed with %s.".format(fn, v, t))
                }
                vcb
              }
            }
          }
        })

      // extract from annotations, if present
      val convertedWithAttrs = Option(v.getAnnotation)
        .fold(convertedWithVariants)(va => {
          val convertedWithAnnotations = variantAnnotationExtractFns
            .foldLeft(convertedWithVariants)((vcb: VariantContextBuilder, fn) => fn(va, vcb))

          // get the attribute map
          val attributes: Map[String, String] = va.getAttributes.toMap

          // apply the attribute converters and return
          attributeFns.foldLeft(convertedWithAnnotations)((vcb: VariantContextBuilder, fn) => {
            val optAttrPair = fn(attributes)
            optAttrPair.fold(vcb)(pair => {
              try {
                vcb.attribute(pair._1, pair._2)
              } catch {
                case t: Throwable => {
                  if (stringency == ValidationStringency.STRICT) {
                    throw t
                  } else {
                    if (stringency == ValidationStringency.LENIENT) {
                      log.warn("Applying annotation extraction function %s to %s failed with %s.".format(fn, v, t))
                    }
                    vcb
                  }
                }
              }
            })
          })
        })

      // build and return
      convertedWithAttrs.make()
    }

    convert(_)
  }

  private def makeGenotypeExtractFn(
    headerLines: Seq[VCFHeaderLine]): (Genotype) => HtsjdkGenotype = {

    val attributeFns: Iterable[(Map[String, String]) => Option[(String, java.lang.Object)]] = headerLines
      .flatMap(hl => hl match {
        case fl: VCFFormatHeaderLine => {

          // get the id of this line
          val key = fl.getID

          // filter out the lines that we already support
          if (DefaultHeaderLines.formatHeaderLines
            .find(_.getID == key)
            .isDefined) {

            None
          } else {
            try {
              Some(extractorFromFormatLine(fl))
            } catch {
              case t: Throwable => {
                if (stringency == ValidationStringency.STRICT) {
                  throw t
                } else {
                  if (stringency == ValidationStringency.LENIENT) {
                    log.warn("Generating field extractor from header line %s failed: %s".format(fl, t))
                  }
                  None
                }
              }
            }
          }
        }
        case _ => None
      })

    def convert(g: Genotype): HtsjdkGenotype = {

      // create the builder
      val builder = new GenotypeBuilder(g.getSampleId,
        VariantContextConverter.convertAlleles(g))

      // bind the conversion functions and fold
      val convertedCore = genotypeExtractFns.foldLeft(builder)(
        (gb: GenotypeBuilder, fn) => {
          try {
            fn(g, gb)
          } catch {
            case t: Throwable => {
              if (stringency == ValidationStringency.STRICT) {
                throw t
              } else {
                if (stringency == ValidationStringency.LENIENT) {
                  log.warn("Applying annotation extraction function %s to %s failed with %s.".format(fn, g, t))
                }

                gb
              }
            }
          }
        })

      // convert the annotations if they exist
      val gtWithAnnotations = Option(g.getVariantCallingAnnotations)
        .fold(convertedCore)(vca => {

          // bind the annotation conversion functions and fold
          val convertedAnnotations = genotypeAnnotationExtractFns.foldLeft(convertedCore)(
            (gb: GenotypeBuilder, fn) => {
              try {
                fn(vca, gb)
              } catch {
                case t: Throwable => {
                  if (stringency == ValidationStringency.STRICT) {
                    throw t
                  } else {
                    if (stringency == ValidationStringency.LENIENT) {
                      log.warn("Applying annotation extraction function %s to %s failed with %s.".format(fn, vca, t))
                    }

                    gb
                  }
                }
              }
            })

          // get the attribute map
          val attributes: Map[String, String] = vca.getAttributes.toMap

          // apply the attribute converters and return
          attributeFns.foldLeft(convertedAnnotations)((gb: GenotypeBuilder, fn) => {
            try {
              val optAttrPair = fn(attributes)

              optAttrPair.fold(gb)(pair => {
                gb.attribute(pair._1, pair._2)
              })
            } catch {
              case t: Throwable => {
                if (stringency == ValidationStringency.STRICT) {
                  throw t
                } else {
                  if (stringency == ValidationStringency.LENIENT) {
                    log.warn("Applying attribute extraction function %s to %s failed with %s.".format(fn, vca, t))
                  }

                  gb
                }
              }
            }
          })
        })

      // build and return
      gtWithAnnotations.make()
    }

    convert(_)
  }

  /**
   * Convert an ADAM variant context into a GATK variant context.
   *
   * @param vc ADAM variant context to convert.
   * @return The specified ADAM variant context converted into a GATK variant context.
   */
  def convert(
    vc: ADAMVariantContext): Option[HtsjdkVariantContext] = {

    // attach genotypes
    try {
      val vcb = new VariantContextBuilder(variantExtractFn(vc))

      Some(vcb.genotypes(vc.genotypes.map(g => genotypeExtractFn(g)))
        .make)
    } catch {
      case t: Throwable => {
        if (stringency == ValidationStringency.STRICT) {
          throw t
        } else {
          if (stringency == ValidationStringency.LENIENT) {
            log.error(
              "Encountered error %s when converting variant context with variant:\n%s\nand genotypes: \n%s".format(t, vc.variant.variant, vc.genotypes.mkString("\n")))
          }
          None
        }
      }
    }
  }
}
