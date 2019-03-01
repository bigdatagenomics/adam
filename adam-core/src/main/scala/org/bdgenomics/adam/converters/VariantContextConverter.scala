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
  Allele => HtsjdkAllele,
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
 */
object VariantContextConverter {

  /**
   * Representation for an unknown non-ref/symbolic allele in VCF.
   */
  private val NON_REF_ALLELE = HtsjdkAllele.create("<NON_REF>", false /* !Reference */ )

  /**
   * The index in the Avro genotype record for the splitFromMultiallelec field.
   *
   * This field is true if the VCF site was not biallelic.
   */
  private lazy val splitFromMultiallelicField = Genotype.SCHEMA$.getField("splitFromMultiallelic")

  /**
   * Convert a htsjdk allele to an Avro allele.
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
  private def convertAllele(vc: HtsjdkVariantContext, allele: HtsjdkAllele): Allele = {
    if (allele.isNoCall) Allele.NO_CALL
    else if (allele.isReference) Allele.REF
    else if (allele == NON_REF_ALLELE || !vc.hasAlternateAllele(allele)) Allele.OTHER_ALT
    else Allele.ALT
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
  private def convertAlleleOpt(allele: String, isRef: Boolean = false): Option[HtsjdkAllele] = {
    if (allele == null) {
      None
    } else {
      Some(HtsjdkAllele.create(allele, isRef))
    }
  }

  private val OPT_NON_REF = Some(HtsjdkAllele.create("<NON_REF>", false))

  private def optNonRef(v: Variant, hasNonRefModel: Boolean): Option[HtsjdkAllele] = {
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
  private def convertAlleles(v: Variant, hasNonRefModel: Boolean): java.util.Collection[HtsjdkAllele] = {
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
  private def convertAlleles(g: Genotype): java.util.List[HtsjdkAllele] = {
    var alleles = g.getAlleles
    if (alleles == null) return Collections.emptyList[HtsjdkAllele]
    else g.getAlleles.map {
      case Allele.NO_CALL | Allele.OTHER_ALT => HtsjdkAllele.NO_CALL
      case Allele.REF                        => HtsjdkAllele.create(g.getReferenceAllele, true)
      case Allele.ALT                        => HtsjdkAllele.create(g.getAlternateAllele)
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
      stringency)
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
    stringency: ValidationStringency) extends Serializable with Logging {
  import VariantContextConverter._

  // format fns gatk --> bdg, extract fns bdg --> gatk
  private val variantFormatFn = makeVariantFormatFn(headerLines, stringency)
  private val variantExtractFn = makeVariantExtractFn(headerLines)
  private val genotypeFormatFn = makeGenotypeFormatFn(headerLines)
  private val genotypeExtractFn = makeGenotypeExtractFn(headerLines)
  private val gvcfBlocks = extractGvcfBlocks(headerLines)

  private val gvcfBlockRegex = """minGQ=([0-9+]).*maxGQ=([0-9]+).*""".r

  private def extractGvcfBlocks(headerLines: Seq[VCFHeaderLine]): Seq[(Int, Int)] = {
    headerLines
      .find(_.getKey() == "##GVCFBlock")
      .map(_.getValue() match {
        case gvcfBlockRegex(minGQ, maxGQ) => (minGQ.toInt, maxGQ.toInt)
      })
      .toSeq
  }

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
          val variant = variantFormatFn(vc, None, 0, false)
          val genotypes = vc.getGenotypes.map(g => {
            genotypeFormatFn(g, variant, NON_REF_ALLELE, 0, Some(1), false)
          })
          return Seq(ADAMVariantContext(variant, genotypes))
        }
        case List(allele) => {
          require(
            allele.isNonReference,
            "Assertion failed when converting: " + vc.toString
          )
          val variant = variantFormatFn(vc, Some(allele.getDisplayString), 0, false)
          val genotypes = vc.getGenotypes.map(g => {
            genotypeFormatFn(g, variant, allele, 1, None, false)
          })
          return Seq(ADAMVariantContext(variant, genotypes))
        }
        case List(allele, NON_REF_ALLELE) => {
          require(
            allele.isNonReference,
            "Assertion failed when converting: " + vc.toString
          )
          val variant = variantFormatFn(vc, Some(allele.getDisplayString), 0, false)
          val genotypes = vc.getGenotypes.map(g => {
            genotypeFormatFn(g, variant, allele, 1, Some(2), false)
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
            val variant = variantFormatFn(vc,
              Some(allele.getDisplayString),
              variantIdx,
              true)
            val genotypes = vc.getGenotypes.map(g => {
              genotypeFormatFn(g, variant, allele, idx, referenceModelIndex, true)
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

  private[converters] def formatFilters(g: HtsjdkGenotype,
                                        gb: Genotype.Builder,
                                        idx: Int,
                                        indices: Array[Int]): Genotype.Builder = {
    // see https://github.com/samtools/htsjdk/issues/741
    val gtFiltersWereApplied = true
    if (gtFiltersWereApplied) {
      val filtersWereApplied = gb.setFiltersApplied(true)
      if (g.isFiltered) {
        filtersWereApplied.setFiltersPassed(false)
          .setFiltersFailed(g.getFilters.split(";").toList)
      } else {
        filtersWereApplied.setFiltersPassed(true)
      }
    } else {
      gb.setFiltersApplied(false)
    }
  }

  private[converters] def formatPhaseSet(g: HtsjdkGenotype,
                                         gb: Genotype.Builder,
                                         gIdx: Int,
                                         gIndices: Array[Int]): Genotype.Builder = {
    if (g.isPhased) {
      gb.setPhased(true)

      Option(g.getExtendedAttribute("PS"))
        .map(attr => {
          tryAndCatchStringCast(attr, attribute => {
            gb.setPhaseSet(attribute.asInstanceOf[java.lang.Integer])
          }, attribute => {
            gb.setPhaseSet(attribute.toInt)
          })
        })
    }
    gb
  }

  private val genotypeFormatFns: Iterable[(HtsjdkGenotype, Genotype.Builder, Int, Array[Int]) => Genotype.Builder] = Iterable(
    formatFilters(_, _, _, _),
    formatPhaseSet(_, _, _, _)
  )

  // genotype --> htsjdk extract functions

  private[converters] def extractFilters(g: Genotype,
                                         gb: GenotypeBuilder): GenotypeBuilder = {
    Option(g.getFiltersApplied)
      .filter(ft => ft)
      .map(applied => {
        Option(g.getFiltersPassed).map(passed => {
          if (passed) {
            gb.filters("PASS")
          } else {
            val failedFilters = g.getFiltersFailed
            require(failedFilters.nonEmpty,
              "Genotype marked as filtered, but no failed filters listed in %s.".format(g))
            gb.filters(failedFilters.mkString(";"))
          }
        }).getOrElse({
          throw new IllegalArgumentException("Genotype filters were applied but filters passed is null in %s.".format(g))
        })
      }).getOrElse(gb.unfiltered())
  }

  private[converters] def extractPhaseSet(g: Genotype,
                                          gb: GenotypeBuilder): GenotypeBuilder = {
    Option(g.getPhased)
      .filter(p => p)
      .map(p => {
        Option(g.getPhaseSet).map(gb.attribute("PS", _))
        gb.phased(true)
      }).getOrElse(gb.phased(false))
  }

  private val genotypeExtractFns: Iterable[(Genotype, GenotypeBuilder) => GenotypeBuilder] = Iterable(
    extractFilters(_, _),
    extractPhaseSet(_, _)
  )

  // htsjdk --> genotype annotation format functions

  private[converters] def formatReadDepths(g: HtsjdkGenotype,
                                           gab: GenotypeAnnotation.Builder,
                                           gIdx: Int,
                                           gIndices: Array[Int]): GenotypeAnnotation.Builder = {
    // AD is an array type field
    if (g.hasAD && gIdx < g.getAD.size) {
      val ad = g.getAD
      gab.setReferenceReadDepth(ad(0))
        .setAlternateReadDepth(ad(gIdx))
    } else {
      gab
    }
  }

  private[converters] def formatForwardReadDepths(g: HtsjdkGenotype,
                                                  gab: GenotypeAnnotation.Builder,
                                                  gIdx: Int,
                                                  gIndices: Array[Int]): GenotypeAnnotation.Builder = {
    Option(g.getExtendedAttribute("ADF"))
      .map(attr => {
        val adf = toIntArray(attr)
        try {
          gab.setReferenceForwardReadDepth(adf(0))
            .setAlternateForwardReadDepth(adf(gIdx))
        } catch {
          case _: ArrayIndexOutOfBoundsException => {
            log.warn("Ran into Array Out of Bounds when accessing index %s for field ADF of genotype %s.".format(gIdx, g))
            gab
          }
        }
      }).getOrElse(gab)
  }

  private[converters] def formatReverseReadDepths(g: HtsjdkGenotype,
                                                  gab: GenotypeAnnotation.Builder,
                                                  gIdx: Int,
                                                  gIndices: Array[Int]): GenotypeAnnotation.Builder = {
    Option(g.getExtendedAttribute("ADR"))
      .map(attr => {
        val adr = toIntArray(attr)
        try {
          gab.setReferenceReverseReadDepth(adr(0))
            .setAlternateReverseReadDepth(adr(gIdx))
        } catch {
          case _: ArrayIndexOutOfBoundsException => {
            log.warn("Ran into Array Out of Bounds when accessing index %s for field ADR of genotype %s.".format(gIdx, g))
            gab
          }
        }
      }).getOrElse(gab)
  }

  private[converters] def formatReadDepth(g: HtsjdkGenotype,
                                          gab: GenotypeAnnotation.Builder,
                                          idx: Int,
                                          indices: Array[Int]): GenotypeAnnotation.Builder = {
    if (g.hasDP) {
      gab.setReadDepth(g.getDP)
    } else {
      gab
    }
  }

  private[converters] def formatLikelihoods(g: HtsjdkGenotype,
                                            gab: GenotypeAnnotation.Builder,
                                            idx: Int,
                                            indices: Array[Int]): GenotypeAnnotation.Builder = {
    // GL Number=G Type=Float
    // Convert GL from log10 scale to log scale, optionally convert PL from phred scale to log scale

    if (g.hasPL) {
      val pl = g.getPL
      try {
        val likelihoods = indices.map(idx => {
          jDouble(PhredUtils.phredToLogProbability(pl(idx)))
        }).toList
        gab.setLikelihoods(likelihoods)
      } catch {
        case _: ArrayIndexOutOfBoundsException => {
          log.warn("Ran into Array Out of Bounds when accessing indices %s for field PL of genotype %s.".format(indices.mkString(","), g))
          gab
        }
      }
    } else {
      gab
    }
  }

  private[converters] def formatNonReferenceLikelihoods(g: HtsjdkGenotype,
                                                        gab: GenotypeAnnotation.Builder,
                                                        idx: Int,
                                                        indices: Array[Int]): GenotypeAnnotation.Builder = {

    /*

     This looks correct, with the exception that nonReferenceLikelihoods shouldn't go into
     a new VCF key, but that nonReferenceLikelihoods encodes the GL's for the symbolic non-ref
     (*) alt in a gVCF line.

     Handling this gets a bit complex, but the way to think of it is that if alternateAllele is
     non-null, we get a VCF line with two alts (alternateAllele, and *). In the diploid case, we'd
     get GL's:

       likelihoods(0), likelihoods(1), likelihoods(2), nonRefLikelihoods(1), nonRefLikelihoods(2), 0?.

     I'm not entirely sure what should go in the last spot, which would be the GT=1/2 likelihood,
     which I feel is not well defined. If alternateAllele is null, then we get a VCF line with just *
     as an alt and with GL=nonRefLikelihoods.

     */
    if (g.hasPL) {
      val pl = g.getPL
      gab.setNonReferenceLikelihoods(indices.map(idx => {
        jDouble(PhredUtils.phredToLogProbability(pl(idx)))
      }).toList)
    } else {
      gab
    }
  }

  private[converters] def formatPriors(g: HtsjdkGenotype,
                                       gab: GenotypeAnnotation.Builder,
                                       idx: Int,
                                       indices: Array[Int]): GenotypeAnnotation.Builder = {

    Option(g.getExtendedAttribute("priors"))
      .map(attr => {
        val p = toFloatArray(attr)
        try {
          val priors = indices.map(idx => {
            jFloat(p(idx))
          }).toList
          gab.setPriors(priors)
        } catch {
          case _: ArrayIndexOutOfBoundsException => {
            log.warn("Ran into Array Out of Bounds when accessing indices %s for field priors of genotype %s.".format(indices.mkString(","), g))
            gab
          }
        }
      }).getOrElse(gab)
  }

  private[converters] def formatPosteriors(g: HtsjdkGenotype,
                                           gab: GenotypeAnnotation.Builder,
                                           idx: Int,
                                           indices: Array[Int]): GenotypeAnnotation.Builder = {

    Option(g.getExtendedAttribute("GP"))
      .map(attr => {
        val gp = toFloatArray(attr)
        try {
          val posteriors = indices.map(idx => {
            // todo: convert GP from log10 to log scale
            jFloat(gp(idx))
          }).toList
          gab.setPosteriors(posteriors)
        } catch {
          case _: ArrayIndexOutOfBoundsException => {
            log.warn("Ran into Array Out of Bounds when accessing indices %s for field GP of genotype %s.".format(indices.mkString(","), g))
            gab
          }
        }
      }).getOrElse(gab)
  }

  private[converters] def formatQuality(g: HtsjdkGenotype,
                                        gab: GenotypeAnnotation.Builder,
                                        idx: Int,
                                        indices: Array[Int]): GenotypeAnnotation.Builder = {
    if (g.hasGQ) {
      val gq = g.getGQ
      gab.setQuality(gq)

      gvcfBlocks.foreach(kv => {
        val minGQ = kv._1
        val maxGQ = kv._2
        if (gq >= minGQ && gq < maxGQ) {
          gab.setMinimumQuality(minGQ)
          gab.setMaximumQuality(maxGQ)
        }
      })
    }
    gab
  }

  private[converters] def formatRmsMappingQuality(g: HtsjdkGenotype,
                                                  gab: GenotypeAnnotation.Builder,
                                                  idx: Int,
                                                  indices: Array[Int]): GenotypeAnnotation.Builder = {

    Option(g.getExtendedAttribute("MQ"))
      .map(attr => {
        tryAndCatchStringCast(attr, attribute => {
          gab.setRmsMappingQuality(attribute.asInstanceOf[java.lang.Integer])
        }, attribute => {
          gab.setRmsMappingQuality(attribute.toInt)
        })
      }).getOrElse(gab)
  }

  private[converters] def formatPhaseSetQuality(g: HtsjdkGenotype,
                                                gab: GenotypeAnnotation.Builder,
                                                idx: Int,
                                                indices: Array[Int]): GenotypeAnnotation.Builder = {

    Option(g.getExtendedAttribute("PQ"))
      .map(attr => {
        tryAndCatchStringCast(attr, attribute => {
          gab.setPhaseSetQuality(attribute.asInstanceOf[java.lang.Integer])
        }, attribute => {
          gab.setPhaseSetQuality(attribute.toInt)
        })
      }).getOrElse(gab)
  }

  private[converters] def formatMinimumReadDepth(g: HtsjdkGenotype,
                                                 gab: GenotypeAnnotation.Builder,
                                                 idx: Int,
                                                 indices: Array[Int]): GenotypeAnnotation.Builder = {

    Option(g.getExtendedAttribute("MIN_DP", null))
      .map(attr => {
        tryAndCatchStringCast(attr, attribute => {
          gab.setMinimumReadDepth(attribute.asInstanceOf[java.lang.Integer])
        }, attribute => {
          gab.setMinimumReadDepth(attribute.toInt)
        })
      }).getOrElse(gab)
  }

  private[converters] def formatFisherStrandBiasPValue(g: HtsjdkGenotype,
                                                       gab: GenotypeAnnotation.Builder,
                                                       idx: Int,
                                                       indices: Array[Int]): GenotypeAnnotation.Builder = {

    Option(g.getExtendedAttribute("FS"))
      .map(attr => {
        tryAndCatchStringCast(attr, attribute => {
          gab.setFisherStrandBiasPValue(attribute.asInstanceOf[java.lang.Float])
        }, attribute => {
          gab.setFisherStrandBiasPValue(toFloat(attribute))
        })
      }).getOrElse(gab)
  }

  private val genotypeAnnotationFormatFns: Iterable[(HtsjdkGenotype, GenotypeAnnotation.Builder, Int, Array[Int]) => GenotypeAnnotation.Builder] = Iterable(
    formatReadDepths(_, _, _, _),
    formatForwardReadDepths(_, _, _, _),
    formatReverseReadDepths(_, _, _, _),
    formatReadDepth(_, _, _, _),
    formatLikelihoods(_, _, _, _),
    formatNonReferenceLikelihoods(_, _, _, _),
    formatPriors(_, _, _, _),
    formatPosteriors(_, _, _, _),
    formatQuality(_, _, _, _),
    formatRmsMappingQuality(_, _, _, _),
    formatPhaseSetQuality(_, _, _, _),
    formatMinimumReadDepth(_, _, _, _),
    formatFisherStrandBiasPValue(_, _, _, _)
  )

  // genotype annotation --> htsjdk extract functions

  private[converters] def extractReadDepths(ga: GenotypeAnnotation,
                                            gb: GenotypeBuilder): GenotypeBuilder = {

    (Option(ga.getReferenceReadDepth), Option(ga.getAlternateReadDepth)) match {
      case (Some(ref), Some(alt)) => gb.AD(Array(ref, alt))
      case (Some(_), None) => {
        throw new IllegalArgumentException("Had reference depth but no alternate depth in %s.".format(ga))
      }
      case (None, Some(_)) => {
        throw new IllegalArgumentException("Had alternate depth but no reference depth in %s.".format(ga))
      }
      case _ => gb.noAD
    }
  }

  private[converters] def extractForwardReadDepths(ga: GenotypeAnnotation,
                                                   gb: GenotypeBuilder): GenotypeBuilder = {

    (Option(ga.getReferenceForwardReadDepth), Option(ga.getAlternateForwardReadDepth)) match {
      case (Some(ref), Some(alt)) => gb.attribute("ADF", ImmutableList.of(ref, alt))
      case (None, Some(_))        => throw new IllegalArgumentException("Forward read depth specified without reference forward read depth")
      case (Some(_), None)        => throw new IllegalArgumentException("Reference forward read depth specified without forward read depth")
      case _                      =>
    }
    gb
  }

  private[converters] def extractReverseReadDepths(ga: GenotypeAnnotation,
                                                   gb: GenotypeBuilder): GenotypeBuilder = {

    (Option(ga.getReferenceReverseReadDepth), Option(ga.getAlternateReverseReadDepth)) match {
      case (Some(ref), Some(alt)) => gb.attribute("ADR", ImmutableList.of(ref, alt))
      case (None, Some(_))        => throw new IllegalArgumentException("Reverse read depth specified without reference reverse read depth")
      case (Some(_), None)        => throw new IllegalArgumentException("Reference reverse read depth specified without reverse read depth")
      case _                      =>
    }
    gb
  }

  private[converters] def extractReadDepth(ga: GenotypeAnnotation,
                                           gb: GenotypeBuilder): GenotypeBuilder = {
    Option(ga.getReadDepth).fold(gb.noDP)(gb.DP(_))
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

  /*
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
   */

  private[converters] def extractLikelihoods(ga: GenotypeAnnotation,
                                             gb: GenotypeBuilder): GenotypeBuilder = {
    gb
  }

  private[converters] def extractNonReferenceLikelihoods(ga: GenotypeAnnotation,
                                                         gb: GenotypeBuilder): GenotypeBuilder = {
    gb
  }

  private[converters] def extractPriors(ga: GenotypeAnnotation,
                                        gb: GenotypeBuilder): GenotypeBuilder = {
    Option(ga.getPriors).fold(gb)(gb.attribute("priors", _))
  }

  private[converters] def extractPosteriors(ga: GenotypeAnnotation,
                                            gb: GenotypeBuilder): GenotypeBuilder = {
    // todo: convert back from log scale to log10 scale
    Option(ga.getPosteriors).fold(gb)(gb.attribute("GP", _))
  }

  private[converters] def extractQuality(ga: GenotypeAnnotation,
                                         gb: GenotypeBuilder): GenotypeBuilder = {
    Option(ga.getQuality).fold(gb.noGQ)(gb.GQ(_))
  }

  private[converters] def extractRmsMappingQuality(ga: GenotypeAnnotation,
                                                   gb: GenotypeBuilder): GenotypeBuilder = {
    Option(ga.getRmsMappingQuality).fold(gb)(gb.attribute("MQ", _))
  }

  private[converters] def extractPhaseSetQuality(ga: GenotypeAnnotation,
                                                 gb: GenotypeBuilder): GenotypeBuilder = {
    Option(ga.getPhaseSetQuality).fold(gb)(gb.attribute("PQ", _))
  }

  private[converters] def extractMinimumReadDepth(ga: GenotypeAnnotation,
                                                  gb: GenotypeBuilder): GenotypeBuilder = {
    Option(ga.getMinimumReadDepth).fold(gb)(gb.attribute("MIN_DP", _))
  }

  private[converters] def extractFisherStrandBiasPValue(ga: GenotypeAnnotation,
                                                        gb: GenotypeBuilder): GenotypeBuilder = {
    Option(ga.getFisherStrandBiasPValue).fold(gb)(gb.attribute("FS", _))
  }

  private val genotypeAnnotationExtractFns: Iterable[(GenotypeAnnotation, GenotypeBuilder) => GenotypeBuilder] = Iterable(
    extractReadDepths(_, _),
    extractForwardReadDepths(_, _),
    extractReverseReadDepths(_, _),
    extractReadDepth(_, _),
    extractLikelihoods(_, _),
    extractNonReferenceLikelihoods(_, _),
    extractPriors(_, _),
    extractPosteriors(_, _),
    extractQuality(_, _),
    extractRmsMappingQuality(_, _),
    extractPhaseSetQuality(_, _),
    extractMinimumReadDepth(_, _),
    extractFisherStrandBiasPValue(_, _)
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
    stringency: ValidationStringency = ValidationStringency.STRICT): (HtsjdkVariantContext, Option[String], Int, Boolean) => Variant = {

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
                wasSplit: Boolean): Variant = {

      // create the builder
      val variantBuilder = Variant.newBuilder
        .setReferenceName(vc.getChr)
        .setStart(vc.getStart - 1)
        .setEnd(vc.getEnd)
        .setReferenceAllele(vc.getReference.getBaseString)

      // was this split?
      if (wasSplit) {
        variantBuilder.setSplitFromMultiallelic(true)
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

      val variant = variantBuilder.build()
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
      variantBuilder.build
    }

    convert(_, _, _, _)
  }

  private def makeGenotypeFormatFn(
    headerLines: Seq[VCFHeaderLine]): (HtsjdkGenotype, Variant, HtsjdkAllele, Int, Option[Int], Boolean) => Genotype = {

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
                allele: HtsjdkAllele,
                alleleIdx: Int,
                nonRefIndex: Option[Int],
                wasSplit: Boolean): Genotype = {

      // create the builder
      val builder = Genotype.newBuilder()
        .setReferenceName(variant.getReferenceName)
        .setStart(variant.getStart)
        .setEnd(variant.getEnd)
        .setReferenceAllele(variant.getReferenceAllele)
        .setAlternateAllele(variant.getAlternateAllele)
        .setSampleId(g.getSampleName)
        .setAlleles(g.getAlleles.map(gtAllele => {
          if (gtAllele.isReference) {
            Allele.REF
          } else if (gtAllele.isNoCall) {
            Allele.NO_CALL
          } else if (gtAllele.equals(allele, true)) {
            Allele.ALT
          } else {
            Allele.OTHER_ALT
          }
        }))

      // was this split?
      if (wasSplit) {
        builder.setSplitFromMultiallelic(true)
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

      val gAnns = GenotypeAnnotation.newBuilder

      // bind the annotation conversion functions and fold
      val boundAnnotationFns: Iterable[GenotypeAnnotation.Builder => GenotypeAnnotation.Builder] = genotypeAnnotationFormatFns
        .map(fn => {
          fn(g, _: GenotypeAnnotation.Builder, alleleIdx, indices)
        })
      val convertedAnnotations = boundAnnotationFns.foldLeft(gAnns)(
        (gab: GenotypeAnnotation.Builder, fn) => {
          try {
            fn(gab)
          } catch {
            case t: Throwable => {
              if (stringency == ValidationStringency.STRICT) {
                throw t
              } else {
                if (stringency == ValidationStringency.LENIENT) {
                  log.warn("Converting genotype annotation field in %s with function %s failed: %s".format(
                    g, fn, t))
                }
                gab
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
      val gtWithAnnotations = convertedCore
        .setAnnotation(convertedAnnotationsWithAttrs.build)

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
        .exists(_.getAnnotation.getNonReferenceLikelihoods.length != 0)
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
      val gtWithAnnotations = Option(g.getAnnotation)
        .fold(convertedCore)(ga => {

          // bind the annotation conversion functions and fold
          val convertedAnnotations = genotypeAnnotationExtractFns.foldLeft(convertedCore)(
            (gb: GenotypeBuilder, fn) => {
              try {
                fn(ga, gb)
              } catch {
                case t: Throwable => {
                  if (stringency == ValidationStringency.STRICT) {
                    throw t
                  } else {
                    if (stringency == ValidationStringency.LENIENT) {
                      log.warn("Applying annotation extraction function %s to %s failed with %s.".format(fn, ga, t))
                    }

                    gb
                  }
                }
              }
            })

          // get the attribute map
          val attributes: Map[String, String] = ga.getAttributes.toMap

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
                    log.warn("Applying attribute extraction function %s to %s failed with %s.".format(fn, ga, t))
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
