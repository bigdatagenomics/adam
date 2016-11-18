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
  VCFConstants,
  VCFFormatHeaderLine,
  VCFHeaderLine,
  VCFHeaderLineCount,
  VCFHeaderLineType
}
import java.util.Collections
import org.bdgenomics.utils.misc.Logging
import org.bdgenomics.adam.models.{
  SequenceDictionary,
  VariantContext => ADAMVariantContext
}
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro._
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
private[adam] object VariantContextConverter {

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

  /**
   * Converts the alleles in a variant into a Java collection of htsjdk alleles.
   *
   * @param v Avro model of the variant at a site.
   * @return Returns a Java collection representing the reference allele and any
   *   alternate allele at the site.
   */
  private def convertAlleles(v: Variant): java.util.Collection[Allele] = {
    val asSeq = Seq(convertAlleleOpt(v.getReferenceAllele, true),
      convertAlleleOpt(v.getAlternateAllele)).flatten

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
      case GenotypeAllele.NO_CALL                        => Allele.NO_CALL
      case GenotypeAllele.REF | GenotypeAllele.OTHER_ALT => Allele.create(g.getVariant.getReferenceAllele, true)
      case GenotypeAllele.ALT                            => Allele.create(g.getVariant.getAlternateAllele)
    }
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
private[adam] class VariantContextConverter(
    headerLines: Seq[VCFHeaderLine]) extends Serializable with Logging {
  import VariantContextConverter._

  private val htsjdkConvFn = makeHtsjdkGenotypeConverter(headerLines)
  private val bdgConvFn = makeBdgGenotypeConverter(headerLines)

  /**
   * Converts a Scala float to a Java float.
   *
   * @param f Scala floating point value.
   * @return Java floating point value.
   */
  private def jFloat(f: Float): java.lang.Float = f

  /**
   * Converts a single GATK variant into ADAMVariantContext(s).
   *
   * @param vc GATK Variant context to convert.
   * @return ADAM variant contexts
   */
  def convert(vc: HtsjdkVariantContext): Seq[ADAMVariantContext] = {

    vc.getAlternateAlleles.toList match {
      case List(NON_REF_ALLELE) => {
        val variant = createADAMVariant(vc, None /* No alternate allele */ )
        val genotypes = vc.getGenotypes.map(g => {
          htsjdkConvFn(g, variant, NON_REF_ALLELE, 0, Some(1), false)
        })
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case List(allele) => {
        require(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        val variant = createADAMVariant(vc, Some(allele.getDisplayString))
        val genotypes = vc.getGenotypes.map(g => {
          htsjdkConvFn(g, variant, allele, 1, None, false)
        })
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case List(allele, NON_REF_ALLELE) => {
        require(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        val variant = createADAMVariant(vc, Some(allele.getDisplayString))
        val genotypes = vc.getGenotypes.map(g => {
          htsjdkConvFn(g, variant, allele, 1, Some(2), false)
        })
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case _ => {
        val vcb = new VariantContextBuilder(vc)

        // is the last allele the non-ref allele?
        val alleles = vc.getAlternateAlleles.toSeq
        val referenceModelIndex = if (alleles.nonEmpty && alleles.last == NON_REF_ALLELE) {
          Some(alleles.length - 1)
        } else {
          None
        }
        val altAlleles = if (referenceModelIndex.isDefined) {
          alleles.dropRight(1)
        } else {
          alleles
        }

        return altAlleles.flatMap(allele => {
          val idx = vc.getAlleleIndex(allele)
          require(idx >= 1, "Unexpected index for alternate allele: " + vc.toString)
          val variant = createADAMVariant(vc, Some(allele.getDisplayString))

          val genotypes = vc.getGenotypes.map(g => {
            htsjdkConvFn(g, variant, allele, idx, referenceModelIndex, true)
          })
          Seq(ADAMVariantContext(variant, genotypes, None))
        })
      }
    }
  }

  /**
   * Extracts a variant annotation from a htsjdk VariantContext.
   *
   * @param vc htsjdk variant context to extract annotations from.
   * @return The variant annotations in Avro format.
   */
  def convertToVariantAnnotation(vc: HtsjdkVariantContext): VariantAnnotation = {
    val variant = vc.getAlternateAlleles.toList match {
      case List(NON_REF_ALLELE) => {
        createADAMVariant(vc, None /* No alternate allele */ )
      }
      case List(allele) => {
        require(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        createADAMVariant(vc, Some(allele.getDisplayString))
      }
      case List(allele, NON_REF_ALLELE) => {
        require(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        createADAMVariant(vc, Some(allele.getDisplayString))
      }
      case alleles :+ NON_REF_ALLELE => {
        throw new IllegalArgumentException("Multi-allelic site with non-ref symbolic allele " +
          vc.toString)
      }
      case _ => {
        throw new IllegalArgumentException("Multi-allelic site " + vc.toString)
      }
    }

    extractVariantAnnotation(variant, vc)
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

  /**
   * Builds an avro Variant for a site with a defined alt allele.
   *
   * @param vc htsjdk variant context to use for building the site.
   * @param alt The alternate allele to use for the site. If not provided, no
   *   alternate allele will be defined.
   * @return Returns an Avro description of the genotyped site.
   */
  private def createADAMVariant(vc: HtsjdkVariantContext, alt: Option[String]): Variant = {
    // VCF CHROM, POS, ID, REF, FORMAT, and ALT
    val builder = Variant.newBuilder
      .setContigName(vc.getChr)
      .setStart(vc.getStart - 1 /* ADAM is 0-indexed */ )
      .setEnd(vc.getEnd /* ADAM is 0-indexed, so the 1-indexed inclusive end becomes exclusive */ )
      .setReferenceAllele(vc.getReference.getBaseString)
    alt.foreach(builder.setAlternateAllele(_))
    splitIds(vc).foreach(builder.setNames(_))
    builder.setFiltersApplied(vc.filtersWereApplied)
    if (vc.filtersWereApplied) {
      builder.setFiltersPassed(!vc.isFiltered)
    }
    if (vc.isFiltered) {
      builder.setFiltersFailed(new java.util.ArrayList(vc.getFilters));
    }
    if (vc.getAttributeAsBoolean("SOMATIC", false)) {
      builder.setSomatic(true)
    }
    builder.build
  }

  /**
   * Populates a variant annotation from an htsjdk variant context.
   *
   * @param variant Avro variant representation for the site.
   * @param vc htsjdk representation of the VCF line.
   * @return Returns the Avro representation of the variant annotations at this site.
   */
  private def extractVariantAnnotation(variant: Variant,
                                       vc: HtsjdkVariantContext): VariantAnnotation = {
    ???
  }

  private[converters] def formatAllelicDepth(g: HtsjdkGenotype,
                                             gb: Genotype.Builder,
                                             gIdx: Int,
                                             gIndices: Array[Int]): Genotype.Builder = {

    // AD is an array type field
    if (g.hasAD) {
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
      gb.setGenotypeLikelihoods(gIndices.map(idx => {
        jFloat(PhredUtils.phredToLogProbability(pl(idx)))
      }).toList)
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
        jFloat(PhredUtils.phredToLogProbability(pl(idx)))
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

  private val coreFormatFieldConversionFns: Iterable[(HtsjdkGenotype, Genotype.Builder, Int, Array[Int]) => Genotype.Builder] = Iterable(
    formatAllelicDepth(_, _, _, _),
    formatReadDepth(_, _, _, _),
    formatMinReadDepth(_, _, _, _),
    formatGenotypeQuality(_, _, _, _),
    formatGenotypeLikelihoods(_, _, _, _),
    formatStrandBiasComponents(_, _, _, _),
    formatPhaseInfo(_, _, _, _)
  )

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

  private[converters] def extractGenotypeLikelihoods(g: Genotype,
                                                     gb: GenotypeBuilder): GenotypeBuilder = {
    val gls = g.getGenotypeLikelihoods

    if (gls.isEmpty) {
      gb.noPL
    } else {
      gb.PL(gls.map(l => PhredUtils.logProbabilityToPhred(l))
        .toArray)
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

  private val coreFormatFieldExtractorFns: Iterable[(Genotype, GenotypeBuilder) => GenotypeBuilder] = Iterable(
    extractAllelicDepth(_, _),
    extractReadDepth(_, _),
    extractMinReadDepth(_, _),
    extractGenotypeQuality(_, _),
    extractGenotypeLikelihoods(_, _),
    extractStrandBiasComponents(_, _),
    extractPhaseInfo(_, _)
  )

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
          vcab.setFisherStrandBiasPValue(attribute.toFloat)
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
          vcab.setRmsMapQ(attribute.toFloat)
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

  private val annotationFormatFieldConversionFns: Iterable[(HtsjdkGenotype, VariantCallingAnnotations.Builder, Int, Array[Int]) => VariantCallingAnnotations.Builder] = Iterable(
    formatFilters(_, _, _, _),
    formatFisherStrandBias(_, _, _, _),
    formatRmsMapQ(_, _, _, _),
    formatMapQ0(_, _, _, _)
  )

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

  private val annotationFormatFieldExtractorFns: Iterable[(VariantCallingAnnotations, GenotypeBuilder) => GenotypeBuilder] = Iterable(
    extractFilters(_, _),
    extractFisherStrandBias(_, _),
    extractRmsMapQ(_, _),
    extractMapQ0(_, _)
  )

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
    }, o => o.toFloat)
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
      splitAndCheckForEmptyArray(o).map(_.toFloat)
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
    } else {
      indices.map(idx => array(idx))
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

  private def lineToExtractor(
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
              fromArrayExtractor(g, id, toFn, idx)
                .map(kv => (kv._1, kv._2.toString))
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

  private[converters] def makeHtsjdkGenotypeConverter(
    headerLines: Seq[VCFHeaderLine]): (HtsjdkGenotype, Variant, Allele, Int, Option[Int], Boolean) => Genotype = {

    val attributeFns: Iterable[(HtsjdkGenotype, Int, Array[Int]) => Option[(String, String)]] = headerLines
      .flatMap(hl => hl match {
        case fl: VCFFormatHeaderLine => {

          // get the id of this line
          val key = fl.getID

          // filter out the lines that we already support
          if (SupportedHeaderLines.formatHeaderLines
            .find(_.getID == key)
            .isEmpty) {

            Some(lineToExtractor(fl))
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
        .setContigName(variant.getContigName)
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
      val boundFns: Iterable[Genotype.Builder => Genotype.Builder] = coreFormatFieldConversionFns
        .map(fn => {
          fn(g, _: Genotype.Builder, alleleIdx, indices)
        })
      val convertedCore = boundFns.foldLeft(builder)((gb: Genotype.Builder, fn) => fn(gb))

      // if we have a non-ref allele, fold and build
      val coreWithOptNonRefs = nonRefIndex.fold(convertedCore)(nonRefAllele => {

        // non-ref pl indices
        val nrIndices = GenotypeLikelihoods.getPLIndecesOfAlleles(alleleIdx, nonRefAllele)

        formatNonRefGenotypeLikelihoods(g, convertedCore, nrIndices)
      })

      val vcAnns = VariantCallingAnnotations.newBuilder

      // bind the annotation conversion functions and fold
      val boundAnnotationFns: Iterable[VariantCallingAnnotations.Builder => VariantCallingAnnotations.Builder] = annotationFormatFieldConversionFns
        .map(fn => {
          fn(g, _: VariantCallingAnnotations.Builder, alleleIdx, indices)
        })
      val convertedAnnotations = boundAnnotationFns.foldLeft(vcAnns)(
        (vcab: VariantCallingAnnotations.Builder, fn) => fn(vcab))

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

  private def extractorFromLine(
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

  private[converters] def makeBdgGenotypeConverter(
    headerLines: Seq[VCFHeaderLine]): (Genotype) => HtsjdkGenotype = {

    val attributeFns: Iterable[(Map[String, String]) => Option[(String, java.lang.Object)]] = headerLines
      .flatMap(hl => hl match {
        case fl: VCFFormatHeaderLine => {

          // get the id of this line
          val key = fl.getID

          // filter out the lines that we already support
          if (SupportedHeaderLines.formatHeaderLines
            .find(_.getID == key)
            .isEmpty) {

            None
          } else {
            Some(extractorFromLine(fl))
          }
        }
        case _ => None
      })

    def convert(g: Genotype): HtsjdkGenotype = {

      // create the builder
      val builder = new GenotypeBuilder(g.getSampleId,
        VariantContextConverter.convertAlleles(g))

      // bind the conversion functions and fold
      val convertedCore = coreFormatFieldExtractorFns.foldLeft(builder)(
        (gb: GenotypeBuilder, fn) => fn(g, gb))

      // convert the annotations if they exist
      val gtWithAnnotations = Option(g.getVariantCallingAnnotations)
        .fold(convertedCore)(vca => {

          // bind the annotation conversion functions and fold
          val convertedAnnotations = annotationFormatFieldExtractorFns.foldLeft(convertedCore)(
            (gb: GenotypeBuilder, fn) => fn(vca, gb))

          // get the attribute map
          val attributes: Map[String, String] = vca.getAttributes.toMap

          // apply the attribute converters and return
          attributeFns.foldLeft(convertedAnnotations)((gb: GenotypeBuilder, fn) => {
            val optAttrPair = fn(attributes)

            optAttrPair.fold(gb)(pair => gb.attribute(pair._1, pair._2))
          })
        })

      // build and return
      gtWithAnnotations.make()
    }

    convert(_)
  }

  /**
   * Extracts annotations from a site.
   *
   * @param vc htsjdk variant context representing this site to extract
   *   annotations from.
   * @return Returns a variant calling annotation with the filters applied to
   *   this site.
   */
  private def extractVariantCallingAnnotations(vc: HtsjdkVariantContext): VariantCallingAnnotations = {
    ???
  }

  /**
   * Extracts VCF info fields from Avro formatted genotype.
   *
   * @param g Genotype record with possible info fields.
   * @return Mapping between VCF info fields and values from genotype record.
   */
  private def extractADAMInfoFields(g: Genotype): HashMap[String, Object] = {
    val infoFields = new HashMap[String, Object]();
    val annotations = g.getVariantCallingAnnotations
    if (annotations != null) {
      Option(annotations.getFisherStrandBiasPValue).foreach(infoFields.put("FS", _))
      Option(annotations.getRmsMapQ).foreach(infoFields.put("MQ", _))
      Option(annotations.getMapq0Reads).foreach(infoFields.put("MQ0", _))
      Option(annotations.getMqRankSum).foreach(infoFields.put("MQRankSum", _))
      Option(annotations.getReadPositionRankSum).foreach(infoFields.put("ReadPosRankSum", _))
    }
    infoFields
  }

  /**
   * Convert an ADAMVariantContext into the equivalent GATK VariantContext
   * @param vc
   * @return GATK VariantContext
   */
  def convert(
    vc: ADAMVariantContext,
    stringency: ValidationStringency = ValidationStringency.LENIENT): Option[HtsjdkVariantContext] = {
    val variant: Variant = vc.variant.variant
    val vcb = new VariantContextBuilder()
      .chr(variant.getContigName)
      .start(variant.getStart + 1 /* Recall ADAM is 0-indexed */ )
      .stop(variant.getStart + variant.getReferenceAllele.length)
      .alleles(VariantContextConverter.convertAlleles(variant))

    joinNames(variant) match {
      case None    => vcb.noID()
      case Some(s) => vcb.id(s)
    }

    val filtersApplied = Option(variant.getFiltersApplied).getOrElse(false)
    val filtersPassed = Option(variant.getFiltersPassed).getOrElse(false)

    (filtersApplied, filtersPassed) match {
      case (false, false) => vcb.unfiltered
      case (false, true)  => vcb.passFilters // log warning?
      case (true, false)  => vcb.filters(new java.util.HashSet(variant.getFiltersFailed()))
      case (true, true)   => vcb.passFilters
    }

    val somatic: java.lang.Boolean = Option(variant.getSomatic).getOrElse(false)
    if (somatic) {
      vcb.attribute("SOMATIC", true)
    }

    // attach genotypes
    try {
      Some(vcb.genotypes(vc.genotypes.map(g => bdgConvFn(g)))
        .make)
    } catch {
      case t: Throwable => {
        if (stringency == ValidationStringency.STRICT) {
          throw t
        } else {
          if (stringency == ValidationStringency.LENIENT) {
            log.error("Encountered error when converting variant context with variant: \n" +
              vc.variant.variant + "\n" +
              "and genotypes: \n" +
              vc.genotypes.mkString("\n"))
          }
          None
        }
      }
    }
  }
}
