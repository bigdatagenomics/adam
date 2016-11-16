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
import htsjdk.variant.variantcontext.{
  Allele,
  GenotypesContext,
  GenotypeLikelihoods,
  VariantContext => HtsjdkVariantContext,
  VariantContextBuilder
}
import htsjdk.variant.vcf.VCFConstants
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
 *
 * @see VariantAnnotationConverter
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
 *
 * @param dict An optional sequence dictionary to use for populating sequence metadata on conversion.
 */
private[adam] class VariantContextConverter(dict: Option[SequenceDictionary] = None) extends Serializable with Logging {
  import VariantContextConverter._

  /**
   * Converts a Scala float to a Java float.
   *
   * @param f Scala floating point value.
   * @return Java floating point value.
   */
  private def jFloat(f: Float): java.lang.Float = f

  /**
   * Mappings between the CHROM names typically used and the more specific
   * RefSeq accessions.
   *
   * @see refSeqToContig
   */
  private lazy val contigToRefSeq: Map[String, String] = dict match {
    case Some(d) => d.records.filter(_.refseq.isDefined).map(r => r.name -> r.refseq.get).toMap
    case _       => Map.empty
  }

  /**
   * Mappings between the specific RefSeq accessions and commonly used CHROM
   * names.
   *
   * @see contigToRefSeq
   */
  private lazy val refSeqToContig: Map[String, String] = dict match {
    case Some(d) => d.records.filter(_.refseq.isDefined).map(r => r.refseq.get -> r.name).toMap
    case _       => Map.empty
  }

  /**
   * Converts a single GATK variant into ADAMVariantContext(s).
   *
   * @param vc GATK Variant context to convert.
   * @return ADAM variant contexts
   */
  def convert(vc: HtsjdkVariantContext): Seq[ADAMVariantContext] = {

    // INFO field variant calling annotations, e.g. MQ
    lazy val calling_annotations: VariantCallingAnnotations = extractVariantCallingAnnotations(vc)

    vc.getAlternateAlleles.toList match {
      case List(NON_REF_ALLELE) => {
        val variant = createADAMVariant(vc, None /* No alternate allele */ )
        val genotypes = extractReferenceGenotypes(vc, variant, calling_annotations)
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case List(allele) => {
        require(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        val variant = createADAMVariant(vc, Some(allele.getDisplayString))
        val genotypes = extractReferenceModelGenotypes(vc, variant, calling_annotations)
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case List(allele, NON_REF_ALLELE) => {
        require(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        val variant = createADAMVariant(vc, Some(allele.getDisplayString))
        val genotypes = extractReferenceModelGenotypes(vc, variant, calling_annotations)
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case _ => {
        val vcb = new VariantContextBuilder(vc)

        // is the last allele the non-ref allele?
        val alleles = vc.getAlternateAlleles.toSeq
        val referenceModelIndex = if (alleles.nonEmpty && alleles.last == NON_REF_ALLELE) {
          alleles.length - 1
        } else {
          -1
        }
        val altAlleles = if (referenceModelIndex > 0) {
          alleles.dropRight(1)
        } else {
          alleles
        }

        return altAlleles.flatMap(allele => {
          val idx = vc.getAlleleIndex(allele)
          require(idx >= 1, "Unexpected index for alternate allele: " + vc.toString)
          vcb.alleles(List(vc.getReference, allele, NON_REF_ALLELE))

          def punchOutGenotype(g: htsjdk.variant.variantcontext.Genotype, idx: Int): htsjdk.variant.variantcontext.Genotype = {

            val gb = new htsjdk.variant.variantcontext.GenotypeBuilder(g)

            if (g.hasAD) {
              val ad = g.getAD
              gb.AD(Array(ad(0), ad(idx)))
            }

            // Recompute PLs as needed to reflect stripped alleles.
            // TODO: Collapse other alternate alleles into a single set of probabilities.
            if (g.hasPL) {
              val oldPLs = g.getPL
              val maxIdx = oldPLs.length

              def extractPls(idx0: Int, idx1: Int): Array[Int] = {
                GenotypeLikelihoods.getPLIndecesOfAlleles(0, idx).map(idx => {
                  require(idx < maxIdx, "Got out-of-range index (%d) for allele %s in %s.".format(
                    idx, allele, vc))
                  oldPLs(idx)
                })
              }

              val newPLs = extractPls(0, idx)
              val referencePLs = if (referenceModelIndex > 0) {
                try {
                  extractPls(idx, referenceModelIndex)
                } catch {
                  case iae: IllegalArgumentException => {
                    log.warn("Caught exception (%s) when trying to build reference model for allele %s at %s. Ignoring...".format(
                      iae.getMessage, allele, g))
                    Array.empty
                  }
                }
              } else {
                Array.empty
              }
              gb.PL(newPLs ++ referencePLs)
            }
            gb.make
          }

          // We purposely retain "invalid" genotype alleles, that will eventually become
          // "OtherAlt" entries, but won't validate against the reduced VariantContext
          val gc = GenotypesContext.create // Fixup genotypes
          gc.addAll(vc.getGenotypes.map(punchOutGenotype(_, idx)))
          vcb.genotypesNoValidation(gc)

          // Recursively convert now bi-allelic VariantContexts, setting any multi-allelic
          // specific fields afterwards
          val adamVCs = convert(vcb.make)
          adamVCs.flatMap(_.genotypes).foreach(g => g.put(splitFromMultiAllelicField.pos, true))
          adamVCs
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
   * Canonicalizes a contig name.
   *
   * Checks for a mapping between a contig and ref seq name. If none is defined,
   * returns the provided contig name.
   *
   * @param vc htsjdk variant context that is mapped to a genomic site.
   * @return Returns the canonical contig name for this site.
   */
  private def createContig(vc: HtsjdkVariantContext): String = {
    contigToRefSeq.getOrElse(vc.getChr, vc.getChr)
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
      .setContigName(createContig(vc))
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
    val annotation = VariantAnnotation.newBuilder()
      .setVariant(variant)
      .build

    VariantAnnotationConverter.convert(vc, annotation)
  }

  /**
   * For a given VCF line, pulls out the per-sample genotype calls in Avro.
   *
   * @param vc htsjdk variant context representing a VCF line.
   * @param variant Avro description for the called site.
   * @param annotations The variant calling annotations for this site.
   * @param setPL A function that maps across Genotype.Builders and sets the
   *   phred-based likelihood for a genotype called at a site.
   * @return Returns a seq containing all of the genotypes called at a single
   *   variant site.
   *
   * @see extractReferenceGenotypes
   * @see extractNonReferenceGenotypes
   * @see extractReferenceModelGenotypes
   */
  private def extractGenotypes(
    vc: HtsjdkVariantContext,
    variant: Variant,
    annotations: VariantCallingAnnotations,
    setPL: (htsjdk.variant.variantcontext.Genotype, Genotype.Builder) => Unit): Seq[Genotype] = {

    // dupe variant, get contig name/start/end and null out
    val contigName = variant.getContigName
    val start = variant.getStart
    val end = variant.getEnd
    val newVariant = Variant.newBuilder(variant)
      .setContigName(null)
      .setStart(null)
      .setEnd(null)
      .build()

    val genotypes: Seq[Genotype] = vc.getGenotypes.map(
      (g: htsjdk.variant.variantcontext.Genotype) => {
        val genotype: Genotype.Builder = Genotype.newBuilder
          .setVariant(newVariant)
          .setContigName(contigName)
          .setStart(start)
          .setEnd(end)
          .setSampleId(g.getSampleName)
          .setAlleles(g.getAlleles.map(VariantContextConverter.convertAllele(vc, _)))
          .setPhased(g.isPhased)

        // copy variant calling annotations to update filter attributes
        // (because the htsjdk Genotype is not available when build is called upstream)
        val copy = VariantCallingAnnotations.newBuilder(annotations)
        // htsjdk does not provide a field filtersWereApplied for genotype as it does in VariantContext
        copy.setFiltersApplied(true)
        copy.setFiltersPassed(!g.isFiltered)
        if (g.isFiltered) {
          copy.setFiltersFailed(Splitter.on(";").splitToList(g.getFilters))
        }
        genotype.setVariantCallingAnnotations(copy.build())

        if (g.hasGQ) genotype.setGenotypeQuality(g.getGQ)
        if (g.hasDP) genotype.setReadDepth(g.getDP)

        if (g.hasAD) {
          val ad = g.getAD
          genotype.setReferenceReadDepth(ad(0)).setAlternateReadDepth(ad(1))
        }
        setPL(g, genotype)

        VariantAnnotationConverter.convert(g, genotype.build)
      }
    ).toSeq

    genotypes
  }

  /**
   * For a given VCF line with ref + alt calls, pulls out the per-sample
   * genotype calls in Avro.
   *
   * @param vc htsjdk variant context representing a VCF line.
   * @param variant Avro description for the called site.
   * @param annotations The variant calling annotations for this site.
   * @return Returns a seq containing all of the genotypes called at a single
   *   variant site.
   *
   * @see extractGenotypes
   */
  private def extractNonReferenceGenotypes(vc: HtsjdkVariantContext,
                                           variant: Variant,
                                           annotations: VariantCallingAnnotations): Seq[Genotype] = {
    assert(vc.isBiallelic)
    extractGenotypes(vc, variant, annotations,
      (g: htsjdk.variant.variantcontext.Genotype, b: Genotype.Builder) => {
        if (g.hasPL) b.setGenotypeLikelihoods(g.getPL.toList.map(p => jFloat(PhredUtils.phredToLogProbability(p))))
      })
  }

  /**
   * For a given VCF line with reference calls, pulls out the per-sample
   * genotype calls in Avro.
   *
   * @param vc htsjdk variant context representing a VCF line.
   * @param variant Avro description for the called site.
   * @param annotations The variant calling annotations for this site.
   * @return Returns a seq containing all of the genotypes called at a single
   *   variant site.
   *
   * @see extractGenotypes
   */
  private def extractReferenceGenotypes(vc: HtsjdkVariantContext,
                                        variant: Variant,
                                        annotations: VariantCallingAnnotations): Seq[Genotype] = {
    assert(vc.isBiallelic)
    extractGenotypes(vc, variant, annotations, (g, b) => {
      if (g.hasPL) b.setNonReferenceLikelihoods(g.getPL.toList.map(p => jFloat(PhredUtils.phredToLogProbability(p))))
    })
  }

  /**
   * For a given VCF line with symbolic alleles (a la gVCF), pulls out the
   * per-sample genotype calls in Avro.
   *
   * @param vc htsjdk variant context representing a VCF line.
   * @param variant Avro description for the called site.
   * @param annotations The variant calling annotations for this site.
   * @return Returns a seq containing all of the genotypes called at a single
   *   variant site.
   *
   * @see extractGenotypes
   */
  private def extractReferenceModelGenotypes(vc: HtsjdkVariantContext,
                                             variant: Variant,
                                             annotations: VariantCallingAnnotations): Seq[Genotype] = {
    extractGenotypes(vc, variant, annotations, (g, b) => {
      if (g.hasPL) {
        val pls = g.getPL.map(p => jFloat(PhredUtils.phredToLogProbability(p)))
        val splitAt: Int = g.getPloidy + 1
        b.setGenotypeLikelihoods(pls.slice(0, splitAt).toList)
        b.setNonReferenceLikelihoods(pls.slice(splitAt, pls.length).toList)
      }
    })
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
    val call: VariantCallingAnnotations.Builder = VariantCallingAnnotations.newBuilder

    VariantAnnotationConverter.convert(vc, call.build())
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
  def convert(vc: ADAMVariantContext): HtsjdkVariantContext = {
    val variant: Variant = vc.variant.variant
    val vcb = new VariantContextBuilder()
      .chr(refSeqToContig.getOrElse(
        variant.getContigName,
        variant.getContigName
      ))
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

    // TODO: Extract provenance INFO fields
    try {
      vcb.genotypes(vc.genotypes.map(g => {
        val gb = new htsjdk.variant.variantcontext.GenotypeBuilder(
          g.getSampleId, VariantContextConverter.convertAlleles(g)
        )

        Option(g.getPhased).foreach(gb.phased(_))
        Option(g.getGenotypeQuality).foreach(gb.GQ(_))
        Option(g.getReadDepth).foreach(gb.DP(_))

        // strand bias components should have length 4 or length 0
        val strandBiasComponents = g.getStrandBiasComponents
        if (strandBiasComponents.length == 4) {
          gb.attribute("SB", strandBiasComponents)
        } else if (!strandBiasComponents.isEmpty) {
          log.warn("Ignoring bad strand bias components (%s) at %s.".format(
            strandBiasComponents.mkString(","), variant))
        }

        if (g.getReferenceReadDepth != null && g.getAlternateReadDepth != null)
          gb.AD(Array(g.getReferenceReadDepth, g.getAlternateReadDepth))

        if (g.getVariantCallingAnnotations != null) {
          val callAnnotations = g.getVariantCallingAnnotations()
          if (callAnnotations.getFiltersPassed() != null && !callAnnotations.getFiltersPassed()) {
            gb.filters(callAnnotations.getFiltersFailed())
          }
        }

        if (g.getGenotypeLikelihoods != null && !g.getGenotypeLikelihoods.isEmpty)
          gb.PL(g.getGenotypeLikelihoods.map(p => PhredUtils.logProbabilityToPhred(p)).toArray)

        gb.make
      }))
      // if only one sample then we putting stuff into vc info fields
      if (vc.genotypes.size == 1) {
        vcb.attributes(extractADAMInfoFields(vc.genotypes.toList(0)))
      }

      vcb.make
    } catch {
      case t: Throwable => {
        log.error("Encountered error when converting variant context with variant: \n" +
          vc.variant.variant + "\n" +
          "and genotypes: \n" +
          vc.genotypes.mkString("\n"))
        throw t
      }
    }
  }
}
