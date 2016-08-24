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

import htsjdk.variant.variantcontext.{
  Allele,
  GenotypesContext,
  GenotypeLikelihoods,
  VariantContext => HtsjdkVariantContext,
  VariantContextBuilder
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
import scala.collection.mutable.HashMap

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
        assert(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        val variant = createADAMVariant(vc, Some(allele.getDisplayString))
        val genotypes = extractReferenceModelGenotypes(vc, variant, calling_annotations)
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case List(allele, NON_REF_ALLELE) => {
        assert(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        val variant = createADAMVariant(vc, Some(allele.getDisplayString))
        val genotypes = extractReferenceModelGenotypes(vc, variant, calling_annotations)
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case alleles :+ NON_REF_ALLELE => {
        throw new IllegalArgumentException("Multi-allelic site with non-ref symbolic allele" +
          vc.toString)
      }
      case _ => {
        // Default case is multi-allelic without reference model
        val vcb = new VariantContextBuilder(vc)
        return vc.getAlternateAlleles.flatMap(allele => {
          val idx = vc.getAlleleIndex(allele)
          assert(idx >= 1, "Unexpected index for alternate allele: " + vc.toString)
          vcb.alleles(List(vc.getReference, allele, NON_REF_ALLELE))

          def punchOutGenotype(g: htsjdk.variant.variantcontext.Genotype, idx: Int): htsjdk.variant.variantcontext.Genotype = {

            val gb = new htsjdk.variant.variantcontext.GenotypeBuilder(g)
            // TODO: Multi-allelic genotypes are locally phased, add phase set
            gb.phased(true)

            if (g.hasAD) {
              val ad = g.getAD
              gb.AD(Array(ad(0), ad(idx)))
            }

            // Recompute PLs as needed to reflect stripped alleles.
            // TODO: Collapse other alternate alleles into a single set of probabilities.
            if (g.hasPL) {
              val oldPLs = g.getPL
              val maxIdx = oldPLs.length
              val newPLs = GenotypeLikelihoods.getPLIndecesOfAlleles(0, idx).filter(_ < maxIdx).map(oldPLs(_))
              // Normalize new likelihoods in log-space
              gb.PL(newPLs.map(_ - newPLs.min))
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
   * @return The database annotations in Avro format.
   */
  def convertToAnnotation(vc: HtsjdkVariantContext): DatabaseVariantAnnotation = {
    val variant = vc.getAlternateAlleles.toList match {
      case List(NON_REF_ALLELE) => {
        createADAMVariant(vc, None /* No alternate allele */ )
      }
      case List(allele) => {
        assert(
          allele.isNonReference,
          "Assertion failed when converting: " + vc.toString
        )
        createADAMVariant(vc, Some(allele.getDisplayString))
      }
      case List(allele, NON_REF_ALLELE) => {
        assert(
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

    extractVariantDatabaseAnnotation(variant, vc)
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
   * Builds an avro Variant for a site with a defined alt allele.
   *
   * @param vc htsjdk variant context to use for building the site.
   * @param alt The alternate allele to use for the site. If not provided, no
   *   alternate allele will be defined.
   * @return Returns an Avro description of the genotyped site.
   */
  private def createADAMVariant(vc: HtsjdkVariantContext, alt: Option[String]): Variant = {
    // VCF CHROM, POS, REF and ALT
    val builder = Variant.newBuilder
      .setContigName(createContig(vc))
      .setStart(vc.getStart - 1 /* ADAM is 0-indexed */ )
      .setEnd(vc.getEnd /* ADAM is 0-indexed, so the 1-indexed inclusive end becomes exclusive */ )
      .setReferenceAllele(vc.getReference.getBaseString)
    if (vc.hasLog10PError) {
      builder.setVariantErrorProbability(vc.getPhredScaledQual.intValue())
    }
    alt.foreach(builder.setAlternateAllele(_))
    builder.build
  }

  /**
   * Populates a site annotation from an htsjdk variant context.
   *
   * @param variant Avro variant representation for the site.
   * @param vc htsjdk representation of the VCF line.
   * @return Returns the Avro representation of the annotations at this site
   *   that indicate membership in an annotation database.
   */
  private def extractVariantDatabaseAnnotation(variant: Variant,
                                               vc: HtsjdkVariantContext): DatabaseVariantAnnotation = {
    val annotation = DatabaseVariantAnnotation.newBuilder()
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
          .setVariantCallingAnnotations(annotations)
          .setSampleId(g.getSampleName)
          .setAlleles(g.getAlleles.map(VariantContextConverter.convertAllele(vc, _)))
          .setPhased(g.isPhased)

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
        val splitAt: Int = g.getPloidy match {
          case 1 => 2
          case 2 => 3
          case _ => require(false, "Ploidy > 2 not supported for this operation"); 0
        }
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

    // VCF QUAL, FILTER and INFO fields
    if (vc.filtersWereApplied && vc.isFiltered) {
      call.setVariantIsPassing(false).setVariantFilters(new java.util.ArrayList(vc.getFilters))
    } else if (vc.filtersWereApplied) {
      call.setVariantIsPassing(true)
    }

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
    val variant: Variant = vc.variant
    val vcb = new VariantContextBuilder()
      .chr(refSeqToContig.getOrElse(
        variant.getContigName,
        variant.getContigName
      ))
      .start(variant.getStart + 1 /* Recall ADAM is 0-indexed */ )
      .stop(variant.getStart + variant.getReferenceAllele.length)
      .alleles(VariantContextConverter.convertAlleles(variant))

    vc.databases.flatMap(d => Option(d.getDbSnpId)).foreach(d => vcb.id("rs" + d))

    // TODO: Extract provenance INFO fields
    try {
      vcb.genotypes(vc.genotypes.map(g => {
        val gb = new htsjdk.variant.variantcontext.GenotypeBuilder(
          g.getSampleId, VariantContextConverter.convertAlleles(g)
        )

        Option(g.getPhased).foreach(gb.phased(_))
        Option(g.getGenotypeQuality).foreach(gb.GQ(_))
        Option(g.getReadDepth).foreach(gb.DP(_))

        if (g.getReferenceReadDepth != null && g.getAlternateReadDepth != null)
          gb.AD(Array(g.getReferenceReadDepth, g.getAlternateReadDepth))

        if (g.getVariantCallingAnnotations != null) {
          val callAnnotations = g.getVariantCallingAnnotations()
          if (callAnnotations.getVariantFilters != null) {
            gb.filters(callAnnotations.getVariantFilters)
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
