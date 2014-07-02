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

import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.models.{ ADAMVariantContext, SequenceDictionary }
import org.broadinstitute.variant.variantcontext._
import scala.collection.JavaConversions._
import java.util.Collections

object VariantContextConverter {
  private val NON_REF_ALLELE = Allele.create("<NON_REF>", false /* !Reference */ )
  private lazy val splitFromMultiAllelicField = ADAMGenotype.SCHEMA$.getField("splitFromMultiAllelic")

  // One conversion method for each way of representing an Allele
  private def convertAllele(vc: VariantContext, allele: Allele): ADAMGenotypeAllele = {
    if (allele.isNoCall) ADAMGenotypeAllele.NoCall
    else if (allele.isReference) ADAMGenotypeAllele.Ref
    else if (allele == NON_REF_ALLELE || !vc.hasAlternateAllele(allele)) ADAMGenotypeAllele.OtherAlt
    else ADAMGenotypeAllele.Alt
  }

  private def convertAllele(allele: CharSequence, isRef: Boolean = false): Seq[Allele] = {
    if (allele == null)
      Seq()
    else
      Seq(Allele.create(allele.toString, isRef))
  }

  private def convertAlleles(v: ADAMVariant): java.util.Collection[Allele] = {
    convertAllele(v.getReferenceAllele, true) ++ convertAllele(v.getVariantAllele)
  }

  private def convertAlleles(g: ADAMGenotype): java.util.List[Allele] = {
    var alleles = g.getAlleles
    if (alleles == null) return Collections.emptyList[Allele]
    else g.getAlleles.map {
      case ADAMGenotypeAllele.NoCall                            => Allele.NO_CALL
      case ADAMGenotypeAllele.Ref | ADAMGenotypeAllele.OtherAlt => Allele.create(g.getVariant.getReferenceAllele.toString, true)
      case ADAMGenotypeAllele.Alt                               => Allele.create(g.getVariant.getVariantAllele.toString)
    }
  }

}

/**
 * This class converts VCF data to and from ADAM. This translation occurs at the abstraction level
 * of the GATK VariantContext which represents VCF data, and at the ADAMVariantContext level, which
 * aggregates ADAM variant/genotype/annotation data together.
 *
 * If an annotation has a corresponding set of fields in the VCF standard, a conversion to/from the
 * GATK VariantContext should be implemented in this class.
 */
class VariantContextConverter(dict: Option[SequenceDictionary] = None) extends Serializable {
  import VariantContextConverter._

  // Mappings between the CHROM names typically used and the more specific RefSeq accessions
  private lazy val contigToRefSeq: Map[String, String] = dict match {
    case Some(d) => d.records.filter(_.refseq.isDefined).map(r => r.name -> r.refseq.get).toMap
    case _       => Map.empty
  }

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
  def convert(vc: VariantContext): Seq[ADAMVariantContext] = {

    // INFO field variant calling annotations, e.g. MQ
    lazy val calling_annotations: VariantCallingAnnotations = extractVariantCallingAnnotations(vc)

    vc.getAlternateAlleles.toList match {
      case List(NON_REF_ALLELE) => {
        val variant = createADAMVariant(vc, None /* No alternate allele */ )
        val genotypes = extractReferenceGenotypes(vc, variant, calling_annotations)
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case List(allele) => {
        assert(allele.isNonReference && !allele.isSymbolic)
        val variant = createADAMVariant(vc, Some(allele.getBaseString))
        val genotypes = extractNonReferenceGenotypes(vc, variant, calling_annotations)
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case List(allele, NON_REF_ALLELE) => {
        assert(allele.isNonReference && !allele.isSymbolic)
        val variant = createADAMVariant(vc, Some(allele.getBaseString))
        val genotypes = extractReferenceModelGenotypes(vc, variant, calling_annotations)
        return Seq(ADAMVariantContext(variant, genotypes, None))
      }
      case alleles :+ NON_REF_ALLELE => {
        assert(false, "Multi-allelic site with non-ref symbolic allele")
        Seq()
      }
      case _ => {
        // Default case is multi-allelic without reference model
        val vcb = new VariantContextBuilder(vc)
        return vc.getAlternateAlleles.flatMap(allele => {
          val idx = vc.getAlleleIndex(allele)
          assert(idx >= 1, "Unexpected index for alternate allele")
          vcb.alleles(List(vc.getReference, allele, NON_REF_ALLELE))

          def punchOutGenotype(g: Genotype, idx: Int): Genotype = {
            val gb = new GenotypeBuilder(g)
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
              val newPLs = GenotypeLikelihoods.getPLIndecesOfAlleles(0, idx).map(oldPLs(_))
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

    /*
    val annotation: Option[ADAMDatabaseVariantAnnotation] =
      if (extractExternalAnnotations)
        Some(extractVariantDatabaseAnnotation(variant, vc))
      else
        None
     */
  }

  def convertToAnnotation(vc: VariantContext): ADAMDatabaseVariantAnnotation = {
    assert(false, "TODO")
    /*
    val variant = createADAMVariant(vc)
    extractVariantDatabaseAnnotation(variant, vc)
    */
    new ADAMDatabaseVariantAnnotation()
  }

  private def createContig(vc: VariantContext): ADAMContig = {
    val contigName = contigToRefSeq.getOrElse(vc.getChr, vc.getChr)

    ADAMContig.newBuilder()
      .setContigName(contigName)
      .build()
  }

  private def createADAMVariant(vc: VariantContext, alt: Option[String]): ADAMVariant = {
    // VCF CHROM, POS, REF and ALT
    val builder = ADAMVariant.newBuilder
      .setContig(createContig(vc))
      .setPosition(vc.getStart - 1 /* ADAM is 0-indexed */ )
      .setExclusiveEnd(vc.getEnd /* ADAM is 0-indexed, so the 1-indexed inclusive end becomes exclusive */ )
      .setReferenceAllele(vc.getReference.getBaseString)
    alt.foreach(builder.setVariantAllele(_))
    builder.build
  }

  private def extractVariantDatabaseAnnotation(variant: ADAMVariant, vc: VariantContext): ADAMDatabaseVariantAnnotation = {
    val annotation = ADAMDatabaseVariantAnnotation.newBuilder()
      .setVariant(variant)
      .build

    VariantAnnotationConverter.convert(vc, annotation)

  }

  private def extractGenotypes(
    vc: VariantContext,
    variant: ADAMVariant,
    annotations: VariantCallingAnnotations,
    setPL: (Genotype, ADAMGenotype.Builder) => Unit): Seq[ADAMGenotype] = {

    val genotypes: Seq[ADAMGenotype] = vc.getGenotypes.map((g: Genotype) => {
      val genotype: ADAMGenotype.Builder = ADAMGenotype.newBuilder
        .setVariant(variant)
        .setVariantCallingAnnotations(annotations)
        .setSampleId(g.getSampleName)
        .setAlleles(g.getAlleles.map(VariantContextConverter.convertAllele(vc, _)))
        .setIsPhased(g.isPhased)

      if (g.hasGQ) genotype.setGenotypeQuality(g.getGQ)
      if (g.hasDP) genotype.setReadDepth(g.getDP)
      if (g.hasAD) {
        val ad = g.getAD
        genotype.setReferenceReadDepth(ad(0)).setAlternateReadDepth(ad(1))
      }
      setPL(g, genotype)

      VariantAnnotationConverter.convert(g, genotype.build)
    }).toSeq

    genotypes
  }

  private def extractNonReferenceGenotypes(vc: VariantContext, variant: ADAMVariant, annotations: VariantCallingAnnotations): Seq[ADAMGenotype] = {
    assert(vc.isBiallelic)
    extractGenotypes(vc, variant, annotations, (g, b) => {
      if (g.hasPL) b.setGenotypeLikelihoods(g.getPL.toList.map(p => p: java.lang.Integer))
    })
  }

  private def extractReferenceGenotypes(vc: VariantContext, variant: ADAMVariant, annotations: VariantCallingAnnotations): Seq[ADAMGenotype] = {
    assert(vc.isBiallelic)
    extractGenotypes(vc, variant, annotations, (g, b) => {
      if (g.hasPL) b.setNonReferenceLikelihoods(g.getPL.toList.map(p => p: java.lang.Integer))
    })
  }

  private def extractReferenceModelGenotypes(vc: VariantContext, variant: ADAMVariant, annotations: VariantCallingAnnotations): Seq[ADAMGenotype] = {
    extractGenotypes(vc, variant, annotations, (g, b) => {
      if (g.hasPL) {
        val pls = g.getPL.map(p => p: java.lang.Integer)
        val splitAt: Int = g.getPloidy match {
          case 1 => 2
          case 2 => 3
          case _ => assert(false, "Ploidy > 2 not supported for this operation"); 0
        }
        b.setGenotypeLikelihoods(pls.slice(0, splitAt).toList)
        b.setNonReferenceLikelihoods(pls.slice(splitAt, pls.length).toList)
      }
    })
  }

  private def extractVariantCallingAnnotations(vc: VariantContext): VariantCallingAnnotations = {
    val call: VariantCallingAnnotations.Builder = VariantCallingAnnotations.newBuilder

    // VCF QUAL, FILTER and INFO fields
    if (vc.hasLog10PError) {
      call.setVariantCallErrorProbability(vc.getPhredScaledQual.asInstanceOf[Float])
    }

    if (vc.filtersWereApplied && vc.isFiltered) {
      call.setVariantIsPassing(false).setVariantFilters(new java.util.ArrayList(vc.getFilters))
    } else if (vc.filtersWereApplied) {
      call.setVariantIsPassing(true)
    }

    VariantAnnotationConverter.convert(vc, call.build())
  }

  /**
   * Convert an ADAMVariantContext into the equivalent GATK VariantContext
   * @param vc
   * @return GATK VariantContext
   */
  def convert(vc: ADAMVariantContext): VariantContext = {
    val variant: ADAMVariant = vc.variant
    val vcb = new VariantContextBuilder()
      .chr(refSeqToContig.getOrElse(variant.getContig.getContigName.toString,
        variant.getContig.getContigName.toString))
      .start(variant.getPosition + 1 /* Recall ADAM is 0-indexed */ )
      .stop(variant.getPosition + variant.getReferenceAllele.length)
      .alleles(VariantContextConverter.convertAlleles(variant))

    vc.databases.flatMap(d => Option(d.getDbSnpId)).foreach(d => vcb.id("rs" + d))

    // TODO: Extract provenance INFO fields
    vcb.genotypes(vc.genotypes.map(g => {
      val gb = new GenotypeBuilder(g.getSampleId.toString, VariantContextConverter.convertAlleles(g))

      Option(g.getIsPhased).foreach(gb.phased(_))
      Option(g.getGenotypeQuality).foreach(gb.GQ(_))
      Option(g.getReadDepth).foreach(gb.DP(_))

      if (g.getReferenceReadDepth != null && g.getAlternateReadDepth != null)
        gb.AD(Array(g.getReferenceReadDepth, g.getAlternateReadDepth))

      if (g.getVariantCallingAnnotations != null) {
        val callAnnotations = g.getVariantCallingAnnotations()
        if (callAnnotations.getVariantFilters != null)
          gb.filters(callAnnotations.getVariantFilters.map(_.toString))
      }

      if (g.getGenotypeLikelihoods != null && !g.getGenotypeLikelihoods.isEmpty)
        gb.PL(g.getGenotypeLikelihoods.map(p => p: Int).toArray)

      gb.make
    }))

    vcb.make
  }

}
