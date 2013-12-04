/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.commands

import org.broadinstitute.variant.variantcontext.{VariantContext, Allele, VariantContextBuilder, GenotypeBuilder, Genotype}
import edu.berkeley.cs.amplab.adam.avro.{ADAMVariant, ADAMGenotype, VariantType, ADAMVariantDomain}
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util.VcfStringUtils._

/**
 * This class converts VCF data to and from ADAM. This translation occurs at the abstraction level
 * of the GATK VariantContext which represents VCF data, and at the ADAMVariantContext level, which
 * aggregates ADAM variant/genotype/annotation data together.
 *
 * If an annotation has a corresponding set of fields in the VCF standard, a conversion to/from the
 * GATK VariantContext should be implemented in this class.
 */
class VariantContextConverter extends Serializable {

  /**
   * Converts a list of ADAMVariants into a list of alleles, and a set of tags. Adds to a
   * provided GATK VariantContext.
   *
   * @param v List of ADAMVariants.
   * @param vc VariantContext builder to modify.
   * @return A GATK VariantContextBuilder.
   */
  def convertVariants (v: Seq[ADAMVariant], vc: VariantContextBuilder): VariantContextBuilder = {

    var tagMap = Map[java.lang.String, java.lang.Object]()

    // for each variant, create an allele
    val alleleList = v.map(variant => {
      Allele.create(variant.getVariant, variant.getIsReference)
    })

    // set alleles in variant context
    vc.alleles(alleleList)

    // get fields - most can be taken from head variant
    val variant = v.head

    vc.start(variant.getPosition)

    Option(variant.getFiltersRun) match {
      case Some(o) => {
        // if field set and is false, set unfiltered flag
        if (!o.asInstanceOf[Boolean]) {
          vc.unfiltered
        } else {
          // if filters were run and filters field is pop'ed, we failed a filter
          // if null, we ran and passed
          Option(variant.getFilters) match {
            case Some(o) => {
              // loop and add filters
              stringToList(o.asInstanceOf[String]).foreach (s => {
                vc.filter(s)
              })
            }
            case None => vc.passFilters
          }
        }
      }
      case None => vc.unfiltered
    }

    Option(variant.getAlleleFrequency) match {
      case Some(o) => tagMap += ("AF" -> o.asInstanceOf[java.lang.Double])
    }

    Option(variant.getRmsBaseQuality) match {
      case Some(o) => tagMap += ("BQ" -> o.asInstanceOf[java.lang.Integer])
    }

    Option(variant.getSiteRmsMappingQuality) match {
      case Some(o) => tagMap += ("MQ" -> o.asInstanceOf[java.lang.Integer])
    }

    Option(variant.getSiteMapQZeroCounts) match {
      case Some(o) => tagMap += ("MQ0" -> o.asInstanceOf[java.lang.Integer])
    }

    Option(variant.getTotalSiteMapCounts) match {
      case Some(o) => tagMap += ("DP" -> o.asInstanceOf[java.lang.Integer])
    }

    Option(variant.getNumberOfSamplesWithData) match {
      case Some(o) => tagMap += ("NS" -> o.asInstanceOf[java.lang.Integer])
    }

    vc.attributes(tagMap)

    vc
  }

  /**
   * Converts a GATK variant context into a list of ADAMVariants.
   *
   * @param vc GATK variant context describing VCF data
   * @return A list of ADAMVariant records
   */
  def convertVariants (vc: VariantContext): List[ADAMVariant] = {

    var variants = List[ADAMVariant]()
    var allele = 0

    // get necessary fields from variant context
    // allele frequency in population
    val alleleFrequency = if (vc.hasAttribute("AF")) {
      val afString = vc.getAttributeAsString("AF", "")
      vcfListToDoubles(afString)
    } else {
      List[Double]()
    }

    // variant ID
    val id = if (vc.hasID) {
      Option(vc.getID)
    } else {
      None
    }

    // filters applied to variant, IFF variant failed a filter and was filtered out
    val filters = if (vc.isFiltered) {
      Option(vc.getFilters)
    } else {
      None
    }

    // RMS quality of bases mapped to site
    val baseQuality = if (vc.hasAttribute("BQ")) {
      Some(vc.getAttributeAsInt("BQ", 0))
    } else {
      None
    }

    // RMS mapping quality of reads mapped to site
    val mapQuality = if (vc.hasAttribute("MQ")) {
      Some(vc.getAttributeAsInt("MQ", 0))
    } else {
      None
    }

    // count of reads mapped to site with mapping quality == 0
    val mapQ0Count = if (vc.hasAttribute("MQ0")) {
      Some(vc.getAttributeAsInt("MQ0", 0))
    } else {
      None
    }

    // total number of reads mapped to site
    val totalMapCount = if (vc.hasAttribute("DP")) {
      Some(vc.getAttributeAsInt("DP", 0))
    } else {
      None
    }

    // total number of samples where at least one read mapped to site
    val samplesWithData = if (vc.hasAttribute("NS")) {
      Some(vc.getAttributeAsInt("NS", 0))
    } else {
      None
    }

    // loop over alleles and create variants
    for (a <- vc.getAlleles) {
      
      /**
       * Method to convert a GATK variant context into an ADAM variant type, if the
       * type can be determined from the VariantContext.
       *
       * @param v VariantContext to convert.
       * @return Some(type) if context expresses variant type, else None
       */
      def convertType (v: VariantContext): Option[VariantType] = {
        if (v.isSymbolicOrSV) {
          if (v.isSymbolic) {
            Option(VariantType.Complex)
          } else {
            Option(VariantType.SV)
          }
        } else {
          v.getType match {
            case VariantContext.Type.SNP => Option(VariantType.SNP)
            case VariantContext.Type.MNP => Option(VariantType.MNP)
            case VariantContext.Type.INDEL => {
              if (v.isSimpleDeletion) {
                Option(VariantType.Insertion)
              } else {
                Option(VariantType.Deletion)
              }
            }
            case _ => None
          }
        }
      }

      // start building new variant
      val builder: ADAMVariant.Builder = ADAMVariant.newBuilder
        .setPosition(vc.getStart - 1)
        .setReferenceAllele(vc.getReference.getBaseString)
        .setIsReference(a.isReference)
        .setQuality(vc.getPhredScaledQual.toInt)
        .setAlleleNumber(allele)
        .setFiltersRun(vc.filtersWereApplied)
      
      // get variant type and convert to ADAM variant type
      val variantType = convertType (vc)
      if (!variantType.isEmpty) {
        builder.setVariantType(variantType.get)
      
        // if variant is not complex, get sequence
        if (variantType.get != VariantType.Complex) {
          builder.setVariant(a.getBaseString)
        }
      }

      if (!alleleFrequency.isEmpty) {
        builder.setAlleleFrequency(alleleFrequency(allele))
      }

      if (!id.isEmpty) {
        builder.setId (id.get)
      }

      if (!filters.isEmpty) {
        builder.setFilters(listToString(filters.get.map(i => i : String).toList))
      }

      if (!baseQuality.isEmpty) {
        builder.setRmsBaseQuality(baseQuality.get)
      }

      if (!mapQuality.isEmpty) {
        builder.setSiteRmsMappingQuality(mapQuality.get)
      }

      if (!mapQ0Count.isEmpty) {
        builder.setSiteMapQZeroCounts(mapQ0Count.get)
      }

      if (!totalMapCount.isEmpty) {
        builder.setTotalSiteMapCounts(totalMapCount.get)
      }

      if (!samplesWithData.isEmpty) {
        builder.setNumberOfSamplesWithData(samplesWithData.get)
      }
      
      // increment allele count
      allele += 1
      // prepend to list
      variants = builder.build :: variants
    }

    variants
  }

  /**
   * Converts a list of ADAMGenotypes to a set of genotypes inside of a GATK VariantContext
   * by way of a VariantContextBuilder.
   *
   * @param g List of ADAMGenotypes to convert.
   * @param vc Variant context builder to use during conversion.
   * @return Variant context builder from "conversion".
   */
  def convertGenotypes (g: Seq[ADAMGenotype], vc: VariantContextBuilder): VariantContextBuilder = {

    // group by sample ID and sort by haplotype
    val bySample: Map[java.lang.CharSequence, Seq[ADAMGenotype]] = g.groupBy(_.getSampleId)
      .map(r => (r._1, r._2.sortBy(_.getHaplotypeNumber)))

    // per sample, generate initial copy of builder
    val genotypeBuilders: Map[ADAMGenotype, GenotypeBuilder] = bySample.map(r => {
      (r._2.head, new GenotypeBuilder(r._1, r._2.map(g => Allele.create(g.getAllele, g.getIsReference)).toList))
    })

    // per genotype, populate fields and build genotypes
    val builtGenotypes: List[Genotype] = genotypeBuilders.map(r => {
      val (g, b) = r
      
      if (g.getIsPhased) {
        b.phased(true)
      } else {
        b.phased(false)
      }

      Option(g.getGenotypeQuality) match {
        case Some(o) => b.GQ(o.asInstanceOf[java.lang.Integer])
        case None => b.noGQ
      }

      Option(g.getDepth) match {
        case Some(o) => b.DP(o.asInstanceOf[java.lang.Integer])
        case None => b.noDP
      }

      b.make()
    }).toList
    
    // add genotypes
    vc.genotypes(builtGenotypes)

    vc
  }

  /**
   * Converts a GATK variant context into a set of genotypes. Genotype numbering corresponds
   * to allele numbering of variants at same locus.
   *
   * @param vc GATK variant context to convert.
   * @return List of ADAMGenotypes.
   */
  def convertGenotypes (vc: VariantContext): List[ADAMGenotype] = {
    
    var genotypes = List[ADAMGenotype]()

    // get genotype sample names
    val samples = vc.getSampleNames.toList

    // loop over samples
    for (s <- samples) {
      // get genotypes from variant context
      val g = vc.getGenotype(s)

      // get alleles from genotype
      val a = g.getAlleles.toList

      // initialize haplotype being called to 0
      var haplotype = 0

      val haplotypeQual = if (g.hasExtendedAttribute("HQ")) {
        vcfListToInts(g.getAttributeAsString("HQ", ""))
      } else {
        List[Int]()
      }

      // loop over called alleles in genotype
      for (allele <- a) {
        // start building new genotype
        val builder = ADAMGenotype.newBuilder
          .setPosition(vc.getStart - 1)
          .setSampleId(s)
          .setPloidy(g.getPloidy)
          .setAllele(allele.getBaseString)
          .setHaplotypeNumber(haplotype)
          .setPloidy(allele.length)
          .setIsPhased(g.isPhased)
          .setIsReference(allele.isReference)

        if (g.hasGQ) {
          builder.setGenotypeQuality(g.getGQ)
        }

        if (g.hasDP) {
          builder.setDepth(g.getDP)
        }

        // set phasing specific fields
        if (g.isPhased) {
          if (g.hasExtendedAttribute("PQ")) {
            builder.setPhaseQuality(g.getAttributeAsInt("PQ", 0))
          }
        
          if (g.hasExtendedAttribute("PS")) {
            builder.setPhaseSetId(g.getAttributeAsString("PS", ""))
          }
        }

        if (haplotypeQual.length != 0) {
          builder.setHaplotypeQuality(haplotypeQual(haplotype))
        }

        if (g.hasPL) {
          builder.setPhredLikelihoods(listToString(g.getPL.toList))
        }

        if (g.hasExtendedAttribute("GP")) {
          builder.setPhredPosteriorLikelihoods(g.getAttributeAsString("GP", ""))
        }

        if (g.hasExtendedAttribute("MQ")) {
          builder.setRmsMappingQuality(g.getAttributeAsInt("MQ", 0))
        }

        if (g.hasExtendedAttribute("GQL")) {
          builder.setPloidyStateGenotypeLikelihoods(g.getAttributeAsString("GQL", ""))
        }

        // increment haplotype count
        haplotype += 1

        // finish building genotype and append to list
        genotypes = builder.build :: genotypes
      }
    }

    genotypes
  }

  /**
   * "Converts" ADAMVariantDomain objects into GATK VariantDomain objects by applying these
   * attributes to a VariantDomainBuilder object.
   *
   * @param vd ADAMVariantDomain object to convert.
   * @param vc GATK variant domain builder to add to.
   * @return The "converted" variant domain builder.
   */
  def convertDomains (vd: ADAMVariantDomain, vc: VariantContextBuilder): VariantContextBuilder = {
    if (vd.getInDbSNP) vc.attribute("DB", true)
    if (vd.getInHM2) vc.attribute("H2", true)
    if (vd.getInHM3) vc.attribute("H3", true)
    if (vd.getIn1000G) vc.attribute("1000G", true)

    vc
  }

  /**
   * Converts GATK Variant Context into ADAMVariantDomain objects. ADAMVariantDomains are a
   * variant specific annotation that describes which variant databases a variant site has been
   * seen in.
   *
   * @param vc GATK variant context to convert.
   * @return ADAM variant domain object indiciating which databases contain this variant.
   */
  def convertDomains (vc: VariantContext): ADAMVariantDomain = {
    val builder: ADAMVariantDomain.Builder = ADAMVariantDomain.newBuilder
      .setPosition(vc.getStart - 1)
    
    if (vc.hasAttribute("DB")) {
      builder.setInDbSNP(true)
    }

    if (vc.hasAttribute("H2")) {
      builder.setInHM2(true)
    }

    if (vc.hasAttribute("H3")) {
      builder.setInHM3(true)
    }

    if (vc.hasAttribute("1000G")) {
      builder.setIn1000G(true)
    }

    builder.build()
  }

  /**
   * Converts a single ADAMVariantContext into a GATK variant context. Performs the opposite
   * process of the GATK-->ADAM method.
   *
   * @param vc ADAM variant context to convert.
   * @return GATK Variant context post conversion.
   */
  def convert (vc: ADAMVariantContext): VariantContext = {
    
    val vcb = new VariantContextBuilder()

    // convert variant data
    val vcbWithVariants = convertVariants (vc.variants, vcb)

    // if we have genotypes, convert genotype data
    val vcbWithGenotypes = if (vc.genotypes.length != 0) {
      convertGenotypes (vc.genotypes, vcbWithVariants)
    } else {
      vcbWithVariants
    }

    // if domains are stuffed, convert domain data
    val vcbWithDomains = vc.domains match {
      case Some(o) => convertDomains (o.asInstanceOf[ADAMVariantDomain], vcbWithVariants)
      case None => vcbWithVariants
    }

    // build and return
    vcbWithDomains.make()
  }

  /**
   * Converts a single GATK variant into an ADAMVariantContext. This involves converting:
   *
   * - Alleles seen segregating at site
   * - Genotypes of samples
   * - Variant domain data
   *
   * @param vc GATK Variant context to convert.
   * @return ADAM variant context containing allele, genotype, and domain data.
   */
  def convert (vc: VariantContext): ADAMVariantContext = {
    
    val variants = convertVariants(vc)
    val domains = convertDomains(vc)

    // if GATK variant context contains genotypes, convert them
    // if not, then return empty list of genotypes
    val genotypes = if (vc.hasGenotypes) {
      convertGenotypes(vc)
    } else {
      List[ADAMGenotype]()
    }

    // assemble a variant context from the variants, genotypes, and domains (if seen)
    new ADAMVariantContext(vc.getStart - 1, variants, genotypes, Option(domains))
  }
}
