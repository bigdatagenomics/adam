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
package edu.berkeley.cs.amplab.adam.converters

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.{ADAMVariant, ADAMGenotype, VariantType}
import edu.berkeley.cs.amplab.adam.util._
import edu.berkeley.cs.amplab.adam.projections.ADAMVariantField
import scala.math.{pow, sqrt}

private[adam] class GenotypesToVariantsConverter(validateSamples: Boolean = false,
                                                 failOnValidationError: Boolean = false) extends Serializable {

  /**
   * Validates consistency between genotypes of a single sample at a single loci. Checks to see that per-sample genotype
   * fields are consistent and that no per-chromosome invariants are violated. This method has no return value, rather,
   * if it fails it throws an exception.
   *
   * @param genotypes A list of genotypes from a single sample.
   *
   * @throws IllegalArgumentException Throws an exception if strict validation is enabled and there is inconsistency in a
   *                                  sample's genotype data. Also throws an exception if the data is from multiple samples or multiple loci.
   */
  def validateGenotypes(genotypes: List[ADAMGenotype]) {
    // method invariant: require that sample ID is defined, there is only one sample,
    // and all records are at the same location
    require(genotypes.forall(_.getSampleId != null),
      "Sample is not defined in genotype:\n" + genotypes)
    require(genotypes.map(_.getSampleId).distinct.length == 1,
      "Genotypes are from different samples:\n" + genotypes)
    require(genotypes.map(_.getPosition).distinct.length == 1,
      "Genotypes are at different positions:\n" + genotypes)

    /**
     * Returns a string that allows us to identify a grouping of genotypes.
     *
     * @param genotypes List of genotypes.
     * @return String identifying sample, reference, and position.
     */
    def identify(genotypes: List[ADAMGenotype]): String = {
      genotypes.head.getSampleId + " @ " + genotypes.head.getReferenceId + "," + genotypes.head.getPosition
    }

    // check ploidy
    require(genotypes.map(_.getPloidy).distinct.length == 1,
      "Sample reports inconsistent ploidy: " + identify(genotypes))
    require(genotypes.length == genotypes.head.getPloidy,
      "Expected " + genotypes.head.getPloidy + " chromosomes called, saw " + genotypes.length + ": "
        + identify(genotypes))

    // check phasing
    require(genotypes.forall(_.getIsPhased != null),
      "Phasing is null for some genotypes: " + identify(genotypes))
    require(genotypes.forall(_.getIsPhased == genotypes.head.getIsPhased),
      "Phasing is inconsistent between chromosomes: " + identify(genotypes))

    /* Do not check:
     *
     * - phase switch: Not necessarily uniform for high (>2) ploidy organisms
     * - phase set id: Not necessarily defined, nor uniform for high (>2) ploidy organisms
     * - phase quality: Not necessarily uniform for high (>2) ploidy organisms
     */

    // check that we don't have duplicate haplotype numbers, if defined
    if (genotypes.head.getIsPhased || !genotypes.forall(_.getHaplotypeNumber == null)) {
      require(genotypes.forall(_.getHaplotypeNumber != null),
        "Haplotype number is not defined for all genotypes: " + identify(genotypes))
      require(genotypes.length == genotypes.map(_.getHaplotypeNumber).distinct.length,
        "Haplotype number is defined, but there are duplicate haplotypes: " + identify(genotypes))
    }

    // check that we don't have multiple reference alleles
    require(genotypes.filter(_.getIsReference).distinct.length == 1,
      "Genotype claims multiple reference alleles: " + identify(genotypes))

    // check that other fields are consistent
    require(genotypes.map(_.getDepth).distinct.length == 1,
      "Genotype claims multiple sequencing depths: " + identify(genotypes))
    require(genotypes.map(_.getRmsMappingQuality).distinct.length == 1,
      "Genotype claims multiple mapping qualities: " + identify(genotypes))
    require(genotypes.map(_.getPhredLikelihoods).distinct.length == 1,
      "Genotype claims multiple likelihoods: " + identify(genotypes))
    require(genotypes.map(_.getPhredPosteriorLikelihoods).distinct.length == 1,
      "Genotype claims multiple posteriors: " + identify(genotypes))
    require(genotypes.map(_.getPloidyStateGenotypeLikelihoods).distinct.length == 1,
      "Genotype claims multiple ploidy state likelihoods: " + identify(genotypes))
  }

  /**
   * Computes root mean squared (RMS) values for a series of doubles.
   *
   * @param values A series of doubles.
   * @return The RMS of this series.
   */
  def rms(values: Seq[Double]): Double = {
    if (values.length > 0) {
      sqrt(values.map(pow(_, 2.0)).reduce(_ + _) / values.length.toDouble)
    } else {
      0.0
    }
  }

  /**
   * Computes root mean squared (RMS) values for a series of phred scaled quality scores.
   *
   * @param values A series of phred scores.
   * @return The RMS of this series.
   */
  def rms(values: Seq[Int]): Int = {
    if (values.length > 0) {
      PhredUtils.doubleToPhred(rms(values.map(PhredUtils.phredToDouble(_))))
    } else {
      0
    }
  }

  /**
   * Finds variant quality from genotype qualities. Variant quality is defined as the likelihood
   * that at least 1 variant exists in the set of samples we have seen. This can be rephrased as
   * the likelihood that there are not 0 variants in the set of samples we have seen. We can
   * assume that all of our genotypes are Bernouli with p=genotype quality. Then, this calculation
   * becomes:
   *
   * P(X = 0) = product, g in genotypes -> (1 - Pg)
   * P(X > 0) = 1 - P(X = 0)
   *
   * Where Pg is the per genotype likelihood that the genotype is correct, and X is the number
   * of times we see a variant.
   *
   * @param values An array of non-phred scaled genotype quality scores.
   * @return A non-phred scaled variant likelihood.
   */
  def variantQualityFromGenotypes(values: Seq[Double]): Double = 1.0 - values.reduce(_ * _)

  /**
   * Converts genotypes that share a reference and allele into a variant.
   *
   * @param genotypes List of genotypes to convert.
   * @param key Tuple identifying (reference ID, allele).
   * @param variantMap Map of existing variants from which to take data.
   * @param takeFromExisting A set containing fields that should NOT be computed if they exist in the current variant collection.
   * @return A collection of variants.
   *
   * @throws IllegalArgumentException Throws an exception if strict validation is enabled and there is inconsistency in a
   *                                  sample's genotype data. If strict validation is not enabled, will still throw an exception if there are inconsistencies
   *                                  between samples in key fields.
   * @seealso validateGenotypes
   */
  def convertGenotypes(genotypes: List[ADAMGenotype],
                       key: (Int, String),
                       wrappedVariant: Option[ADAMVariant],
                       takeFromExisting: Set[ADAMVariantField.Value],
                       totalGenotypeLength: Int,
                       totalSampleLength: Int
                        ): ADAMVariant = {

    // critical validation - not allowed to skip
    // check that all genotypes have the same reference name, reference allele, and isReference status
    require(genotypes.map(_.getReferenceName).distinct.length == 1,
      "Reference ID and name are inconsistent:\n" + genotypes)
    require(genotypes.map(_.getReferenceAllele).distinct.length == 1,
      "Reference allele is inconsistent:\n" + genotypes)
    require(genotypes.map(_.getIsReference).distinct.length == 1,
      "Reference allele status is inconsistent:\n" + genotypes)

    // group by samples
    val genotypesBySamples = genotypes.groupBy(_.getSampleId)

    // do we have a variant?
    val haveVariant = wrappedVariant.isEmpty

    /**
     * If validation fails, throw/print depending on failure mode.
     *
     * @param op Operation to validate.
     * @param msg Message to print.
     *
     * @throws IllegalArgumentException Throws if validation is set to fail and op == false.
     */
    def validationRequire(op: Boolean, msg: String) = {
      if (!op) {
        if (failOnValidationError) {
          throw new IllegalArgumentException(msg)
        } else {
          println(msg)
        }
      }
    }

    // run validation
    if (validateSamples) {
      // run validation across all samples
      validationRequire(genotypes.map(_.getAlleleVariantType).distinct.length == 1,
        "Allele variant type is inconsistent across samples:\n" + genotypes)

      // run per-sample validation
      genotypesBySamples.foreach(s => {
        try {
          validateGenotypes(s._2)
        } catch {
          case (e: IllegalArgumentException) => {
            // if we fail, throw again, else print warning
            if (failOnValidationError) {
              throw e
            } else {
              println(e.getMessage)
            }
          }
        }
      })
    }

    // start building variant
    val builder = ADAMVariant.newBuilder

    /**
     * Returns true if we have a variant, and the set of fields to take from the variant contains this field.
     *
     * @param field Field we are looking for.
     * @return True if field should be taken from the existing variant.
     */
    def takeFromVariant(field: ADAMVariantField.Value): Boolean = haveVariant && takeFromExisting.contains(field)

    if (takeFromVariant(ADAMVariantField.referenceId)) {
      builder.setReferenceId(wrappedVariant.get.getReferenceId)
    } else {
      builder.setReferenceId(genotypes.head.getReferenceId)
    }

    if (takeFromVariant(ADAMVariantField.referenceName)) {
      builder.setReferenceName(wrappedVariant.get.getReferenceName)
    } else {
      builder.setReferenceName(genotypes.head.getReferenceName)
    }

    if (takeFromVariant(ADAMVariantField.position)) {
      builder.setPosition(wrappedVariant.get.getPosition)
    } else {
      builder.setPosition(genotypes.head.getPosition)
    }

    if (takeFromVariant(ADAMVariantField.referenceAllele)) {
      builder.setReferenceAllele(wrappedVariant.get.getReferenceAllele)
    } else {
      builder.setReferenceAllele(genotypes.head.getReferenceAllele)
    }

    if (takeFromVariant(ADAMVariantField.isReference)) {
      builder.setIsReference(wrappedVariant.get.getIsReference)
    } else {
      builder.setIsReference(genotypes.head.getIsReference)
    }

    if (takeFromVariant(ADAMVariantField.variant)) {
      builder.setVariant(wrappedVariant.get.getVariant)
    } else {
      builder.setVariant(genotypes.head.getAllele)
    }

    // more extensive decoding as structural variant is a special case for field stuffing
    val variantType = if (takeFromVariant(ADAMVariantField.variantType)) {
      wrappedVariant.get.getVariantType
    } else {
      genotypes.head.getAlleleVariantType
    }
    builder.setVariantType(variantType)

    // if structural variant...
    if (variantType == VariantType.SV) {
      if (takeFromVariant(ADAMVariantField.svType)) {
        builder.setSvType(wrappedVariant.get.getSvType)
      } else {
        builder.setSvType(genotypes.head.getSvType)
      }

      if (takeFromVariant(ADAMVariantField.svLength)) {
        builder.setSvLength(wrappedVariant.get.getSvLength)
      } else {
        builder.setSvLength(genotypes.head.getSvLength)
      }

      if (takeFromVariant(ADAMVariantField.svIsPrecise)) {
        builder.setSvIsPrecise(wrappedVariant.get.getSvIsPrecise)
      } else {
        builder.setSvIsPrecise(genotypes.head.getSvIsPrecise)
      }

      if (takeFromVariant(ADAMVariantField.svIsPrecise)) {
        builder.setSvIsPrecise(wrappedVariant.get.getSvIsPrecise)
      } else {
        builder.setSvIsPrecise(genotypes.head.getSvIsPrecise)
      }

      if (takeFromVariant(ADAMVariantField.svEnd)) {
        builder.setSvEnd(wrappedVariant.get.getSvEnd)
      } else {
        builder.setSvEnd(genotypes.head.getSvEnd)
      }

      if (takeFromVariant(ADAMVariantField.svConfidenceIntervalStartLow)) {
        builder.setSvConfidenceIntervalStartLow(wrappedVariant.get.getSvConfidenceIntervalStartLow)
      } else {
        builder.setSvConfidenceIntervalStartLow(genotypes.head.getSvConfidenceIntervalStartLow)
      }

      if (takeFromVariant(ADAMVariantField.svConfidenceIntervalStartHigh)) {
        builder.setSvConfidenceIntervalStartHigh(wrappedVariant.get.getSvConfidenceIntervalStartHigh)
      } else {
        builder.setSvConfidenceIntervalStartHigh(genotypes.head.getSvConfidenceIntervalStartHigh)
      }

      if (takeFromVariant(ADAMVariantField.svConfidenceIntervalEndLow)) {
        builder.setSvConfidenceIntervalEndLow(wrappedVariant.get.getSvConfidenceIntervalEndLow)
      } else {
        builder.setSvConfidenceIntervalEndLow(genotypes.head.getSvConfidenceIntervalEndLow)
      }

      if (takeFromVariant(ADAMVariantField.svConfidenceIntervalEndHigh)) {
        builder.setSvConfidenceIntervalEndHigh(wrappedVariant.get.getSvConfidenceIntervalEndHigh)
      } else {
        builder.setSvConfidenceIntervalEndHigh(genotypes.head.getSvConfidenceIntervalEndHigh)
      }
    }

    if (takeFromVariant(ADAMVariantField.id)) {
      builder.setId(wrappedVariant.get.getId)
    }

    if (takeFromVariant(ADAMVariantField.quality)) {
      builder.setQuality(wrappedVariant.get.getQuality)
    } else {
      val quals = genotypes.map(_.getGenotypeQuality)
        .filter(_ != null)
        .map(PhredUtils.phredToDouble(_))

      if (quals.length != 0) {
        builder.setQuality(PhredUtils.doubleToPhred(variantQualityFromGenotypes(quals)))
      }
    }

    if (takeFromVariant(ADAMVariantField.filters)) {
      builder.setFilters(wrappedVariant.get.getFilters)
    }

    if (takeFromVariant(ADAMVariantField.filtersRun)) {
      builder.setFiltersRun(wrappedVariant.get.getFiltersRun)
    }

    if (takeFromVariant(ADAMVariantField.alleleFrequency)) {
      builder.setAlleleFrequency(wrappedVariant.get.getAlleleFrequency)
    } else {
      // the frequency that this allele shows up out of all genotypes
      builder.setAlleleFrequency(genotypes.length.toDouble / totalGenotypeLength.toDouble)
    }

    if (takeFromVariant(ADAMVariantField.rmsBaseQuality)) {
      builder.setRmsBaseQuality(wrappedVariant.get.getRmsBaseQuality)
    } else {
      // this is an approximation - given low variance, this will work sufficiently well
      builder.setRmsBaseQuality(rms(genotypes.filter(g => g.getRmsBaseQuality != null && g.getDepth != null)
        .flatMap(g => {
        val l: List[Int] = List[Int]()

        l.padTo(g.getDepth.toInt, g.getRmsBaseQuality)
      }).toSeq.asInstanceOf[Seq[Int]]))
    }

    if (takeFromVariant(ADAMVariantField.siteRmsMappingQuality)) {
      builder.setSiteRmsMappingQuality(wrappedVariant.get.getSiteRmsMappingQuality)
    } else {
      // this is an approximation - given low variance, this will work sufficiently well
      builder.setSiteRmsMappingQuality(rms(genotypes.filter(g => g.getRmsMappingQuality != null && g.getDepth != null)
        .flatMap(g => {
        val l: List[Int] = List[Int]()

        l.padTo(g.getDepth.toInt, g.getRmsMappingQuality)
      }).toSeq.asInstanceOf[Seq[Int]]))
    }

    if (takeFromVariant(ADAMVariantField.siteMapQZeroCounts)) {
      builder.setSiteMapQZeroCounts(wrappedVariant.get.getSiteMapQZeroCounts)
    } else {
      val mq0 = genotypes.map(_.getReadsMappedMapQ0)
        .filter(_ != null)

      if (mq0.length > 0) {
        builder.setSiteMapQZeroCounts(mq0.reduce(_ + _))
      }
    }

    if (takeFromVariant(ADAMVariantField.totalSiteMapCounts)) {
      builder.setTotalSiteMapCounts(wrappedVariant.get.getTotalSiteMapCounts)
    } else {
      val mc = genotypes.map(_.getDepth)
        .filter(_ != null)

      if (mc.length > 0) {
        builder.setTotalSiteMapCounts(mc.reduce(_ + _))
      }
    }

    if (takeFromVariant(ADAMVariantField.numberOfSamplesWithData)) {
      builder.setNumberOfSamplesWithData(wrappedVariant.get.getNumberOfSamplesWithData)
    } else {
      builder.setNumberOfSamplesWithData(totalSampleLength)
    }

    if (takeFromVariant(ADAMVariantField.strandBias)) {
      builder.setStrandBias(wrappedVariant.get.getStrandBias)
    } else {
      val g = genotypes.filter(r => r.getDepth != null && r.getReadsMappedForwardStrand != null)

      if (g.length > 0) {
        val total = g.map(_.getDepth).reduce(_ + _)
        val forward = g.map(_.getReadsMappedForwardStrand).reduce(_ + _)

        // bias is forward / rev, rev = total - forward
        builder.setStrandBias(forward.toDouble / (total - forward).toDouble)
      }
    }

    // build and return
    builder.build()
  }

  /**
   * Calculates variant data from genotype data.
   *
   * @param genotypes A collection of genotypes at a given site that we will calculate variant information from.
   * @param variant Existing collection of variants to take data from.
   * @param takeFromExisting A set containing fields that should NOT be computed if they exist in the current variant collection.
   * @return A collection of variants.
   *
   * @throws IllegalArgumentException Throws an exception if strict validation is enabled and there is inconsistency in a
   *                                  sample's genotype data. If strict validation is not enabled, will still throw an exception if there are inconsistencies
   *                                  between samples in key fields.
   * @seealso validateGenotypes
   */
  def convert(genotypes: Seq[ADAMGenotype],
              variant: Option[Seq[ADAMVariant]] = None,
              takeFromExisting: Set[ADAMVariantField.Value] = Set[ADAMVariantField.Value]()
               ): Seq[ADAMVariant] = {

    // group by allele and reference genome
    val genotypeGroups: Map[(Int, String), Seq[ADAMGenotype]] = genotypes.groupBy(g => {
      val k: (Int, String) = (g.getReferenceId, g.getAllele)
      k
    })
    val variantGroups: Map[(Int, String), Seq[ADAMVariant]] = variant match {
      case Some(o) => o.asInstanceOf[Seq[ADAMVariant]].groupBy(g => {
        val k: (Int, String) = (g.getReferenceId, g.getVariant)
        k
      })
      case None => Map[(Int, String), Seq[ADAMVariant]]()
    }

    // should not have multiply mapped variants
    assert(variantGroups.forall(_._2.length == 1))

    // generate new set of variants
    val variants: Seq[ADAMVariant] = genotypeGroups.map(kv => {
      val (key, genotypes): ((Int, String), Seq[ADAMGenotype]) = kv

      // get variant if it exists
      val variant: Option[ADAMVariant] = if (variantGroups.contains(key)) {
        Option(variantGroups(key).head)
      } else {
        None
      }

      convertGenotypes(genotypes.toList,
        key,
        variant,
        takeFromExisting,
        genotypes.length,
        genotypes.map(_.getSampleId).distinct.length)
    }).toSeq

    variants
  }
}
