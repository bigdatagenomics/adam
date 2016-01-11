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
package org.bdgenomics.adam.rich

import org.bdgenomics.formats.avro.{ GenotypeType, GenotypeAllele, Genotype }
import scala.collection.JavaConversions._

object RichGenotype {
  implicit def genotypeToRichGenotype(g: Genotype) = new RichGenotype(g)
  implicit def richGenotypeToGenotype(g: RichGenotype) = g.genotype
}

class RichGenotype(val genotype: Genotype) {

  def ploidy: Int = genotype.getAlleles.size

  def getType: GenotypeType = {

    // Get the list with the distinct alleles
    val distinctListOfAlleles = genotype.getAlleles.toList.distinct

    // In the case that there is only one distinct allele
    // This should be applicable to any genome ploidy.
    if (distinctListOfAlleles.size == 1) {
      distinctListOfAlleles match {

        // If all alleles are the reference allele, the genotype is Homozygous Reference (HOM_REF)
        case List(GenotypeAllele.Ref)      => GenotypeType.HOM_REF

        // If all alleles are the primary alternative allele, the genotype is Homozygous Alternative (HOM_ALT)
        case List(GenotypeAllele.Alt)      => GenotypeType.HOM_ALT

        // If all alleles are not called, the genotype is not called  (NO_CALL)
        case List(GenotypeAllele.NoCall)   => GenotypeType.NO_CALL

        // If all alleles are OtherAlt.
        // If genotype.getAlleles returns a single OtherAlt, the genotype is Homozygous Alternative (HOM_ALT)
        // If genotype.getAlleles returns a multiple OtherAlt, the genotype is
        // A) The OtherAlt alleles are the same OtherAlt alleles: Homozygous Alternative (HOM_ALT)
        // B) The OtherAlt allales are different OtherAlt alleles: Heterozygous
        // For now return NO_CALL as the genotypes, as was done in the previous getType function
        // See also issue https://github.com/bigdatagenomics/adam/issues/897
        case List(GenotypeAllele.OtherAlt) => GenotypeType.NO_CALL

        // only the four above alleles are possible
        // https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/bdg.avdl#L464
        case _                             => throw new IllegalStateException("Found single distinct allele other than the four possible alleles: Ref, Alt, NoCall and OtherAlt")
      }
    } // In the case that there are multiple distinct alleles
    // This should be applicable to any genome ploidy.
    else {
      // If there is a not called allele in this distinct list of alleles
      // The genotype is NO_CALL
      // IN HTS-JDK this would be GenotypeType.MIXED , this type is not available in BDG / ADAM
      // https://github.com/bigdatagenomics/bdg-formats/blob/master/src/main/resources/avro/bdg.avdl#L483
      // https://github.com/samtools/htsjdk/blob/master/src/java/htsjdk/variant/variantcontext/Genotype.java#L218
      if (distinctListOfAlleles contains GenotypeAllele.NoCall) {
        GenotypeType.NO_CALL
      } // Otherwise the distinct alleles are a combination of 2 or 3 alleles from the list  (GenotypeAllele.Ref, GenotypeAllele.Alt, GenotypeAllele.OtherAlt)
      // Therefore the genotype is Heterozygous HET
      else {
        GenotypeType.HET
      }
    }
  }

  /**
   * True if all observed alleles are the same (regardless of whether they are ref or alt); if any alleles are no-calls, this method will return false.
   */
  def isHom: Boolean = { isHomRef || isHomAlt }

  /**
   * True if all observed alleles are ref; if any alleles are no-calls, this method will return false.
   */
  def isHomRef: Boolean = { getType == GenotypeType.HOM_REF }

  /**
   * True if all observed alleles are alt; if any alleles are no-calls, this method will return false.
   */
  def isHomAlt: Boolean = { getType == GenotypeType.HOM_ALT }

  /**
   * True if we're het (observed alleles differ); if the ploidy is less than 2 or if any alleles are no-calls, this method will return false.
   */
  def isHet: Boolean = { getType == GenotypeType.HET }

  /**
   * True if this genotype is not actually a genotype but a "no call" (e.g. './.' in VCF). Also true for partial no call genotypes (e.g. '0/.' in VCF)
   */
  def isNoCall: Boolean = { getType == GenotypeType.NO_CALL; }

  /**
   * True if the genotype does not contain any alleles that are not called. e.g. './.' or  '0/.'  in VCF
   */
  def isCalled: Boolean = { getType != GenotypeType.NO_CALL }

}
