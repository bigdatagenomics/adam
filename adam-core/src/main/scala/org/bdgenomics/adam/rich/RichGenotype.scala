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
    assert(ploidy <= 2, "getType only meaningful for genotypes with ploidy <= 2")
    genotype.getAlleles.toList.distinct match {
      case List(GenotypeAllele.Ref) => GenotypeType.HOM_REF
      case List(GenotypeAllele.Alt) => GenotypeType.HOM_ALT
      case List(GenotypeAllele.Ref, GenotypeAllele.Alt) |
        List(GenotypeAllele.Alt, GenotypeAllele.Ref) => GenotypeType.HET
      case _ => GenotypeType.NO_CALL
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
