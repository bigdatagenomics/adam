/*
 * Copyright (c) 2014. Mount Sinai School of Medicine
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

package edu.berkeley.cs.amplab.adam.rich

import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotypeType, ADAMGenotypeAllele, ADAMGenotype}
import scala.collection.JavaConversions._

object RichADAMGenotype {
  implicit def genotypeToRichGenotype(g: ADAMGenotype) = new RichADAMGenotype(g)
  implicit def richGenotypeToGenotype(g: RichADAMGenotype) = g.genotype
}

class RichADAMGenotype(val genotype: ADAMGenotype) {
  def ploidy: Int = genotype.getAlleles.size

  def getType: ADAMGenotypeType = {
    assert(ploidy <= 2, "getType only meaningful for genotypes with ploidy <= 2")
    genotype.getAlleles.toList.distinct match {
      case List(ADAMGenotypeAllele.Ref) => ADAMGenotypeType.HOM_REF
      case List(ADAMGenotypeAllele.Alt) => ADAMGenotypeType.HOM_ALT
      case List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt) |
           List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Ref) => ADAMGenotypeType.HET
      case _ => ADAMGenotypeType.NO_CALL
    }
  }
}
