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

import org.scalatest.FunSuite
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rich.RichGenotype._
import scala.collection.JavaConversions._

class RichGenotypeSuite extends FunSuite {

  def v0 = Variant.newBuilder
    .setContigName("chr1")
    .setStart(0).setReferenceAllele("A").setAlternateAllele("T")
    .build

  test("different ploidy") {
    val gb = Genotype.newBuilder.setVariant(v0).setSampleId("NA12878")
    for (ploidy <- 0 until 3) {
      val g = gb.setAlleles(List.fill(ploidy)(GenotypeAllele.REF)).build
      assert(g.ploidy === ploidy)
    }
  }

  test("all types for diploid genotype") {
    val gb = Genotype.newBuilder.setVariant(v0).setSampleId("NA12878")

    val hom_ref = gb.setAlleles(List(GenotypeAllele.REF, GenotypeAllele.REF)).build
    assert(hom_ref.getType === GenotypeType.HOM_REF)

    val het1 = gb.setAlleles(List(GenotypeAllele.REF, GenotypeAllele.ALT)).build
    assert(het1.getType === GenotypeType.HET)
    val het2 = gb.setAlleles(List(GenotypeAllele.ALT, GenotypeAllele.REF)).build
    assert(het2.getType === GenotypeType.HET)

    val hom_alt = gb.setAlleles(List(GenotypeAllele.ALT, GenotypeAllele.ALT)).build
    assert(hom_alt.getType === GenotypeType.HOM_ALT)

    for (a <- GenotypeAllele.values) {
      val no_call1 = gb.setAlleles(List(GenotypeAllele.NO_CALL, a)).build
      assert(no_call1.getType === GenotypeType.NO_CALL)
      val no_call2 = gb.setAlleles(List(a, GenotypeAllele.NO_CALL)).build
      assert(no_call2.getType === GenotypeType.NO_CALL)
    }
  }
}
