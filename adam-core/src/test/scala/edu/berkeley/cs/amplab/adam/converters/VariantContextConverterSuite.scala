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

import org.broadinstitute.variant.variantcontext.{Allele, VariantContextBuilder, GenotypeBuilder}
import edu.berkeley.cs.amplab.adam.avro.VariantType
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.scalatest.FunSuite

class VariantContextConverterSuite extends FunSuite {

  test("Test variant unpacking from simple variant context with 1 allele") {
    val Aref = Allele.create("A", true)

    // make variant at locus position 1 with allele "A" which is reference, and passes filters
    val vc = new VariantContextBuilder().alleles(List(Aref))
      .id("MyID")
      .passFilters()
      .start(1L)
      .stop(1L)
      .chr("A")
      .make()

    val converter = new VariantContextConverter

    val adamVariants = converter.convertVariants(vc)
    val variant = adamVariants.head

    assert(adamVariants.length === 1)
    assert(variant.getReferenceAllele === "A")
    assert(variant.getIsReference)
    assert(variant.getId === "MyID")
    assert(variant.getFiltersRun)
    assert(variant.getFilters === null)
    assert(variant.getPosition === 0L)
  }

  test("Test variant unpacking from context with reference allele and SNP") {
    val Aref = Allele.create("A", true)
    val Tsnp = Allele.create("T", false)

    // make variant at locus position 1 with allele "A" which is reference
    val vc = new VariantContextBuilder().alleles(List(Aref, Tsnp))
      .id("MyID")
      .passFilters()
      .start(1L)
      .stop(1L)
      .chr("A")
      .make()

    val converter = new VariantContextConverter

    val adamVariants = converter.convertVariants(vc)
    val ref = adamVariants(1)
    val variant = adamVariants(0)

    assert(adamVariants.length === 2)
    assert(ref.getReferenceAllele === "A")
    assert(ref.getIsReference)
    assert(ref.getId === "MyID")
    assert(ref.getFiltersRun)
    assert(ref.getFilters === null)
    assert(ref.getPosition === 0L)
    assert(ref.getVariantType === VariantType.SNP)
    assert(variant.getReferenceAllele === "A")
    assert(!variant.getIsReference)
    assert(variant.getVariant === "T")
    assert(variant.getId === "MyID")
    assert(variant.getFiltersRun)
    assert(variant.getFilters === null)
    assert(variant.getPosition === 0L)
    assert(variant.getVariantType === VariantType.SNP)
  }

  test("Test variant unpacking from simple variant context with 1 allele and 1 genotype") {
    val Aref = Allele.create("A", true)
    val g = GenotypeBuilder.create("mySample", List(Aref))

    // make variant at locus position 1 with allele "A" which is reference, and passes filters
    val vc = new VariantContextBuilder().alleles(List(Aref))
      .id("MyID")
      .passFilters()
      .start(1L)
      .stop(1L)
      .chr("A")
      .genotypes(List(g))
      .make()

    val converter = new VariantContextConverter

    val adamVariants = converter.convertVariants(vc)
    val variant = adamVariants.head

    assert(adamVariants.length === 1)
    assert(variant.getReferenceAllele === "A")
    assert(variant.getIsReference)
    assert(variant.getId === "MyID")
    assert(variant.getFiltersRun)
    assert(variant.getFilters === null)
    assert(variant.getPosition === 0L)

    val adamGenotypes = converter.convertGenotypes(vc)
    val genotype = adamGenotypes.head

    assert(adamGenotypes.length === 1)
    assert(genotype.getIsReference)
    assert(genotype.getAllele === "A")
    assert(genotype.getPosition === 0L)
  }

  test("Test variant unpacking from simple variant context with 1 allele and 1 diploid genotype") {
    val Aref = Allele.create("A", true)
    val g = GenotypeBuilder.create("mySample", List(Aref, Aref))

    // make variant at locus position 1 with allele "A" which is reference, and passes filters
    val vc = new VariantContextBuilder().alleles(List(Aref))
      .id("MyID")
      .passFilters()
      .start(1L)
      .stop(1L)
      .chr("A")
      .genotypes(List(g))
      .make()

    val converter = new VariantContextConverter

    val adamVariants = converter.convertVariants(vc)
    val variant = adamVariants.head

    assert(adamVariants.length === 1)
    assert(variant.getReferenceAllele === "A")
    assert(variant.getIsReference)
    assert(variant.getId === "MyID")
    assert(variant.getFiltersRun)
    assert(variant.getFilters === null)
    assert(variant.getPosition === 0L)

    val adamGenotypes = converter.convertGenotypes(vc)

    assert(adamGenotypes.length === 2)
    assert(adamGenotypes.forall(_.getIsReference))
    assert(adamGenotypes.forall(_.getAllele == "A"))
    assert(adamGenotypes.forall(_.getPosition == 0L))
  }

  test("Test VCF->ADAM->VCF conversion with 1 allele and 1 genotype") {
    val Aref = Allele.create("A", true)
    val g = GenotypeBuilder.create("mySample", List(Aref))

    // make variant at locus position 1 with allele "A" which is reference, and passes filters
    val vc = new VariantContextBuilder().alleles(List(Aref))
      .id("MyID")
      .passFilters()
      .start(1L)
      .stop(1L)
      .chr("A")
      .genotypes(List(g))
      .make()

    val converter = new VariantContextConverter

    val adamVC = converter.convert(vc)

    assert(adamVC.position === 0L)
    assert(adamVC.variants.length === 1)
    assert(adamVC.genotypes.length === 1)

    val vcfVC = converter.convert(adamVC)

    assert(vcfVC.hasSameAllelesAs(vc))
    assert(vcfVC.getStart == vc.getStart)
    assert(vcfVC.getEnd == vc.getEnd)
  }

}
