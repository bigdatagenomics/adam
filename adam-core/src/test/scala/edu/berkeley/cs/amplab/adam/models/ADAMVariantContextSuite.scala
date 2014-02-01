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

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.{ADAMVariant, ADAMGenotype}
import edu.berkeley.cs.amplab.adam.models.{ADAMVariantContext, ReferencePosition}
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite

class ADAMVariantContextSuite extends SparkFunSuite {

  sparkTest("Merge 1 variant and 1 genotype.") {
    val variant = ADAMVariant.newBuilder()
      .setPosition(0L)
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()
    val genotype = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()

    val variantRDD = sc.parallelize(List(variant))
    val genotypeRDD = sc.parallelize(List(genotype))

    val vc = ADAMVariantContext.mergeVariantsAndGenotypes(variantRDD, genotypeRDD)

    assert(vc.count === 1)
    assert(vc.first.variants.length === 1)
    assert(vc.first.genotypes.length === 1)
  }

  sparkTest("Merge 2 variants and 2 genotypes at same locus.") {
    val variant0 = ADAMVariant.newBuilder()
      .setPosition(0L)
      .setVariant("A")
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()
    val variant1 = ADAMVariant.newBuilder()
      .setPosition(0L)
      .setVariant("C")
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setAllele("A")
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setAllele("C")
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()

    val variantRDD = sc.parallelize(List(variant0, variant1))
    val genotypeRDD = sc.parallelize(List(genotype0, genotype1))

    val vc = ADAMVariantContext.mergeVariantsAndGenotypes(variantRDD, genotypeRDD)

    assert(vc.count === 1)
    assert(vc.first.variants.length === 2)
    assert(vc.first.genotypes.length === 2)
  }

  sparkTest("Merge 2 variants and 2 genotypes at different locus.") {
    val variant0 = ADAMVariant.newBuilder()
      .setPosition(0L)
      .setVariant("A")
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()
    val variant1 = ADAMVariant.newBuilder()
      .setPosition(1L)
      .setVariant("C")
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setAllele("A")
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(1L)
      .setAllele("C")
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()

    val variantRDD = sc.parallelize(List(variant0, variant1))
    val genotypeRDD = sc.parallelize(List(genotype0, genotype1))

    val vc = ADAMVariantContext.mergeVariantsAndGenotypes(variantRDD, genotypeRDD)

    assert(vc.count === 2)
    assert(vc.filter(_.variants.length == 1).count == 2)
    assert(vc.filter(_.genotypes.length == 1).count == 2)
  }

  sparkTest("Merge 1 variant and 1 genotype at disjoint locations.") {
    val variant = ADAMVariant.newBuilder()
      .setPosition(0L)
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()
    val genotype = ADAMGenotype.newBuilder()
      .setPosition(1L)
      .setReferenceId(0)
      .setReferenceName("chr0")
      .build()

    val variantRDD = sc.parallelize(List(variant))
    val genotypeRDD = sc.parallelize(List(genotype))

    val vc = ADAMVariantContext.mergeVariantsAndGenotypes(variantRDD, genotypeRDD)

    assert(vc.count === 0)
  }

  test("build a variant context from a single genotype") {
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample0")
      .setAllele("A")
      .setReferenceAllele("A")
      .setIsReference(true)
      .setRmsMappingQuality(40)
      .setRmsBaseQuality(30)
      .setDepth(2)
      .setReferenceId(1)
      .setReferenceName("myRef")
      .setGenotypeQuality(50)
      .setReadsMappedMapQ0(0)
      .setReadsMappedForwardStrand(1)
      .build()

    val gtSeq = Seq(genotype0)

    val vc = ADAMVariantContext.buildFromGenotypes(gtSeq)

    assert(vc.position == ReferencePosition(1, 0L))
    assert(vc.variants.length === 1L)
  }

  test("build a variant context from multiple genotypes that represent the same variant") {
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample0")
      .setAllele("A")
      .setReferenceAllele("A")
      .setIsReference(true)
      .setRmsMappingQuality(40)
      .setRmsBaseQuality(30)
      .setDepth(2)
      .setReferenceId(1)
      .setReferenceName("myRef")
      .setGenotypeQuality(50)
      .setReadsMappedMapQ0(0)
      .setReadsMappedForwardStrand(1)
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample1")
      .setAllele("A")
      .setReferenceAllele("A")
      .setIsReference(true)
      .setRmsMappingQuality(40)
      .setRmsBaseQuality(30)
      .setDepth(2)
      .setReferenceId(1)
      .setReferenceName("myRef")
      .setGenotypeQuality(50)
      .setReadsMappedMapQ0(0)
      .setReadsMappedForwardStrand(1)
      .build()

    val gtSeq = Seq(genotype0, genotype1)

    val vc = ADAMVariantContext.buildFromGenotypes(gtSeq)

    assert(vc.position == ReferencePosition(1, 0L))
    assert(vc.variants.length === 1L)
  }

  test("build a variant context from multiple genotypes that represent different variants") {
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample0")
      .setAllele("A")
      .setReferenceAllele("A")
      .setIsReference(true)
      .setRmsMappingQuality(40)
      .setRmsBaseQuality(30)
      .setDepth(2)
      .setReferenceId(1)
      .setReferenceName("myRef")
      .setGenotypeQuality(50)
      .setReadsMappedMapQ0(0)
      .setReadsMappedForwardStrand(1)
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample1")
      .setAllele("T")
      .setReferenceAllele("A")
      .setIsReference(true)
      .setRmsMappingQuality(40)
      .setRmsBaseQuality(30)
      .setDepth(2)
      .setReferenceId(1)
      .setReferenceName("myRef")
      .setGenotypeQuality(50)
      .setReadsMappedMapQ0(0)
      .setReadsMappedForwardStrand(1)
      .build()

    val gtSeq = Seq(genotype0, genotype1)

    val vc = ADAMVariantContext.buildFromGenotypes(gtSeq)

    assert(vc.position == ReferencePosition(1, 0L))
    assert(vc.variants.length === 2L)
  }

  test("cannot build a variant context from variants at different sites") {
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample0")
      .setAllele("A")
      .setReferenceAllele("A")
      .setIsReference(true)
      .setRmsMappingQuality(40)
      .setRmsBaseQuality(30)
      .setDepth(2)
      .setReferenceId(1)
      .setReferenceName("myRef")
      .setGenotypeQuality(50)
      .setReadsMappedMapQ0(0)
      .setReadsMappedForwardStrand(1)
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(1L)
      .setSampleId("mySample1")
      .setAllele("A")
      .setReferenceAllele("A")
      .setIsReference(true)
      .setRmsMappingQuality(40)
      .setRmsBaseQuality(30)
      .setDepth(2)
      .setReferenceId(1)
      .setReferenceName("myRef")
      .setGenotypeQuality(50)
      .setReadsMappedMapQ0(0)
      .setReadsMappedForwardStrand(1)
      .build()

    val gtSeq = Seq(genotype0, genotype1)

    intercept[AssertionError] {
      ADAMVariantContext.buildFromGenotypes(gtSeq)
    }
  }

}
