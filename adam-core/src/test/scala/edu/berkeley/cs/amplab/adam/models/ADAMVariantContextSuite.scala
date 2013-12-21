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
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite

class ADAMVariantContextSuite extends SparkFunSuite {

  sparkTest("Merge 1 variant and 1 genotype.") {
    val variant = ADAMVariant.newBuilder()
      .setPosition(0L)
      .build()
    val genotype = ADAMGenotype.newBuilder()
      .setPosition(0L)
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
      .build()
    val variant1 = ADAMVariant.newBuilder()
      .setPosition(0L)
      .setVariant("C")
      .build()
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setAllele("A")
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setAllele("C")
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
      .build()
    val variant1 = ADAMVariant.newBuilder()
      .setPosition(1L)
      .setVariant("C")
      .build()
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setAllele("A")
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(1L)
      .setAllele("C")
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
      .build()
    val genotype = ADAMGenotype.newBuilder()
      .setPosition(1L)
      .build()

    val variantRDD = sc.parallelize(List(variant))
    val genotypeRDD = sc.parallelize(List(genotype))

    val vc = ADAMVariantContext.mergeVariantsAndGenotypes(variantRDD, genotypeRDD)

    assert(vc.count === 0)
  }

}
