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
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util.Args4j
import edu.berkeley.cs.amplab.adam.projections.ADAMVariantField
import org.scalatest.FunSuite

class GenotypesToVariantsConverterSuite extends FunSuite {

  test("Simple test of integer RMS") {
    val v = new GenotypesToVariantsConverter
    assert(v.rms(List(1, -1)) === 1)
  }

  test("Simple test of floating point RMS") {
    val v = new GenotypesToVariantsConverter
    val rmsVal = v.rms(List(39.0, -40.0, 41.0))
    
    // floating point, so apply tolerances
    assert(rmsVal > 40.0 && rmsVal < 40.01)
  }

  test("Max genotype quality should lead to max variant quality") {
    val v = new GenotypesToVariantsConverter
    // if p = 1, then the rest of our samples don't matter
    val vq = v.variantQualityFromGenotypes(List(1.0, 0.0))

    // floating point, so apply tolerances
    assert(vq > 0.999 && vq < 1.001)
  }

  test("Genotype quality = 0.5 for two samples should lead to variant quality of 0.75") {
    val v = new GenotypesToVariantsConverter
    val vq = v.variantQualityFromGenotypes(List(0.5, 0.5))

    // floating point, so apply tolerances
    assert(vq > 0.745 && vq < 0.755)
  }

  test("Does validation work? Genotypes from different positions.") {
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample")
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(1L)
      .setSampleId("mySample")
      .build()

    val v = new GenotypesToVariantsConverter

    intercept[IllegalArgumentException] {
      v.validateGenotypes(List(genotype0, genotype1))
    }
  }

  test("Does validation work? Genotypes from different samples.") {
    val genotype0 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample0")
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setPosition(0L)
      .setSampleId("mySample1")
      .build()

    val v = new GenotypesToVariantsConverter

    intercept[IllegalArgumentException] {
      v.validateGenotypes(List(genotype0, genotype1))
    }
  }

  test("Basic genotype->variant conversion test.") {
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
      .setRmsMappingQuality(30)
      .setRmsBaseQuality(30)
      .setDepth(1)
      .setReferenceId(1)
      .setGenotypeQuality(50)
      .setReadsMappedMapQ0(0)
      .setReadsMappedForwardStrand(0)
      .setReferenceName("myRef")
      .build()

    val v = new GenotypesToVariantsConverter
    
    val va = v.convertGenotypes(List(genotype0, genotype1),
                                 (1, "A"),
                                 None,
                                 Set[ADAMVariantField.Value](),
                                 3,
                                 3)

    assert(va.getPosition === 0L)
    assert(va.getVariant === "A")
    assert(va.getIsReference)
    assert(va.getTotalSiteMapCounts === 3)
    assert(va.getNumberOfSamplesWithData === 3)
    assert(va.getSiteMapQZeroCounts === 0)

    // floating point, so apply tolerance
    assert(va.getAlleleFrequency > 0.65 && va.getAlleleFrequency < 0.67)
    val rmsMap = v.rms(List(30, 40, 40))
    assert(va.getSiteRmsMappingQuality > 0.9 * rmsMap && va.getSiteRmsMappingQuality < 1.1 * rmsMap)
    val rmsBase = v.rms(List(30, 30, 30))
    assert(va.getRmsBaseQuality > 0.9 * rmsBase && va.getRmsBaseQuality < 1.1 * rmsBase)
    assert(va.getStrandBias > 0.49 && va.getStrandBias < 0.51)
  }

}
