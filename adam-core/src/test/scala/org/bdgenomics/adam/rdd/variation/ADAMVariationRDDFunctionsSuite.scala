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
package org.bdgenomics.adam.rdd.variation

import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.variation.VariationContext._

class ADAMVariationRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("recover samples from variant context") {
    val variant0 = Variant.newBuilder()
      .setStart(0L)
      .setAlternateAllele("A")
      .setReferenceAllele("T")
      .setContig(Contig.newBuilder.setContigName("chr0").build)
      .build()
    val variant1 = Variant.newBuilder()
      .setStart(0L)
      .setAlternateAllele("C")
      .setReferenceAllele("T")
      .setContig(Contig.newBuilder.setContigName("chr0").build)
      .build()
    val genotype0 = Genotype.newBuilder()
      .setVariant(variant0)
      .setSampleId("me")
      .build()
    val genotype1 = Genotype.newBuilder()
      .setVariant(variant1)
      .setSampleId("you")
      .build()

    val vc = VariantContext.buildFromGenotypes(List(genotype0, genotype1))
    val samples = sc.parallelize(List(vc)).getCallsetSamples()

    assert(samples.count(_ == "you") === 1)
    assert(samples.count(_ == "me") === 1)
  }

  sparkTest("joins SNV database annotation") {
    val v0 = Variant.newBuilder
      .setContig(Contig.newBuilder.setContigName("11").build)
      .setStart(17409572)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .build

    val vc: RDD[VariantContext] = sc.parallelize(List(
      VariantContext(v0)))

    val a0 = DatabaseVariantAnnotation.newBuilder
      .setVariant(v0)
      .setDbSnpId(5219)
      .build

    val vda: RDD[DatabaseVariantAnnotation] = sc.parallelize(List(
      a0))

    // TODO: implicit conversion to VariantContextRDD
    val annotated = vc.joinDatabaseVariantAnnotation(vda)
    assert(annotated.map(_.databases.isDefined).reduce { (a, b) => a && b })
  }

}
