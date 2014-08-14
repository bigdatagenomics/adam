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
package org.bdgenomics.adam.rdd

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ VariantContext, ReferenceRegion }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro._

class ADAMRDDFunctionsSuite extends SparkFunSuite {

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
    val samples = sc.parallelize(List(vc)).adamGetCallsetSamples()

    assert(samples.count(_ == "you") === 1)
    assert(samples.count(_ == "me") === 1)
  }
}
