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

import java.io.File

import com.google.common.io.Files
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.variation.VariationContext._
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ Contig, Genotype, GenotypeAllele, Variant }

import scala.collection.JavaConversions._

class ADAMVariationContextSuite extends SparkFunSuite {
  val tempDir = Files.createTempDir()

  def variants: RDD[VariantContext] = {
    val v0 = Variant.newBuilder
      .setContig(Contig.newBuilder.setContigName("chr11").build)
      .setStart(17409572)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .build

    val g0 = Genotype.newBuilder().setVariant(v0)
      .setSampleId("NA12878")
      .setAlleles(List(GenotypeAllele.Ref, GenotypeAllele.Alt))
      .build

    sc.parallelize(List(
      VariantContext(v0, Seq(g0))))
  }

  sparkTest("can read a small .vcf file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.vcf").getFile

    val vcs: RDD[VariantContext] = sc.adamVCFLoad(path)
    assert(vcs.count === 5)

    val vc = vcs.first
    assert(vc.genotypes.size === 3)

    val gt = vc.genotypes.head
    assert(gt.getVariantCallingAnnotations != null)
    assert(gt.getVariantCallingAnnotations.getReadDepth === 69)
    // Recall we are testing parsing, so we assert that our value is
    // the same as should have been parsed
    assert(gt.getVariantCallingAnnotations.getClippingRankSum === java.lang.Float.valueOf("0.138"))
  }

  sparkTest("can write, then read in .vcf file") {
    val path = new File(tempDir, "test.vcf")
    sc.adamVCFSave(path.getAbsolutePath, variants)
    assert(path.exists)
  }
}
