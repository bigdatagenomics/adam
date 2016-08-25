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

import com.google.common.io.Files
import java.io.File
import org.bdgenomics.adam.models.{
  SequenceDictionary,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._

class VariantContextRDDSuite extends ADAMFunSuite {

  val tempDir = Files.createTempDir()

  def variants: VariantContextRDD = {
    val contig = Contig.newBuilder.setContigName("chr11")
      .setContigLength(249250621L)
      .build
    val v0 = Variant.newBuilder
      .setContigName("chr11")
      .setStart(17409572)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .build

    val g0 = Genotype.newBuilder().setVariant(v0)
      .setSampleId("NA12878")
      .setAlleles(List(GenotypeAllele.REF, GenotypeAllele.ALT))
      .build

    VariantContextRDD(sc.parallelize(List(
      VariantContext(v0, Seq(g0))), 1),
      SequenceDictionary.fromAvro(Seq(contig)), Seq(Sample.newBuilder()
        .setSampleId("NA12878")
        .build))
  }

  sparkTest("can write, then read in .vcf file") {
    val path = new File(tempDir, "test.vcf")
    variants.saveAsVcf(TestSaveArgs(path.getAbsolutePath), false)
    assert(path.exists)
    val vcRdd = sc.loadVcf("%s/test.vcf/part-r-00000".format(tempDir))
    assert(vcRdd.rdd.count === 1)
    assert(vcRdd.sequences.records.size === 1)
    assert(vcRdd.sequences.records(0).name === "chr11")
  }

  sparkTest("joins SNV database annotation") {
    val v0 = Variant.newBuilder
      .setContigName("11")
      .setStart(17409572)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .build

    val sd = SequenceDictionary(SequenceRecord("11", 20000000))

    val vc = VariantContextRDD(sc.parallelize(List(
      VariantContext(v0))), sd, Seq.empty)

    val a0 = DatabaseVariantAnnotation.newBuilder
      .setVariant(v0)
      .setDbSnpId(5219)
      .build

    val vda = DatabaseVariantAnnotationRDD(sc.parallelize(List(
      a0)), sd)

    val annotated = vc.joinDatabaseVariantAnnotation(vda).rdd
    assert(annotated.map(_.databases.isDefined).reduce { (a, b) => a && b })
  }
}
