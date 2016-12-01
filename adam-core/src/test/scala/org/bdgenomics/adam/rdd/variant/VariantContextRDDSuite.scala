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
package org.bdgenomics.adam.rdd.variant

import com.google.common.collect.ImmutableList
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
import scala.collection.JavaConversions._

class VariantContextRDDSuite extends ADAMFunSuite {

  val tempDir = Files.createTempDir()

  def variants: VariantContextRDD = {
    val contig = Contig.newBuilder.setContigName("chr11")
      .setContigLength(249250621L)
      .build
    val v0 = Variant.newBuilder
      .setContigName("chr11")
      .setStart(17409572L)
      .setReferenceAllele("T")
      .setAlternateAllele("C")
      .setNames(ImmutableList.of("rs3131972", "rs201888535"))
      .setFiltersApplied(true)
      .setFiltersPassed(true)
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

    val variant = vcRdd.rdd.first.variant.variant
    assert(variant.getContigName === "chr11")
    assert(variant.getStart === 17409572)
    assert(variant.getReferenceAllele === "T")
    assert(variant.getAlternateAllele === "C")
    assert(variant.getNames.length === 2)
    assert(variant.getNames.get(0) === "rs3131972")
    assert(variant.getNames.get(1) === "rs201888535")
    assert(variant.getFiltersApplied === true)
    assert(variant.getFiltersPassed === true)
    assert(variant.getFiltersFailed.isEmpty)

    assert(vcRdd.sequences.records.size === 1)
    assert(vcRdd.sequences.records(0).name === "chr11")
  }

  sparkTest("can write as a single file, then read in .vcf file") {
    val path = new File(tempDir, "test_single.vcf")
    variants.saveAsVcf(path.getAbsolutePath, asSingleFile = true)
    assert(path.exists)
    println("%s/test_single.vcf".format(tempDir))
    val vcRdd = sc.loadVcf("%s/test_single.vcf".format(tempDir))
    assert(vcRdd.rdd.count === 1)
    assert(vcRdd.sequences.records.size === 1)
    assert(vcRdd.sequences.records(0).name === "chr11")
  }

  sparkTest("don't lose any variants when piping as VCF") {
    val smallVcf = testFile("small.vcf")
    val rdd: VariantContextRDD = sc.loadVcf(smallVcf)
    val records = rdd.rdd.count

    implicit val tFormatter = VCFInFormatter
    implicit val uFormatter = new VCFOutFormatter(rdd.headerLines)

    val pipedRdd: VariantContextRDD = rdd.pipe[VariantContext, VariantContextRDD, VCFInFormatter]("tee /dev/null")
      .transform(_.cache())
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
    assert(pipedRdd.rdd.flatMap(_.genotypes).count === 18)
  }

  sparkTest("save a file sorted by contig index") {
    val inputPath = testFile("random.vcf")
    val variants = sc.loadVcf(inputPath)
    val outputPath = tmpFile("sorted.vcf")

    variants.sort()
      .saveAsVcf(outputPath,
        asSingleFile = true)

    checkFiles(outputPath, testFile("sorted.vcf"))
  }

  sparkTest("save a lexicographically sorted file") {
    val inputPath = testFile("random.vcf")
    val variants = sc.loadVcf(inputPath)
    val outputPath = tmpFile("sorted.lex.vcf")

    variants.sortLexicographically()
      .saveAsVcf(outputPath,
        asSingleFile = true)

    checkFiles(outputPath, testFile("sorted.lex.vcf"))
  }
}
