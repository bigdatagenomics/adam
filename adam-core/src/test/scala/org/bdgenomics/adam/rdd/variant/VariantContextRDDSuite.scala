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
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeaderLine
import java.io.File
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{
  Coverage,
  SequenceDictionary,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.{ CoverageRDD, FeatureRDD }
import org.bdgenomics.adam.rdd.fragment.FragmentRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
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
      .setEnd(17409573L)
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
        .build),
      DefaultHeaderLines.allHeaderLines)
  }

  sparkTest("union two variant context rdds together") {
    val vc1 = sc.loadVcf(testFile("gvcf_dir/gvcf_multiallelic.g.vcf"))
    val vc2 = sc.loadVcf(testFile("small.vcf"))
    val union = vc1.union(vc2)
    assert(union.rdd.count === (vc1.rdd.count + vc2.rdd.count))
    assert(union.sequences.size === (vc1.sequences.size + vc2.sequences.size))
    assert(union.samples.size === 4)
  }

  sparkTest("can write, then read in .vcf file") {
    val path = new File(tempDir, "test.vcf")
    variants.saveAsVcf(TestSaveArgs(path.getAbsolutePath))
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

  sparkTest("can write as a single file via simple saveAsVcf method, then read in .vcf file") {
    val path = new File(tempDir, "test_single.vcf")
    variants.saveAsVcf(path.getAbsolutePath)
    assert(path.exists)
    val vcRdd = sc.loadVcf("%s/test_single.vcf".format(tempDir))
    assert(vcRdd.rdd.count === 1)
    assert(vcRdd.sequences.records.size === 1)
    assert(vcRdd.sequences.records(0).name === "chr11")
  }

  sparkTest("can write as a single file via full saveAsVcf method, then read in .vcf file") {
    val path = new File(tempDir, "test_single.vcf")
    variants.saveAsVcf(path.getAbsolutePath,
      asSingleFile = true,
      deferMerging = false,
      disableFastConcat = false,
      ValidationStringency.LENIENT)
    assert(path.exists)
    val vcRdd = sc.loadVcf("%s/test_single.vcf".format(tempDir))
    assert(vcRdd.rdd.count === 1)
    assert(vcRdd.sequences.records.size === 1)
    assert(vcRdd.sequences.records(0).name === "chr11")
  }

  sparkTest("transform a vcf file with bad header") {
    val path = testFile("invalid/truth_small_variants.vcf")
    val before = sc.loadVcf(path, ValidationStringency.SILENT)
    assert(before.rdd.count === 7)
    assert(before.toGenotypes().rdd.filter(_.getPhaseSetId == null).count === 7)

    val tempPath = tmpLocation(".adam")
    before.toVariants().saveAsParquet(tempPath)

    val after = sc.loadVariants(tempPath).toVariantContexts()
    assert(after.rdd.count === 7)
  }

  sparkTest("read a vcf file with multi-allelic variants to split") {
    val path = testFile("HG001_GRCh38_GIAB_highconf_CG-IllFB-IllGATKHC-Ion-10X-SOLID_CHROM1-X_v.3.3.2_all.fixed-phase-set.excerpt.vcf")
    val vcs = sc.loadVcf(path, ValidationStringency.SILENT)
    assert(vcs.rdd.count === 17)

    // AD should be zero or null after splitting ref=GAAGAAAGAAAGA alt=GAAGAAAGA,GAAGA,G AD 0,0,0
    val filtered = vcs.toGenotypes().rdd.filter(_.start == 66631043)
    val referenceReadDepths = filtered.map(_.getAlternateReadDepth).collect()
    val alternateReadDepths = filtered.map(_.getAlternateReadDepth).collect()

    assert(referenceReadDepths.forall(rd => (rd == 0 || rd == null)))
    assert(alternateReadDepths.forall(rd => (rd == 0 || rd == null)))

    // ADALL should be zeros or null after splitting ref=GAAGAAAGAAAGA alt=GAAGAAAGA,GAAGA,G ADALL 0,0,0
    val netAlleleDepths = filtered.map(_.getVariantCallingAnnotations.getAttributes.get("ADALL")).collect()

    assert(netAlleleDepths.forall(adall => (adall == "0,0" || adall == "")))
  }

  sparkTest("support VCFs with +Inf/-Inf float values") {
    val path = testFile("inf_float_values.vcf")
    val vcs = sc.loadVcf(path, ValidationStringency.LENIENT)
    val variant = vcs.toVariants().rdd.filter(_.getStart == 14396L).first()
    assert(variant.getAnnotation.getAlleleFrequency === Float.PositiveInfinity)
    // -Inf INFO value --> -Infinity after conversion
    assert(variant.getAnnotation.getAttributes.get("BaseQRankSum") === "-Infinity")

    val genotype = vcs.toGenotypes().rdd.filter(_.getVariant == variant).first()
    assert(genotype.getVariantCallingAnnotations.getRmsMapQ === Float.NegativeInfinity)
    // +Inf FORMAT value --> Infinity after conversion
    assert(genotype.getVariantCallingAnnotations.getAttributes.get("float") === "Infinity")
  }

  sparkTest("don't lose any variants when piping as VCF") {
    val smallVcf = testFile("small.vcf")
    val rdd: VariantContextRDD = sc.loadVcf(smallVcf)
    val records = rdd.rdd.count

    implicit val tFormatter = VCFInFormatter
    implicit val uFormatter = new VCFOutFormatter

    val pipedRdd: VariantContextRDD = rdd.pipe[VariantContext, VariantContextRDD, VCFInFormatter]("tee /dev/null")
      .transform(_.cache())
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
    assert(pipedRdd.rdd.flatMap(_.genotypes).count === 18)
  }

  sparkTest("don't lose any non-default VCF header lines or attributes when piping as VCF") {
    val freebayesVcf = testFile("NA12878.chr22.tiny.freebayes.vcf")
    val rdd: VariantContextRDD = sc.loadVcf(freebayesVcf)

    implicit val tFormatter = VCFInFormatter
    implicit val uFormatter = new VCFOutFormatter

    val pipedRdd: VariantContextRDD = rdd.pipe[VariantContext, VariantContextRDD, VCFInFormatter]("tee /dev/null")

    // check for freebayes-specific VCF INFO keys
    val variant = pipedRdd.toVariants.rdd.first
    for (freebayesInfoKey <- Seq("NS", "DPB", "RO", "AO", "PRO", "PAO", "QR", "QA")) {
      assert(variant.getAnnotation.getAttributes.containsKey(freebayesInfoKey))
    }

    // check for freebayes-specific VCF FORMAT keys
    val genotype = pipedRdd.toGenotypes.rdd.first
    for (freebayesFormatKey <- Seq("RO", "QR", "AO", "QA")) {
      assert(genotype.getVariantCallingAnnotations.getAttributes.containsKey(freebayesFormatKey))
    }
  }

  sparkTest("save a file sorted by contig index") {
    val inputPath = testFile("random.vcf")
    val variants = sc.loadVcf(inputPath)
    val outputPath = tmpFile("sorted.vcf")

    variants.sort()
      .saveAsVcf(outputPath,
        asSingleFile = true,
        deferMerging = false,
        disableFastConcat = false,
        ValidationStringency.LENIENT)

    checkFiles(outputPath, testFile("sorted.vcf"))
  }

  sparkTest("save a lexicographically sorted file") {
    val inputPath = testFile("random.vcf")
    val variants = sc.loadVcf(inputPath)
    val outputPath = tmpFile("sorted.lex.vcf")

    variants.sortLexicographically()
      .saveAsVcf(outputPath,
        asSingleFile = true,
        deferMerging = false,
        disableFastConcat = false,
        ValidationStringency.LENIENT)

    checkFiles(outputPath, testFile("sorted.lex.vcf"))
  }

  ignore("save a multiallelic gvcf") {

    // this test relies on sort order to pass
    // however, the sort order is not canonical
    // specifically, we don't define how to canonically sort multiallelic sites

    val inputPath = testFile("gvcf_dir/gvcf_multiallelic.g.vcf")
    val variants = sc.loadVcf(inputPath)
    val outputPath = tmpFile("multiallelic.vcf")

    variants.sort()
      .saveAsVcf(outputPath,
        asSingleFile = true,
        deferMerging = false,
        disableFastConcat = false,
        ValidationStringency.LENIENT)

    checkFiles(outputPath, testFile("gvcf_multiallelic/multiallelic.vcf"))
  }

  sparkTest("test metadata") {
    def testMetadata(vRdd: VariantContextRDD) {
      val sequenceRdd = vRdd.addSequence(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.sequences.containsRefName("aSequence"))

      val headerRdd = vRdd.addHeaderLine(new VCFHeaderLine("ABC", "123"))
      assert(headerRdd.headerLines.exists(_.getKey == "ABC"))

      val sampleRdd = vRdd.addSample(Sample.newBuilder
        .setSampleId("aSample")
        .build)
      assert(sampleRdd.samples.exists(_.getSampleId == "aSample"))
    }

    testMetadata(sc.loadVcf(testFile("small.vcf")))
  }

  sparkTest("save sharded bgzip vcf") {
    val smallVcf = testFile("bqsr1.vcf")
    val rdd: VariantContextRDD = sc.loadVcf(smallVcf)
    val outputPath = tmpFile("bqsr1.vcf.bgz")
    rdd.transform(_.repartition(4)).saveAsVcf(outputPath,
      asSingleFile = false,
      deferMerging = false,
      disableFastConcat = false,
      stringency = ValidationStringency.STRICT)

    assert(sc.loadVcf(outputPath).rdd.count === 681)
  }

  sparkTest("save bgzip vcf as single file") {
    val smallVcf = testFile("small.vcf")
    val rdd: VariantContextRDD = sc.loadVcf(smallVcf)
    val outputPath = tmpFile("small.vcf.bgz")
    rdd.saveAsVcf(outputPath,
      asSingleFile = true,
      deferMerging = false,
      disableFastConcat = false,
      stringency = ValidationStringency.STRICT)

    assert(sc.loadVcf(outputPath).rdd.count === 6)
  }

  sparkTest("can't save file with non-vcf extension") {
    val smallVcf = testFile("small.vcf")
    val rdd: VariantContextRDD = sc.loadVcf(smallVcf)

    intercept[IllegalArgumentException] {
      rdd.saveAsVcf("small.bcf",
        asSingleFile = false,
        deferMerging = false,
        disableFastConcat = false,
        stringency = ValidationStringency.STRICT)
    }
  }

  sparkTest("transform variant contexts to contig rdd") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(contigs: NucleotideContigFragmentRDD) {
      val tempPath = tmpLocation(".adam")
      contigs.saveAsParquet(tempPath)

      assert(sc.loadContigFragments(tempPath).rdd.count === 6)
    }

    val contigs: NucleotideContigFragmentRDD = variantContexts.transmute(rdd => {
      rdd.map(VariantRDDSuite.ncfFn)
    })

    checkSave(contigs)
  }

  sparkTest("transform variant contexts to coverage rdd") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(coverage: CoverageRDD) {
      val tempPath = tmpLocation(".bed")
      coverage.save(tempPath, false, false)

      assert(sc.loadCoverage(tempPath).rdd.count === 6)
    }

    val coverage: CoverageRDD = variantContexts.transmute(rdd => {
      rdd.map(VariantRDDSuite.covFn)
    })

    checkSave(coverage)
  }

  sparkTest("transform variant contexts to feature rdd") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(features: FeatureRDD) {
      val tempPath = tmpLocation(".bed")
      features.save(tempPath, false, false)

      assert(sc.loadFeatures(tempPath).rdd.count === 6)
    }

    val features: FeatureRDD = variantContexts.transmute(rdd => {
      rdd.map(VariantRDDSuite.featFn)
    })

    checkSave(features)
  }

  sparkTest("transform variant contexts to fragment rdd") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(fragments: FragmentRDD) {
      val tempPath = tmpLocation(".adam")
      fragments.saveAsParquet(tempPath)

      assert(sc.loadFragments(tempPath).rdd.count === 6)
    }

    val fragments: FragmentRDD = variantContexts.transmute(rdd => {
      rdd.map(VariantRDDSuite.fragFn)
    })

    checkSave(fragments)
  }

  sparkTest("transform variant contexts to read rdd") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(reads: AlignmentRecordRDD) {
      val tempPath = tmpLocation(".adam")
      reads.saveAsParquet(tempPath)

      assert(sc.loadAlignments(tempPath).rdd.count === 6)
    }

    val reads: AlignmentRecordRDD = variantContexts.transmute(rdd => {
      rdd.map(VariantRDDSuite.readFn)
    })

    checkSave(reads)
  }

  sparkTest("transform variant contexts to genotype rdd") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(genotypes: GenotypeRDD) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 6)
    }

    val genotypes: GenotypeRDD = variantContexts.transmute(rdd => {
      rdd.map(VariantRDDSuite.genFn)
    })

    checkSave(genotypes)
  }

  sparkTest("transform variant contexts to variant rdd") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(variants: VariantRDD) {
      val tempPath = tmpLocation(".adam")
      variants.saveAsParquet(tempPath)

      assert(sc.loadVariants(tempPath).rdd.count === 6)
    }

    val variants: VariantRDD = variantContexts.transmute(rdd => {
      rdd.map(_.variant.variant)
    })

    checkSave(variants)
  }
}
