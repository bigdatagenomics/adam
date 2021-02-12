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
package org.bdgenomics.adam.ds.variant

import com.google.common.collect.ImmutableList
import com.google.common.io.Files
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.{
  VCFFormatHeaderLine,
  VCFHeaderLine,
  VCFInfoHeaderLine
}
import java.io.File
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset }
import org.apache.spark.util.CollectionAccumulator
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{
  Coverage,
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.TestSaveArgs
import org.bdgenomics.adam.ds.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.ds.fragment.FragmentDataset
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.ds.sequence.SliceDataset
import org.bdgenomics.adam.sql.{
  Alignment => AlignmentProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  Slice => SliceProduct,
  Variant => VariantProduct,
  VariantContext => VariantContextProduct
}
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import scala.collection.JavaConversions._

class VariantContextDatasetSuite extends ADAMFunSuite {

  val tempDir = Files.createTempDir()

  def variants: VariantContextDataset = {
    val reference = Reference.newBuilder
      .setName("chr11")
      .setLength(249250621L)
      .build
    val v0 = Variant.newBuilder
      .setReferenceName("chr11")
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

    VariantContextDataset(sc.parallelize(List(
      VariantContext(v0, Seq(g0))), 1),
      SequenceDictionary.fromAvro(Seq(reference)), Seq(Sample.newBuilder()
        .setId("NA12878")
        .build),
      DefaultHeaderLines.allHeaderLines)
  }

  sparkTest("load a gvcf with a missing info field set to .") {
    val vc1 = sc.loadVcf(testFile("gvcf_multiallelic/multiallelic.vcf"))

    assert(vc1.toVariants.rdd.filter(v => {
      v.getAnnotation.getAttributes().containsKey("MQA")
    }).count === 1)
    assert(vc1.toGenotypes.rdd.filter(gt => {
      gt.getVariantCallingAnnotations().getAttributes().containsKey("MQA")
    }).count === 1)
    assert(vc1.rdd.count === 6)
  }

  sparkTest("union two variant context genomic datasets together") {
    val vc1 = sc.loadVcf(testFile("gvcf_dir/gvcf_multiallelic.g.vcf"))
    val vc2 = sc.loadVcf(testFile("small.vcf"))
    val union = vc1.union(vc2)
    assert(union.rdd.count === (vc1.rdd.count + vc2.rdd.count))
    assert(union.references.size === (vc1.references.size + vc2.references.size))
    assert(union.samples.size === 4)
  }

  sparkTest("can write, then read in .vcf file") {
    val path = new File(tempDir, "test.vcf")
    variants.saveAsVcf(TestSaveArgs(path.getAbsolutePath))
    assert(path.exists)

    val vcRdd = sc.loadVcf("%s/test.vcf/part-r-00000".format(tempDir))
    assert(vcRdd.rdd.count === 1)

    val variant = vcRdd.rdd.first.variant.variant
    assert(variant.getReferenceName === "chr11")
    assert(variant.getStart === 17409572)
    assert(variant.getReferenceAllele === "T")
    assert(variant.getAlternateAllele === "C")
    assert(variant.getNames.length === 2)
    assert(variant.getNames.get(0) === "rs3131972")
    assert(variant.getNames.get(1) === "rs201888535")
    assert(variant.getFiltersApplied === true)
    assert(variant.getFiltersPassed === true)
    assert(variant.getFiltersFailed.isEmpty)

    assert(vcRdd.references.records.size === 1)
    assert(vcRdd.references.records(0).name === "chr11")
  }

  sparkTest("can write as a single file via simple saveAsVcf method, then read in .vcf file") {
    val path = new File(tempDir, "test_single.vcf")
    variants.saveAsVcf(path.getAbsolutePath)
    assert(path.exists)
    val vcRdd = sc.loadVcf("%s/test_single.vcf".format(tempDir))
    assert(vcRdd.rdd.count === 1)
    assert(vcRdd.references.records.size === 1)
    assert(vcRdd.references.records(0).name === "chr11")
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
    assert(vcRdd.references.records.size === 1)
    assert(vcRdd.references.records(0).name === "chr11")
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

    val genotype = vcs.toGenotypes().rdd.filter(_.getStart == 14396L).first()
    assert(genotype.getVariantCallingAnnotations.getRmsMapQ === Float.NegativeInfinity)
    // +Inf FORMAT value --> Infinity after conversion
    assert(genotype.getVariantCallingAnnotations.getAttributes.get("float") === "Infinity")
  }

  sparkTest("support VCFs with `nan` instead of `NaN` float values") {
    val path = testFile("nan_float_values.vcf")
    val vcs = sc.loadVcf(path, ValidationStringency.LENIENT)
    val variant = vcs.toVariants().rdd.filter(_.getStart == 14396L).first()
    assert(variant.getAnnotation.getAlleleFrequency.isNaN)
    assert(variant.getAnnotation.getAttributes.get("BaseQRankSum") === "NaN")
    assert(variant.getAnnotation.getAttributes.get("ClippingRankSum") === "NaN")

    val genotype = vcs.toGenotypes().rdd.filter(_.getStart == 14396L).first()
    assert(genotype.getVariantCallingAnnotations.getRmsMapQ.isNaN)
    assert(genotype.getVariantCallingAnnotations.getAttributes.get("float") === "NaN")
  }

  sparkTest("don't lose any variants when piping as VCF") {
    val smallVcf = testFile("small.vcf")
    val rdd: VariantContextDataset = sc.loadVcf(smallVcf)
    val records = rdd.rdd.count

    implicit val tFormatter = VCFInFormatter
    implicit val uFormatter = new VCFOutFormatter(sc.hadoopConfiguration)

    val pipedRdd: VariantContextDataset = rdd.pipe[VariantContext, VariantContextProduct, VariantContextDataset, VCFInFormatter](Seq("tee", "/dev/null"))
      .cache()
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
    assert(pipedRdd.rdd.flatMap(_.genotypes).count === 18)
  }

  sparkTest("pipe works with empty partitions") {
    val smallVcf = testFile("small.addctg.vcf")
    val rdd: VariantContextDataset = sc.loadVcf(smallVcf)
    val records = rdd.rdd.count

    implicit val tFormatter = VCFInFormatter
    implicit val uFormatter = new VCFOutFormatter(sc.hadoopConfiguration)

    val pipedRdd: VariantContextDataset = rdd.pipe[VariantContext, VariantContextProduct, VariantContextDataset, VCFInFormatter](Seq("tee", "/dev/null"))
      .cache()
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
    assert(pipedRdd.rdd.flatMap(_.genotypes).count === 18)
  }

  sparkTest("don't lose any non-default VCF header lines or attributes when piping as VCF") {
    val freebayesVcf = testFile("NA12878.chr22.tiny.freebayes.vcf")
    val rdd: VariantContextDataset = sc.loadVcf(freebayesVcf)

    val accumulator: CollectionAccumulator[VCFHeaderLine] = sc.collectionAccumulator("headerLines")

    implicit val tFormatter = VCFInFormatter
    implicit val uFormatter = new VCFOutFormatter(sc.hadoopConfiguration, Some(accumulator))

    val pipedRdd: VariantContextDataset = rdd.pipe[VariantContext, VariantContextProduct, VariantContextDataset, VCFInFormatter](Seq("tee", "/dev/null"))

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

    // retrieve accumulated VCF header lines
    val headerLines = accumulator.value.distinct
    val updatedHeaders = pipedRdd.replaceHeaderLines(headerLines)

    // check for freebayes-specific VCF INFO header lines
    assert(updatedHeaders.headerLines.exists(line => line match {
      case info: VCFInfoHeaderLine => info.getID == "NS"
      case _                       => false
    }))

    // check for freebayes-specific VCF FORMAT header lines
    assert(updatedHeaders.headerLines.exists(line => line match {
      case format: VCFFormatHeaderLine => format.getID == "QR"
      case _                           => false
    }))
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
    def testMetadata(vRdd: VariantContextDataset) {
      val sequenceRdd = vRdd.addReference(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.references.containsReferenceName("aSequence"))

      val headerRdd = vRdd.addHeaderLine(new VCFHeaderLine("ABC", "123"))
      assert(headerRdd.headerLines.exists(_.getKey == "ABC"))

      val sampleRdd = vRdd.addSample(Sample.newBuilder
        .setId("aSample")
        .build)
      assert(sampleRdd.samples.exists(_.getId == "aSample"))
    }

    testMetadata(sc.loadVcf(testFile("small.vcf")))
  }

  sparkTest("save sharded bgzip vcf") {
    val smallVcf = testFile("bqsr1.vcf")
    val rdd: VariantContextDataset = sc.loadVcf(smallVcf)
    val outputPath = tmpFile("bqsr1.vcf.bgz")
    rdd.transform((rdd: RDD[VariantContext]) => rdd.repartition(4))
      .saveAsVcf(outputPath,
        asSingleFile = false,
        deferMerging = false,
        disableFastConcat = false,
        stringency = ValidationStringency.STRICT)

    assert(sc.loadVcf(outputPath).rdd.count === 681)
  }

  sparkTest("save bgzip vcf as single file") {
    val smallVcf = testFile("small.vcf")
    val rdd: VariantContextDataset = sc.loadVcf(smallVcf)
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
    val rdd: VariantContextDataset = sc.loadVcf(smallVcf)

    intercept[IllegalArgumentException] {
      rdd.saveAsVcf("small.bcf",
        asSingleFile = false,
        deferMerging = false,
        disableFastConcat = false,
        stringency = ValidationStringency.STRICT)
    }
  }

  sparkTest("transform variant contexts to slice genomic dataset") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(slices: SliceDataset) {
      val tempPath = tmpLocation(".adam")
      slices.saveAsParquet(tempPath)

      assert(sc.loadSlices(tempPath).rdd.count === 6)
    }

    val slices: SliceDataset = variantContexts.transmute[Slice, SliceProduct, SliceDataset](
      (rdd: RDD[VariantContext]) => {
        rdd.map(VariantDatasetSuite.sliceFn)
      })

    checkSave(slices)
  }

  sparkTest("transform variant contexts to coverage genomic dataset") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(coverage: CoverageDataset) {
      val tempPath = tmpLocation(".bed")
      coverage.save(tempPath, false, false)

      assert(sc.loadCoverage(tempPath).rdd.count === 6)
    }

    val coverage: CoverageDataset = variantContexts.transmute[Coverage, Coverage, CoverageDataset](
      (rdd: RDD[VariantContext]) => {
        rdd.map(VariantDatasetSuite.covFn)
      })

    checkSave(coverage)
  }

  sparkTest("transform variant contexts to feature genomic dataset") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(features: FeatureDataset) {
      val tempPath = tmpLocation(".bed")
      features.save(tempPath, false, false)

      assert(sc.loadFeatures(tempPath).rdd.count === 6)
    }

    val features: FeatureDataset = variantContexts.transmute[Feature, FeatureProduct, FeatureDataset](
      (rdd: RDD[VariantContext]) => {
        rdd.map(VariantDatasetSuite.featFn)
      })

    checkSave(features)
  }

  sparkTest("transform variant contexts to fragment genomic dataset") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(fragments: FragmentDataset) {
      val tempPath = tmpLocation(".adam")
      fragments.saveAsParquet(tempPath)

      assert(sc.loadFragments(tempPath).rdd.count === 6)
    }

    val fragments: FragmentDataset = variantContexts.transmute[Fragment, FragmentProduct, FragmentDataset](
      (rdd: RDD[VariantContext]) => {
        rdd.map(VariantDatasetSuite.fragFn)
      })

    checkSave(fragments)
  }

  sparkTest("transform variant contexts to read genomic dataset") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(reads: AlignmentDataset) {
      val tempPath = tmpLocation(".adam")
      reads.saveAsParquet(tempPath)

      assert(sc.loadAlignments(tempPath).rdd.count === 6)
    }

    val reads: AlignmentDataset = variantContexts.transmute[Alignment, AlignmentProduct, AlignmentDataset](
      (rdd: RDD[VariantContext]) => {
        rdd.map(VariantDatasetSuite.readFn)
      })

    checkSave(reads)
  }

  sparkTest("transform variant contexts to genotype genomic dataset") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(genotypes: GenotypeDataset) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 6)
    }

    val genotypes: GenotypeDataset = variantContexts.transmute[Genotype, GenotypeProduct, GenotypeDataset](
      (rdd: RDD[VariantContext]) => {
        rdd.map(VariantDatasetSuite.genFn)
      })

    checkSave(genotypes)
  }

  sparkTest("transform variant contexts to variant genomic dataset") {
    val variantContexts = sc.loadVcf(testFile("small.vcf"))

    def checkSave(variants: VariantDataset) {
      val tempPath = tmpLocation(".adam")
      variants.saveAsParquet(tempPath)

      assert(sc.loadVariants(tempPath).rdd.count === 6)
    }

    val variants: VariantDataset = variantContexts.transmute[Variant, VariantProduct, VariantDataset](
      (rdd: RDD[VariantContext]) => {
        rdd.map(_.variant.variant)
      })

    checkSave(variants)
  }

  sparkTest("save and reload from partitioned parquet") {
    def testMetadata(vcs: VariantContextDataset) {
      assert(vcs.references.containsReferenceName("13"))
      assert(vcs.samples.isEmpty)
      assert(vcs.headerLines.exists(_.getKey == "GATKCommandLine"))
    }

    val variantContexts = sc.loadVcf(testFile("sorted-variants.vcf"))
    val outputPath = tmpLocation()
    variantContexts.saveAsPartitionedParquet(outputPath, partitionSize = 1000000)
    val unfilteredVariantContexts = sc.loadPartitionedParquetVariantContexts(outputPath)
    testMetadata(unfilteredVariantContexts)
    assert(unfilteredVariantContexts.rdd.count === 6)
    assert(unfilteredVariantContexts.dataset.count === 6)

    val regionsVariantContexts = sc.loadPartitionedParquetVariantContexts(outputPath,
      List(ReferenceRegion("2", 19000L, 21000L),
        ReferenceRegion("13", 752700L, 752750L)))
    testMetadata(regionsVariantContexts)
    assert(regionsVariantContexts.rdd.count === 2)
    assert(regionsVariantContexts.dataset.count === 2)
  }

  sparkTest("transform dataset via java API") {
    val variantContexts = sc.loadVcf(testFile("sorted-variants.vcf"))

    val transformed = variantContexts.transformDataset(new JFunction[Dataset[VariantContextProduct], Dataset[VariantContextProduct]]() {
      override def call(ds: Dataset[VariantContextProduct]): Dataset[VariantContextProduct] = {
        ds
      }
    })

    assert(variantContexts.dataset.first().start === transformed.dataset.first().start)
  }
}
