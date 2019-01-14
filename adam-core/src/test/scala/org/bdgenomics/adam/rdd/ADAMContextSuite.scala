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

import htsjdk.samtools.{
  SAMFormatException,
  SAMProgramRecord,
  ValidationStringency
}
import java.io.{ File, FileNotFoundException }
import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.PhredUtils._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import org.seqdoop.hadoop_bam.CRAMInputFormat
import org.seqdoop.hadoop_bam.util.SAMHeaderReader

case class TestSaveArgs(var outputPath: String) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
  var asSingleFile = false
  var deferMerging = false
  var blockSize = 128 * 1024 * 1024
  var pageSize = 1 * 1024 * 1024
  var compressionCodec = CompressionCodecName.GZIP
  var logLevel = "SEVERE"
  var disableDictionaryEncoding = false
  var disableFastConcat = false
}

class ADAMContextSuite extends ADAMFunSuite {

  sparkTest("ctr is accessible") {
    new ADAMContext(sc)
  }

  sparkTest("load from an empty directory") {
    val emptyDirectory = java.nio.file.Files.createTempDirectory("").toAbsolutePath.toString

    val e = intercept[FileNotFoundException] {
      sc.loadAlignments(emptyDirectory)
    }
    assert(e.getMessage.contains("directory is empty"))
  }

  sparkTest("sc.loadParquet should not fail on unmapped reads") {
    val readsFilepath = testFile("unmapped.sam")

    // Convert the reads12.sam file into a parquet file
    val bamReads = sc.loadAlignments(readsFilepath)
    assert(bamReads.rdd.count === 200)
  }

  sparkTest("sc.loadParquet should not load a file without a type specified") {
    //load an existing file from the resources and save it as an ADAM file.
    //This way we are not dependent on the ADAM format (as we would if we used a pre-made ADAM file)
    //but we are dependent on the unmapped.sam file existing, maybe I should make a new one
    val readsFilepath = testFile("unmapped.sam")
    val bamReads = sc.loadAlignments(readsFilepath)
    //save it as an Adam file so we can test the Adam loader
    val bamReadsAdamFile = new File(Files.createTempDir(), "bamReads.adam")
    bamReads.saveAsParquet(bamReadsAdamFile.getAbsolutePath)
    intercept[IllegalArgumentException] {
      val noReturnType = sc.loadParquet(bamReadsAdamFile.getAbsolutePath)
    }
    //finally just make sure we did not break anything,we came might as well
    val returnType: RDD[AlignmentRecord] = sc.loadParquet(bamReadsAdamFile.getAbsolutePath)
    assert(manifest[returnType.type] != manifest[RDD[Nothing]])
  }

  sparkTest("can read a small .SAM file") {
    val path = testFile("small.sam")
    val reads = sc.loadAlignments(path)
    assert(reads.rdd.count() === 20)
    assert(reads.dataset.count === 20)
    assert(reads.dataset.rdd.count === 20)
  }

  sparkTest("loading a sam file with a bad header and strict stringency should fail") {
    val path = testFile("badheader.sam")
    intercept[SAMFormatException] {
      sc.loadBam(path, stringency = ValidationStringency.STRICT)
    }
  }

  sparkTest("can read a small .CRAM file") {
    val path = testFile("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    sc.hadoopConfiguration.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY,
      referencePath)
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path).rdd
    assert(reads.count() === 10)
  }

  sparkTest("can read a small .SAM with all attribute tag types") {
    val path = testFile("tags.sam")
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path).rdd
    assert(reads.count() === 7)
  }

  sparkTest("can filter a .SAM file based on quality") {
    val path = testFile("small.sam")
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path)
      .rdd
      .filter(a => (a.getReadMapped && a.getMappingQuality > 30))
    assert(reads.count() === 18)
  }

  test("Can convert to phred") {
    assert(successProbabilityToPhred(0.9) === 10)
    assert(successProbabilityToPhred(0.99999) === 50)
  }

  test("Can convert from phred") {
    // result is floating point, so apply tolerance
    assert(phredToSuccessProbability(10) > 0.89 && phredToSuccessProbability(10) < 0.91)
    assert(phredToSuccessProbability(50) > 0.99998 && phredToSuccessProbability(50) < 0.999999)
  }

  sparkTest("Can read a .gtf file") {
    val path = testFile("Homo_sapiens.GRCh37.75.trun20.gtf")
    val features = sc.loadFeatures(path)
    assert(features.rdd.count === 15)
    assert(features.dataset.count === 15)
    assert(features.dataset.rdd.count === 15)
  }

  sparkTest("Can read a .bed file") {
    // note: this .bed doesn't actually conform to the UCSC BED spec...sigh...
    val path = testFile("gencode.v7.annotation.trunc10.bed")
    val features: RDD[Feature] = sc.loadFeatures(path).rdd
    assert(features.count === 10)
  }

  sparkTest("Can read a BED 12 file") {
    // note: this .bed doesn't actually conform to the UCSC BED spec...sigh...
    val path = testFile("small.1_12.bed")
    val features: RDD[Feature] = sc.loadFeatures(path).rdd
    assert(features.count === 4)
  }

  sparkTest("Can read a .narrowPeak file") {
    val path = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val annot: RDD[Feature] = sc.loadFeatures(path).rdd
    assert(annot.count === 10)
  }

  sparkTest("Can read a .interval_list file") {
    val path = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val annot: RDD[Feature] = sc.loadFeatures(path).rdd
    assert(annot.count == 369)
    val arr = annot.collect

    val first = arr.find(f => f.getReferenceName == "chr1" && f.getStart == 14415L && f.getEnd == 14499L).get
    assert(first.getName === "gn|DDX11L1;gn|RP11-34P13.2;ens|ENSG00000223972;ens|ENSG00000227232;vega|OTTHUMG00000000958;vega|OTTHUMG00000000961")

    val last = arr.find(f => f.getReferenceName == "chrY" && f.getStart == 27190031L && f.getEnd == 27190210L).get
    assert(last.getName === "gn|BPY2C;ccds|CCDS44030;ens|ENSG00000185894;vega|OTTHUMG00000045199")
  }

  sparkTest("can read a small .vcf file with a validation issue") {
    val path = testFile("invalid/small.INFO_flag.vcf")

    val vcs = sc.loadVcf(path, stringency = ValidationStringency.LENIENT)
    assert(vcs.rdd.count === 1)
  }

  sparkTest("can read a small .vcf file") {
    val path = testFile("small.vcf")

    val gts = sc.loadGenotypes(path)
    val vcRdd = gts.toVariantContexts
    val vcs = vcRdd.rdd.collect.sortBy(_.position)
    assert(vcs.size === 6)

    val vc = vcs.head
    val variant = vc.variant.variant
    assert(variant.getReferenceName === "1")
    assert(variant.getStart === 14396L)
    assert(variant.getEnd === 14400L)
    assert(variant.getReferenceAllele === "CTGT")
    assert(variant.getAlternateAllele === "C")
    assert(variant.getNames.isEmpty)
    assert(variant.getFiltersApplied === true)
    assert(variant.getFiltersPassed === false)
    assert(variant.getFiltersFailed.contains("IndelQD"))

    assert(vc.genotypes.size === 3)

    val gt = vc.genotypes.head
    assert(gt.getVariantCallingAnnotations != null)
    assert(gt.getReadDepth === 20)
  }

  sparkTest("can read a gzipped .vcf file") {
    val path = testFile("test.vcf.gz")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 7)
  }

  sparkTest("can read a vcf file with an empty alt") {
    val path = testFile("test.vcf")
    val vcs = sc.loadVariants(path)
    assert(vcs.rdd.count === 7)
  }

  sparkTest("can read a BGZF gzipped .vcf file with .gz file extension") {
    val path = testFile("test.vcf.bgzf.gz")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 7)
  }

  sparkTest("can read a BGZF gzipped .vcf file with .bgz file extension") {
    val path = testFile("test.vcf.bgz")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 7)
  }

  sparkTest("can read a vcf file with a projection") {
    val path = testFile("test.vcf")
    val vcs = sc.loadVcfWithProjection(path, Set("NS"), Set())
    assert(vcs.rdd.count === 7)
    assert(vcs.rdd.filter(_.variant.variant.getAnnotation().getAttributes().containsKey("NS"))
      .count === 7)
    assert(vcs.rdd.flatMap(_.genotypes)
      .filter(_.getVariantCallingAnnotations().getAttributes().containsKey("HQ"))
      .count === 0)
  }

  ignore("can read an uncompressed BCFv2.2 file") { // see https://github.com/samtools/htsjdk/issues/507
    val path = testFile("test.uncompressed.bcf")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 7)
  }

  ignore("can read a BGZF compressed BCFv2.2 file") { // see https://github.com/samtools/htsjdk/issues/507
    val path = testFile("test.compressed.bcf")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 7)
  }

  sparkTest("loadIndexedVcf with 1 ReferenceRegion") {
    val path = testFile("bqsr1.vcf")
    val refRegion = ReferenceRegion("22", 16097643, 16098647)
    val vcs = sc.loadIndexedVcf(path, refRegion)
    assert(vcs.rdd.count == 17)
  }

  sparkTest("loadIndexedVcf with multiple ReferenceRegions") {
    val path = testFile("bqsr1.vcf")
    val refRegion1 = ReferenceRegion("22", 16050677, 16050822)
    val refRegion2 = ReferenceRegion("22", 16097643, 16098647)
    val vcs = sc.loadIndexedVcf(path, Iterable(refRegion1, refRegion2))
    assert(vcs.rdd.count == 23)
  }

  sparkTest("load block compressed interleaved fastq") {
    Seq(".gz", ".bgz", ".bz2").foreach(ext => {
      val inputName = "interleaved_fastq_sample1.ifq%s".format(ext)
      val path = testFile(inputName)
      val reads = sc.loadAlignments(path)
      assert(reads.rdd.count === 6)
      assert(reads.rdd.filter(_.getReadPaired).count === 6)
      assert(reads.rdd.filter(_.getReadInFragment == 0).count === 3)
      assert(reads.rdd.filter(_.getReadInFragment == 1).count === 3)
    })
  }

  (1 to 4) foreach { testNumber =>
    val inputName = "interleaved_fastq_sample%d.ifq".format(testNumber)
    val path = testFile(inputName)

    sparkTest("import records from interleaved FASTQ: %d".format(testNumber)) {

      val reads = sc.loadAlignments(path)
      if (testNumber == 1) {
        assert(reads.rdd.count === 6)
        assert(reads.rdd.filter(_.getReadPaired).count === 6)
        assert(reads.rdd.filter(_.getReadInFragment == 0).count === 3)
        assert(reads.rdd.filter(_.getReadInFragment == 1).count === 3)
      } else {
        assert(reads.rdd.count === 4)
        assert(reads.rdd.filter(_.getReadPaired).count === 4)
        assert(reads.rdd.filter(_.getReadInFragment == 0).count === 2)
        assert(reads.rdd.filter(_.getReadInFragment == 1).count === 2)
      }

      assert(reads.rdd.collect.forall(_.getSequence.toString.length === 250))
      assert(reads.rdd.collect.forall(_.getQuality.toString.length === 250))
    }
  }

  sparkTest("import block compressed single fastq") {
    Seq(".gz", ".bgz", ".bz2").foreach(ext => {
      val inputName = "fastq_sample1.fq%s".format(ext)
      val path = testFile(inputName)
      val reads = sc.loadAlignments(path)
      assert(reads.rdd.count === 6)
      assert(reads.rdd.filter(_.getReadPaired).count === 0)
    })
  }

  (1 to 4) foreach { testNumber =>
    val inputName = "fastq_sample%d.fq".format(testNumber)
    val path = testFile(inputName)

    sparkTest("import records from single ended FASTQ: %d".format(testNumber)) {

      val reads = sc.loadAlignments(path)
      if (testNumber == 1) {
        assert(reads.rdd.count === 6)
        assert(reads.rdd.filter(_.getReadPaired).count === 0)
      } else if (testNumber == 4) {
        assert(reads.rdd.count === 4)
        assert(reads.rdd.filter(_.getReadPaired).count === 0)
      } else {
        assert(reads.rdd.count === 5)
        assert(reads.rdd.filter(_.getReadPaired).count === 0)
      }

      assert(reads.rdd.collect.forall(_.getSequence.toString.length === 250))
      assert(reads.rdd.collect.forall(_.getQuality.toString.length === 250))
    }
  }

  sparkTest("filter on load using the filter2 API") {
    val path = testFile("bqsr1.vcf")

    val variants = sc.loadVariants(path)
    assert(variants.rdd.count === 681)
    assert(variants.dataset.count === 681)
    assert(variants.dataset.rdd.count === 681)

    val loc = tmpLocation()
    variants.saveAsParquet(loc, 1024, 1024) // force more than one row group (block)

    val pred: FilterPredicate = (LongColumn("start") === 16097631L)
    // the following only reads one row group
    val adamVariants = sc.loadParquetVariants(loc, optPredicate = Some(pred))
    assert(adamVariants.rdd.count === 1)
  }

  sparkTest("saveAsParquet with file path") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(outputPath)
    val reloadedReads = sc.loadAlignments(outputPath)
    assert(reads.rdd.count === reloadedReads.rdd.count)
  }

  sparkTest("saveAsParquet with file path, block size, page size") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(outputPath, 1024, 2048)
    val reloadedReads = sc.loadAlignments(outputPath)
    assert(reads.rdd.count === reloadedReads.rdd.count)
  }

  sparkTest("saveAsParquet with save args") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(TestSaveArgs(outputPath))
    val reloadedReads = sc.loadAlignments(outputPath)
    assert(reads.rdd.count === reloadedReads.rdd.count)
  }

  sparkTest("read a HLA fasta from GRCh38") {
    val inputPath = testFile("HLA_DQB1_05_01_01_02.fa")
    val gDataset = sc.loadFasta(inputPath, 10000L)
    assert(gDataset.sequences.records.size === 1)
    assert(gDataset.sequences.records.head.name === "HLA-DQB1*05:01:01:02")
    val fragments = gDataset.rdd.collect
    assert(fragments.size === 1)
    assert(fragments.head.getContigName === "HLA-DQB1*05:01:01:02")
  }

  sparkTest("read a gzipped fasta file") {
    val inputPath = testFile("chr20.250k.fa.gz")
    val contigFragments = sc.loadFasta(inputPath, 10000L)
      .transform((rdd: RDD[NucleotideContigFragment]) => {
        rdd.sortBy(_.getIndex.toInt)
      })
    assert(contigFragments.rdd.count() === 26)
    val first: NucleotideContigFragment = contigFragments.rdd.first()
    assert(first.getContigName === null)
    assert(first.getDescription === "gi|224384749|gb|CM000682.1| Homo sapiens chromosome 20, GRCh37 primary reference assembly")
    assert(first.getIndex === 0)
    assert(first.getSequence.length === 10000)
    assert(first.getStart === 0L)
    assert(first.getEnd === 10000L)
    assert(first.getFragments === 26)

    // 250k file actually has 251930 bases
    val last: NucleotideContigFragment = contigFragments.rdd.collect().last
    assert(last.getIndex === 25)
    assert(last.getStart === 250000L)
    assert(last.getEnd === 251930L)
  }

  sparkTest("loadIndexedBam with 1 ReferenceRegion") {
    val refRegion = ReferenceRegion("chr2", 100, 101)
    val path = testFile("indexed_bams/sorted.bam")
    val reads = sc.loadIndexedBam(path, refRegion)
    assert(reads.rdd.count == 1)
  }

  sparkTest("loadIndexedBam with multiple ReferenceRegions") {
    val refRegion1 = ReferenceRegion("chr2", 100, 101)
    val refRegion2 = ReferenceRegion("3", 10, 17)
    val path = testFile("indexed_bams/sorted.bam")
    val reads = sc.loadIndexedBam(path, Iterable(refRegion1, refRegion2))
    assert(reads.rdd.count == 2)
  }

  sparkTest("loadIndexedBam with multiple ReferenceRegions and indexed bams") {
    val refRegion1 = ReferenceRegion("chr2", 100, 101)
    val refRegion2 = ReferenceRegion("3", 10, 17)
    val path = testFile("indexed_bams/sorted.bam").replace(".bam", "*.bam")
    val reads = sc.loadIndexedBam(path, Iterable(refRegion1, refRegion2))
    assert(reads.rdd.count == 4)
  }

  sparkTest("loadIndexedBam with multiple ReferenceRegions and a directory of indexed bams") {
    val refRegion1 = ReferenceRegion("chr2", 100, 101)
    val refRegion2 = ReferenceRegion("3", 10, 17)
    val path = new File(testFile("indexed_bams/sorted.bam")).getParent()
    val reads = sc.loadIndexedBam(path, Iterable(refRegion1, refRegion2))
    assert(reads.rdd.count == 4)
  }

  sparkTest("loadIndexedBam should throw exception without an index file") {
    val refRegion = ReferenceRegion("1", 26472780, 26472790)
    val path = testFile("bams/small.bam")
    intercept[FileNotFoundException] {
      sc.loadIndexedBam(path, Iterable(refRegion))
    }
  }

  sparkTest("loadIndexedBam should work with indexed file with index naming format <filename>.bai") {
    val refRegion = ReferenceRegion("1", 1, 100)
    val path = testFile("indexed_bams/sorted.2.bam")
    val reads = sc.loadIndexedBam(path, refRegion)
    assert(reads.rdd.count == 1)
  }

  sparkTest("loadIndexedBam glob should throw exception without an index file") {
    val refRegion = ReferenceRegion("1", 26472780, 26472790)
    val path = new File(testFile("bams/small.bam")).getParent()
    intercept[FileNotFoundException] {
      sc.loadIndexedBam(path, Iterable(refRegion))
    }
  }

  sparkTest("loadBam with a glob") {
    val path = testFile("indexed_bams/sorted.bam").replace(".bam", "*.bam")
    val reads = sc.loadBam(path)
    assert(reads.rdd.count == 10)
  }

  sparkTest("loadBam with a directory") {
    val path = new File(testFile("indexed_bams/sorted.bam")).getParent()
    val reads = sc.loadBam(path)
    assert(reads.rdd.count == 10)
  }

  sparkTest("load vcf with a glob") {
    val path = testFile("bqsr1.vcf").replace("bqsr1", "*")

    val vcs = sc.loadVcf(path)

    assert(vcs.samples.size === 8)
    assert(vcs.headerLines.size === 154)
    assert(vcs.sequences.size === 31)

    val variants = vcs.toVariants
    assert(variants.rdd.count === 778)
  }

  sparkTest("load vcf from a directory") {
    val path = new File(testFile("vcf_dir/1.vcf")).getParent()

    val variants = sc.loadVcf(path).toVariants
    assert(variants.rdd.count === 682)
  }

  sparkTest("load gvcf which contains a multi-allelic row from a directory") {
    val path = new File(testFile("gvcf_dir/gvcf_multiallelic.g.vcf")).getParent()

    val variants = sc.loadVcf(path).toVariants
    assert(variants.rdd.count === 12)
  }

  sparkTest("load and save gvcf which contains rows without likelihoods") {
    val vcs = sc.loadVcf(testFile("gvcf_dir/gvcf_multiallelic_noPLs.g.vcf"))
    assert(vcs.toVariants.rdd.count === 6)

    // can't validate output due to multiallelic sorting issue,
    // but we can validate that saving doesn't throw an exception, which is
    // important for #1673
    vcs.saveAsVcf(tmpLocation(".vcf"), false, false, false, ValidationStringency.STRICT)
  }

  sparkTest("parse annotations for multi-allelic rows") {
    val path = testFile("gvcf_dir/gvcf_multiallelic.g.vcf")

    val variants = sc.loadVcf(path).toVariants
    val multiAllelicVariants = variants.rdd
      .filter(_.getReferenceAllele == "TAAA")
      .sortBy(_.getAlternateAllele.length)
      .collect()

    val mleCounts = multiAllelicVariants.map(_.getAnnotation.getAttributes.get("MLEAC"))
    //ALT    T,TA,TAA,<NON_REF>
    //MLEAC  0,1,1,0
    assert(mleCounts === Array("0", "1", "1"))
  }

  sparkTest("load parquet with globs") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(outputPath)
    reads.saveAsParquet(outputPath.replace(".adam", ".2.adam"))

    val paths = new Path(outputPath.replace(".adam", "*.adam") + "/*")

    val reloadedReads = sc.loadParquetAlignments(outputPath.replace(".adam", "*.adam") + "/*")
    assert((2 * reads.rdd.count) === reloadedReads.rdd.count)
  }

  sparkTest("bad glob should fail") {
    val inputPath = testFile("small.sam").replace(".sam", "*.sad")
    intercept[FileNotFoundException] {
      sc.loadAlignments(inputPath)
    }
  }

  sparkTest("empty directory should fail") {
    val inputPath = tmpLocation()
    intercept[FileNotFoundException] {
      sc.loadAlignments(inputPath)
    }
  }

  sparkTest("can read a SnpEff-annotated .vcf file") {
    val path = testFile("small_snpeff.vcf")
    val variantRdd = sc.loadVariants(path)
    val variants = variantRdd.rdd.sortBy(_.getStart).collect

    variants.foreach(v => v.getStart.longValue match {
      case 14396L => {
        assert(v.getReferenceAllele === "CTGT")
        assert(v.getAlternateAllele === "C")
        assert(v.getAnnotation.getTranscriptEffects.size === 4)

        val te = v.getAnnotation.getTranscriptEffects.get(0)
        assert(te.getAlternateAllele === "C")
        assert(te.getEffects.contains("downstream_gene_variant"))
        assert(te.getGeneName === "WASH7P")
        assert(te.getGeneId === "ENSG00000227232")
        assert(te.getFeatureType === "transcript")
        assert(te.getFeatureId === "ENST00000488147.1")
        assert(te.getBiotype === "unprocessed_pseudogene")
      }
      case 14521L => {
        assert(v.getReferenceAllele === "G")
        assert(v.getAlternateAllele === "A")
        assert(v.getAnnotation.getTranscriptEffects.size === 4)
      }
      case 19189L => {
        assert(v.getReferenceAllele === "GC")
        assert(v.getAlternateAllele === "G")
        assert(v.getAnnotation.getTranscriptEffects.size === 3)
      }
      case 63734L => {
        assert(v.getReferenceAllele === "CCTA")
        assert(v.getAlternateAllele === "C")
        assert(v.getAnnotation.getTranscriptEffects.size === 1)
      }
      case 752720L => {
        assert(v.getReferenceAllele === "A")
        assert(v.getAlternateAllele === "G")
        assert(v.getAnnotation.getTranscriptEffects.size === 2)
      }
      case _ => fail("unexpected variant start " + v.getStart)
    })
  }

  sparkTest("loadAlignments should not fail on single-end and paired-end fastq reads") {
    val readsFilepath1 = testFile("bqsr1-r1.fq")
    val readsFilepath2 = testFile("bqsr1-r2.fq")
    val fastqReads1 = sc.loadAlignments(readsFilepath1)
    val fastqReads2 = sc.loadAlignments(readsFilepath2)
    val pairedReads = sc.loadAlignments(readsFilepath1, optPathName2 = Option(readsFilepath2))
    assert(fastqReads1.rdd.count === 488)
    assert(fastqReads2.rdd.count === 488)
    assert(pairedReads.rdd.count === 976)
  }

  sparkTest("load queryname sorted sam as fragments") {
    val samFile = testFile("sample1.queryname.sam")
    assert(sc.filesAreQueryGrouped(samFile))
    val fragments = sc.loadFragments(samFile)
    assert(fragments.rdd.count === 3)
    val reads = fragments.toReads
    assert(reads.rdd.count === 6)
  }

  sparkTest("load query grouped sam as fragments") {
    val samFile = testFile("sample1.query.sam")
    assert(sc.filesAreQueryGrouped(samFile))
    val fragments = sc.loadFragments(samFile)
    assert(fragments.rdd.count === 3)
    val reads = fragments.toReads
    assert(reads.rdd.count === 6)
  }

  sparkTest("load paired fastq") {
    val pathR1 = testFile("proper_pairs_1.fq")
    val pathR2 = testFile("proper_pairs_2.fq")
    val reads = sc.loadPairedFastq(pathR1, pathR2)
    assert(reads.rdd.count === 6)
  }

  sparkTest("load paired fastq without cache") {
    val pathR1 = testFile("proper_pairs_1.fq")
    val pathR2 = testFile("proper_pairs_2.fq")
    val reads = sc.loadPairedFastq(pathR1, pathR2, persistLevel = None)
    assert(reads.rdd.count === 6)
  }

  sparkTest("load paired fastq as fragments") {
    val pathR1 = testFile("proper_pairs_1.fq")
    val pathR2 = testFile("proper_pairs_2.fq")
    val fragments = sc.loadPairedFastqAsFragments(pathR1, pathR2)
    assert(fragments.rdd.count === 3)
  }

  sparkTest("load paired fastq as fragments without cache") {
    val pathR1 = testFile("proper_pairs_1.fq")
    val pathR2 = testFile("proper_pairs_2.fq")
    val fragments = sc.loadPairedFastqAsFragments(pathR1, pathR2, persistLevel = None)
    assert(fragments.rdd.count === 3)
  }

  sparkTest("load HTSJDK sequence dictionary") {
    val path = testFile("hs37d5.dict")
    val sequences = sc.loadSequenceDictionary(path)

    assert(sequences.records.size === 85)
    assert(sequences("1").isDefined)
    assert(sequences("1").get.length === 249250621L)
    assert(sequences("NC_007605").isDefined)
    assert(sequences("NC_007605").get.length === 171823L)
  }

  sparkTest("load Bedtools .genome file as sequence dictionary") {
    val path = testFile("hg19.genome")
    val sequences = sc.loadSequenceDictionary(path)

    assert(sequences.records.size === 93)
    assert(sequences("chr1").isDefined)
    assert(sequences("chr1").get.length === 249250621L)
    assert(sequences("chr17_gl000206_random").isDefined)
    assert(sequences("chr17_gl000206_random").get.length === 41001L)
  }

  sparkTest("load Bedtools .genome.txt file as sequence dictionary") {
    val path = testFile("hg19.genome.txt")
    val sequences = sc.loadSequenceDictionary(path)

    assert(sequences.records.size === 93)
    assert(sequences("chr1").isDefined)
    assert(sequences("chr1").get.length === 249250621L)
    assert(sequences("chr17_gl000206_random").isDefined)
    assert(sequences("chr17_gl000206_random").get.length === 41001L)
  }

  sparkTest("load UCSC Genome Browser chromInfo.txt file as sequence dictionary") {
    val path = testFile("chromInfo.txt")
    val sequences = sc.loadSequenceDictionary(path)

    assert(sequences.records.size === 93)
    assert(sequences("chr1").isDefined)
    assert(sequences("chr1").get.length === 249250621L)
    assert(sequences("chr1").get.url.get === "/gbdb/hg19/hg19.2bit")
    assert(sequences("chr17_ctg5_hap1").isDefined)
    assert(sequences("chr17_ctg5_hap1").get.length === 1680828L)
    assert(sequences("chr17_ctg5_hap1").get.url.get === "/gbdb/hg19/hg19.2bit")
  }

  sparkTest("load unrecognized file extension as sequence dictionary fails") {
    val path = testFile("unmapped.sam")
    intercept[IllegalArgumentException] {
      sc.loadSequenceDictionary(path)
    }
  }

  sparkTest("load BED features with Bedtools .genome file as sequence dictionary") {
    val sdPath = testFile("hg19.genome") // uses "chr1"
    val sd = sc.loadSequenceDictionary(sdPath)

    val path = testFile("gencode.v7.annotation.trunc10.bed") // uses "chr1"
    val featureDs = sc.sc.loadFeatures(path, optSequenceDictionary = Some(sd))
    val features: RDD[Feature] = featureDs.rdd
    assert(features.count === 10)

    val sequences = featureDs.sequences
    assert(sequences.records.size === 93)
    assert(sequences("chr1").isDefined)
    assert(sequences("chr1").get.length === 249250621L)
    assert(sequences("chr17_gl000206_random").isDefined)
    assert(sequences("chr17_gl000206_random").get.length === 41001L)
  }

  sparkTest("load BED features with Bedtools .genome file as sequence dictionary, no matching features") {
    val sdPath = testFile("hg19.genome") // uses "chr1"
    val sd = sc.loadSequenceDictionary(sdPath)

    val path = testFile("dvl1.200.bed") // uses "1"
    val featureDs = sc.sc.loadFeatures(path, optSequenceDictionary = Some(sd))
    val features: RDD[Feature] = featureDs.rdd
    assert(features.count === 197)

    val sequences = featureDs.sequences
    assert(sequences.records.size === 93)
    assert(sequences("chr1").isDefined)
    assert(sequences("chr1").get.length === 249250621L)
    assert(sequences("chr17_gl000206_random").isDefined)
    assert(sequences("chr17_gl000206_random").get.length === 41001L)
  }

  sparkTest("convert program record") {
    val pr = new SAMProgramRecord("pgId")
    pr.setPreviousProgramGroupId("ppgId")
    pr.setProgramName("myProg")
    pr.setProgramVersion("1.2.3")
    pr.setCommandLine("myProg aCommand")
    val ps = ADAMContext.convertSAMProgramRecord(pr)
    assert(ps.getId === "pgId")
    assert(ps.getProgramName === "myProg")
    assert(ps.getVersion === "1.2.3")
    assert(ps.getCommandLine === "myProg aCommand")
    assert(ps.getPreviousId === "ppgId")
  }

  sparkTest("load program record from sam file") {
    val input = testFile("small.sam")
    val samHeader = SAMHeaderReader.readSAMHeaderFrom(new Path(input),
      sc.hadoopConfiguration)
    val programs = sc.loadBamPrograms(samHeader)
    assert(programs.size === 2)
    val firstPg = programs.filter(_.getPreviousId == null)
    assert(firstPg.size === 1)
    assert(firstPg.head.getId === "p1")
    assert(firstPg.head.getProgramName === "myProg")
    assert(firstPg.head.getCommandLine === "\"myProg 123\"")
    assert(firstPg.head.getVersion === "1.0.0")
    val secondPg = programs.filter(_.getPreviousId != null)
    assert(secondPg.size === 1)
    assert(secondPg.head.getId === "p2")
    assert(secondPg.head.getPreviousId === "p1")
    assert(secondPg.head.getProgramName === "myProg")
    assert(secondPg.head.getCommandLine === "\"myProg 456\"")
    assert(secondPg.head.getVersion === "1.0.0")
  }
}
