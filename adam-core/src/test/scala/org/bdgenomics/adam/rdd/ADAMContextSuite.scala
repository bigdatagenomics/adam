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

import java.io.{ File, FileNotFoundException }
import java.util.UUID
import htsjdk.samtools.DiskBasedBAMFileIndex
import com.google.common.io.Files
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignedReadRDD
import org.bdgenomics.adam.util.PhredUtils._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import org.seqdoop.hadoop_bam.{ BAMInputFormat, CRAMInputFormat }
import scala.collection.JavaConversions._

case class TestSaveArgs(var outputPath: String) extends ADAMSaveAnyArgs {
  var sortFastqOutput = false
  var asSingleFile = false
  var deferMerging = false
  var blockSize = 128 * 1024 * 1024
  var pageSize = 1 * 1024 * 1024
  var compressionCodec = CompressionCodecName.GZIP
  var logLevel = "SEVERE"
  var disableDictionaryEncoding = false
}

class ADAMContextSuite extends ADAMFunSuite {

  sparkTest("sc.loadParquet should not fail on unmapped reads") {
    val readsFilepath = testFile("unmapped.sam")

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[AlignmentRecord] = sc.loadAlignments(readsFilepath).rdd
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
    val reads: RDD[AlignmentRecord] = sc.loadAlignments(path).rdd
    assert(reads.count() === 20)
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
      .filter(a => (a.getReadMapped && a.getMapq > 30))
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
    val features: RDD[Feature] = sc.loadFeatures(path).rdd
    assert(features.count === 15)
  }

  sparkTest("Can read a .bed file") {
    // note: this .bed doesn't actually conform to the UCSC BED spec...sigh...
    val path = testFile("gencode.v7.annotation.trunc10.bed")
    val features: RDD[Feature] = sc.loadFeatures(path).rdd
    assert(features.count === 10)
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

    val first = arr.find(f => f.getContigName == "chr1" && f.getStart == 14415L && f.getEnd == 14499L).get
    assert(first.getName === "gn|DDX11L1;gn|RP11-34P13.2;ens|ENSG00000223972;ens|ENSG00000227232;vega|OTTHUMG00000000958;vega|OTTHUMG00000000961")

    val last = arr.find(f => f.getContigName == "chrY" && f.getStart == 27190031L && f.getEnd == 27190210L).get
    assert(last.getName === "gn|BPY2C;ccds|CCDS44030;ens|ENSG00000185894;vega|OTTHUMG00000045199")
  }

  sparkTest("can read a small .vcf file") {
    val path = testFile("small.vcf")

    val gts = sc.loadGenotypes(path)
    val vcRdd = gts.toVariantContextRDD
    val vcs = vcRdd.rdd.collect.sortBy(_.position)
    assert(vcs.size === 6)

    val vc = vcs.head
    val variant = vc.variant.variant
    assert(variant.getContigName === "1")
    assert(variant.getStart === 14396L)
    assert(variant.getEnd === 14400L)
    assert(variant.getReferenceAllele === "CTGT")
    assert(variant.getAlternateAllele === "C")
    assert(variant.getNames.isEmpty)
    assert(variant.getFiltersApplied === true)
    assert(variant.getFiltersPassed === false)
    assert(variant.getFiltersFailed.contains("IndelQD"))
    assert(variant.getSomatic === false)

    assert(vc.genotypes.size === 3)

    val gt = vc.genotypes.head
    assert(gt.getVariantCallingAnnotations != null)
    assert(gt.getReadDepth === 20)
  }

  sparkTest("can read a gzipped .vcf file") {
    val path = testFile("test.vcf.gz")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 6)
  }

  sparkTest("can read a BGZF gzipped .vcf file with .gz file extension") {
    val path = testFile("test.vcf.bgzf.gz")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 6)
  }

  sparkTest("can read a BGZF gzipped .vcf file with .bgz file extension") {
    val path = testFile("test.vcf.bgz")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 6)
  }

  ignore("can read an uncompressed BCFv2.2 file") { // see https://github.com/samtools/htsjdk/issues/507
    val path = testFile("test.uncompressed.bcf")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 6)
  }

  ignore("can read a BGZF compressed BCFv2.2 file") { // see https://github.com/samtools/htsjdk/issues/507
    val path = testFile("test.compressed.bcf")
    val vcs = sc.loadVcf(path)
    assert(vcs.rdd.count === 6)
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
      assert(reads.rdd.collect.forall(_.getQual.toString.length === 250))
    }
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
      assert(reads.rdd.collect.forall(_.getQual.toString.length === 250))
    }
  }

  sparkTest("filter on load using the filter2 API") {
    val path = testFile("bqsr1.vcf")

    val variants = sc.loadVariants(path)
    assert(variants.rdd.count === 681)

    val loc = tmpLocation()
    variants.saveAsParquet(loc, 1024, 1024) // force more than one row group (block)

    val pred: FilterPredicate = (LongColumn("start") === 16097631L)
    // the following only reads one row group
    val adamVariants = sc.loadParquetVariants(loc, predicate = Some(pred))
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
    val gRdd = sc.loadFasta(inputPath, 10000L)
    assert(gRdd.sequences.records.size === 1)
    assert(gRdd.sequences.records.head.name === "HLA-DQB1*05:01:01:02")
    val fragments = gRdd.rdd.collect
    assert(fragments.size === 1)
    assert(fragments.head.getContig.getContigName === "HLA-DQB1*05:01:01:02")
  }

  sparkTest("read a gzipped fasta file") {
    val inputPath = testFile("chr20.250k.fa.gz")
    val contigFragments: RDD[NucleotideContigFragment] = sc.loadFasta(inputPath, 10000L)
      .rdd
      .sortBy(_.getFragmentNumber.toInt)
    assert(contigFragments.rdd.count() === 26)
    val first: NucleotideContigFragment = contigFragments.first()
    assert(first.getContig.getContigName === null)
    assert(first.getDescription === "gi|224384749|gb|CM000682.1| Homo sapiens chromosome 20, GRCh37 primary reference assembly")
    assert(first.getFragmentNumber === 0)
    assert(first.getFragmentSequence.length === 10000)
    assert(first.getFragmentStartPosition === 0L)
    assert(first.getFragmentEndPosition === 9999L)
    assert(first.getNumberOfFragmentsInContig === 26)

    // 250k file actually has 251930 bases
    val last: NucleotideContigFragment = contigFragments.rdd.collect().last
    assert(last.getFragmentNumber === 25)
    assert(last.getFragmentStartPosition === 250000L)
    assert(last.getFragmentEndPosition === 251929L)
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

    val variants = sc.loadVcf(path).toVariantRDD
    assert(variants.rdd.count === 710)
  }

  sparkTest("load vcf from a directory") {
    val path = new File(testFile("vcf_dir/1.vcf")).getParent()

    val variants = sc.loadVcf(path).toVariantRDD
    assert(variants.rdd.count === 681)
  }

  sparkTest("load gvcf which contains a multi-allelic row from a directory") {
    val path = new File(testFile("gvcf_dir/gvcf_multiallelic.g.vcf")).getParent()

    val variants = sc.loadVcf(path).toVariantRDD
    // Not sure that the count should be 7 below, however the current failure to read the mult-allelic site happens
    // before this assertion is even reached
    assert(variants.rdd.count === 6)
  }

  sparkTest("load parquet with globs") {
    val inputPath = testFile("small.sam")
    val reads = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(outputPath)
    reads.saveAsParquet(outputPath.replace(".adam", ".2.adam"))

    val paths = new Path(outputPath.replace(".adam", "*.adam") + "/*")
    assert(sc.getFsAndFiles(paths).size > 2)

    val reloadedReads = sc.loadParquetAlignments(outputPath.replace(".adam", "*.adam") + "/*")
    assert((2 * reads.rdd.count) === reloadedReads.rdd.count)
  }

  sparkTest("bad glob should fail") {
    val inputPath = testFile("small.sam")
    intercept[FileNotFoundException] {
      sc.getFsAndFiles(new Path(inputPath.replace(".sam", "*.sad")))
    }
  }

  sparkTest("empty directory should fail") {
    val outputPath = tmpLocation()
    intercept[FileNotFoundException] {
      sc.getFsAndFiles(new Path(outputPath))
    }
  }
}

