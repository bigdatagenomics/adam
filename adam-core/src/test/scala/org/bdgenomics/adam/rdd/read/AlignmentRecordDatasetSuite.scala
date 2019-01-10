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
package org.bdgenomics.adam.rdd.read

import java.io.File
import java.nio.file.Files
import htsjdk.samtools.{ SAMFileHeader, ValidationStringency }
import org.apache.spark.api.java.function.{ Function => JFunction, Function2 }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.io.FastqRecordReader
import org.bdgenomics.adam.models.{
  Coverage,
  ReadGroup,
  ReadGroupDictionary,
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord,
  SnpTable,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{
  ADAMContext,
  TestSaveArgs
}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentDataset
import org.bdgenomics.adam.rdd.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.variant.{
  GenotypeDataset,
  VariantDataset,
  VariantContextDataset,
  VCFOutFormatter
}
import org.bdgenomics.adam.sql.{
  AlignmentRecord => AlignmentRecordProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  NucleotideContigFragment => NucleotideContigFragmentProduct,
  Variant => VariantProduct,
  VariantContext => VariantContextProduct
}
import org.bdgenomics.adam.util.{ ADAMFunSuite, AttributeUtils, ManualRegionPartitioner }
import org.bdgenomics.formats.avro._
import org.seqdoop.hadoop_bam.{ CRAMInputFormat, SAMFormat }
import scala.collection.JavaConversions._
import scala.util.Random

private object SequenceIndexWithReadOrdering extends Ordering[((Int, Long), (AlignmentRecord, Int))] {
  def compare(a: ((Int, Long), (AlignmentRecord, Int)),
              b: ((Int, Long), (AlignmentRecord, Int))): Int = {
    if (a._1._1 == b._1._1) {
      a._1._2.compareTo(b._1._2)
    } else {
      a._1._1.compareTo(b._1._1)
    }
  }
}

class SameTypeFunction2 extends Function2[AlignmentRecordDataset, RDD[AlignmentRecord], AlignmentRecordDataset] {

  def call(v1: AlignmentRecordDataset, v2: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    ADAMContext.alignmentRecordsToAlignmentRecordsConversionFn(v1, v2)
  }
}

object AlignmentRecordDatasetSuite extends Serializable {

  private def fragToRead(f: Fragment): AlignmentRecord = {
    f.getAlignments().get(0)
  }

  def ncfFn(r: AlignmentRecord): NucleotideContigFragment = {
    NucleotideContigFragment.newBuilder
      .setContigName(r.getReferenceName)
      .setSequence(r.getSequence)
      .build
  }

  def ncfFn(f: Fragment): NucleotideContigFragment = {
    ncfFn(fragToRead(f))
  }

  def covFn(r: AlignmentRecord): Coverage = {
    Coverage(r.getReferenceName,
      r.getStart,
      r.getEnd,
      1)
  }

  def covFn(f: Fragment): Coverage = {
    covFn(fragToRead(f))
  }

  def featFn(r: AlignmentRecord): Feature = {
    Feature.newBuilder
      .setReferenceName(r.getReferenceName)
      .setStart(r.getStart)
      .setEnd(r.getEnd)
      .build
  }

  def featFn(f: Fragment): Feature = {
    featFn(fragToRead(f))
  }

  def fragFn(r: AlignmentRecord): Fragment = {
    Fragment.newBuilder
      .setName(r.getReadName)
      .build
  }

  def genFn(r: AlignmentRecord): Genotype = {
    Genotype.newBuilder
      .setReferenceName(r.getReferenceName)
      .setStart(r.getStart)
      .setEnd(r.getEnd)
      .build
  }

  def genFn(f: Fragment): Genotype = {
    genFn(fragToRead(f))
  }

  def varFn(r: AlignmentRecord): Variant = {
    Variant.newBuilder
      .setReferenceName(r.getReferenceName)
      .setStart(r.getStart)
      .setEnd(r.getEnd)
      .build
  }

  def varFn(f: Fragment): Variant = {
    varFn(fragToRead(f))
  }
}

class AlignmentRecordDatasetSuite extends ADAMFunSuite {

  sparkTest("sorting reads") {
    val random = new Random("sorting".hashCode)
    val numReadsToCreate = 1000
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = AlignmentRecord.newBuilder().setReadMapped(mapped)
      if (mapped) {
        val referenceName = random.nextInt(numReadsToCreate / 10).toString
        val start = random.nextInt(1000000)
        builder.setReferenceName(referenceName).setStart(start).setEnd(start)
      }
      builder.setReadName((0 until 20).map(i => (random.nextInt(100) + 64)).mkString)
      builder.build()
    }
    val rdd = sc.parallelize(reads)

    // make seq dict 
    val contigNames = rdd.flatMap(r => Option(r.getReferenceName)).distinct.collect
    val sd = new SequenceDictionary(contigNames.map(v => SequenceRecord(v, 1000000L)).toVector)

    val sortedReads = AlignmentRecordDataset(rdd, sd, ReadGroupDictionary.empty, Seq.empty)
      .sortReadsByReferencePosition()
      .rdd
      .collect()
      .zipWithIndex
    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)
    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))
    // Make sure that we appropriately sorted the reads
    val expectedSortedReads = mapped.sortWith(
      (a, b) => a._1.getReferenceName < b._1.getReferenceName && a._1.getStart < b._1.getStart)
    assert(expectedSortedReads === mapped)
  }

  sparkTest("unmapped reads go at the end when sorting") {
    val inputPath = testFile("reads13.sam")
    val reads = sc.loadAlignments(inputPath)
    val sortedReads = reads.sortReadsByReferencePosition()
      .rdd
      .collect()
    assert(!sortedReads.last.getReadMapped)
    assert(sortedReads.last.getReadName === "&5[d@xJO")
  }

  sparkTest("coverage does not fail on unmapped reads") {
    val inputPath = testFile("unmapped.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
      .transform(rdd => {
        rdd.filter(!_.getReadMapped)
      })

    val coverage = reads.toCoverage()
    assert(coverage.rdd.count === 0)
  }

  sparkTest("computes coverage") {
    val inputPath = testFile("artificial.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)

    // get pileup at position 30
    val pointCoverage = reads.filterByOverlappingRegion(ReferenceRegion("artificial", 30, 31)).rdd.count
    def testCoverage(coverage: CoverageDataset) {
      assert(coverage.rdd.filter(r => r.start == 30).first.count == pointCoverage)
    }

    val coverage = reads.toCoverage()
    testCoverage(coverage)

    // test dataset path
    val readsDs = reads.transformDataset(ds => ds)
    val coverageDs = readsDs.toCoverage()
    testCoverage(coverageDs)
  }

  sparkTest("computes coverage with multiple samples") {
    // glob in 2 files with different samples
    // testFile cannot read complex globs, so we have to parse the parent path
    val relativePath = new File(testFile("NA12878.1_854950_855150.sam")).getParentFile.getPath
    val inputPath = relativePath + "/{NA12878.1_854950_855150,bqsr1}.sam"

    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)

    def countBySampleId(coverage: CoverageDataset, sampleId: String): Long = {
      coverage.rdd.filter(r => r.optSampleId == Some(sampleId)).count
    }

    val coverageRdd = reads.toCoverage()
    assert(countBySampleId(coverageRdd, "NA12878") == 677) // number of coverage when just NA12878 is computed
    assert(countBySampleId(coverageRdd, "HG00096") == 39000) // number of coverage when just HG00096 is computed
  }

  sparkTest("merges adjacent records with equal coverage values") {
    val inputPath = testFile("artificial.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)

    // repartition reads to 1 partition to achieve maximal merging of coverage
    val coverage: CoverageDataset = reads.transform(_.repartition(1))
      .toCoverage()
      .sort()
      .collapse()

    assert(coverage.rdd.count == 18)
    assert(coverage.flatten.rdd.count == 170)
  }

  sparkTest("sorting reads by reference index") {
    val random = new Random("sortingIndices".hashCode)
    val numReadsToCreate = 1000
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = AlignmentRecord.newBuilder().setReadMapped(mapped)
      if (mapped) {
        val contigName = random.nextInt(numReadsToCreate / 10).toString
        val start = random.nextInt(1000000)
        builder.setReferenceName(contigName).setStart(start).setEnd(start)
      }
      builder.setReadName((0 until 20).map(i => (random.nextInt(100) + 64)).mkString)
      builder.build()
    }
    val contigNames = reads.filter(_.getReadMapped).map(_.getReferenceName).toSet
    val sd = new SequenceDictionary(contigNames.toSeq
      .zipWithIndex
      .map(kv => {
        val (name, idx) = kv
        SequenceRecord(name, Int.MaxValue, index = Some(idx))
      }).toVector)

    val rdd = sc.parallelize(reads)
    val sortedReads = AlignmentRecordDataset(rdd, sd, ReadGroupDictionary.empty, Seq.empty)
      .sortReadsByReferencePositionAndIndex()
      .rdd
      .collect()
      .zipWithIndex
    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)

    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))

    def toIndex(r: AlignmentRecord): Int = {
      sd(r.getReferenceName).get.index.get
    }

    // Make sure that we appropriately sorted the reads
    import scala.math.Ordering._
    val expectedSortedReads = mapped.map(kv => {
      val (r, idx) = kv
      val start: Long = r.getStart
      ((toIndex(r), start), (r, idx))
    }).sortBy(_._1)
      .map(_._2)
    assert(expectedSortedReads === mapped)
  }

  sparkTest("round trip from ADAM to SAM and back to ADAM produces equivalent Read values") {
    val reads12Path = Thread.currentThread().getContextClassLoader.getResource("reads12.sam").getFile
    val rdd12A = sc.loadBam(reads12Path)

    val tempFile = Files.createTempDirectory("reads12")
    rdd12A.saveAsSam(tempFile.toAbsolutePath.toString + "/reads12.sam",
      asType = Some(SAMFormat.SAM))

    val rdd12B = sc.loadBam(tempFile.toAbsolutePath.toString + "/reads12.sam/part-r-00000.sam")

    assert(rdd12B.rdd.count() === rdd12A.rdd.count())

    val reads12A = rdd12A.rdd.collect()
    val reads12B = rdd12B.rdd.collect()

    reads12A.indices.foreach {
      case i: Int =>
        val (readA, readB) = (reads12A(i), reads12B(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQuality === readB.getQuality)
        assert(readA.getCigar === readB.getCigar)
    }
  }

  sparkTest("round trip with single CRAM file produces equivalent Read values") {
    val readsPath = testFile("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    sc.hadoopConfiguration.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY,
      referencePath)

    val rddA = sc.loadBam(readsPath)

    val tempFile = tmpFile("artificial.cram")
    rddA.saveAsSam(tempFile,
      asType = Some(SAMFormat.CRAM),
      asSingleFile = true,
      isSorted = true)

    val rddB = sc.loadBam(tempFile)

    assert(rddB.rdd.count() === rddA.rdd.count())

    val readsA = rddA.rdd.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      case i: Int =>
        val (readA, readB) = (readsA(i), readsB(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQuality === readB.getQuality)
        assert(readA.getCigar === readB.getCigar)
    }
  }

  sparkTest("round trip with sharded CRAM file produces equivalent Read values") {
    val readsPath = testFile("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    sc.hadoopConfiguration.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY,
      referencePath)

    val ardd = sc.loadBam(readsPath)
    val rddA = ardd.rdd

    val tempFile = tmpFile("artificial.cram")
    ardd.saveAsSam(tempFile,
      asType = Some(SAMFormat.CRAM),
      asSingleFile = false,
      isSorted = true)

    val rddB = sc.loadBam(tempFile + "/part-r-00000.cram")

    assert(rddB.rdd.count() === rddA.count())

    val readsA = rddA.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      case i: Int =>
        val (readA, readB) = (readsA(i), readsB(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQuality === readB.getQuality)
        assert(readA.getCigar === readB.getCigar)
    }
  }

  sparkTest("SAM conversion sets read mapped flag properly") {
    val filePath = getClass.getClassLoader.getResource("reads12.sam").getFile
    val sam = sc.loadAlignments(filePath)

    sam.rdd.collect().foreach(r => assert(r.getReadMapped))
  }

  sparkTest("load long FASTQ reads") {
    val readsPath = testFile("combined_2018-05-18.9900-10050.fastq")
    FastqRecordReader.setMaxReadLength(sc.hadoopConfiguration, 10050)
    val reads = sc.loadAlignments(readsPath)

    val readLengths = reads.rdd.map(_.getSequence.length)
      .countByValue

    assert(readLengths.size === 8)
    Seq(10038, 9987, 10010, 9935, 10016, 10042, 9906, 10044).foreach(i => {
      assert(readLengths(i) === 1L)
    })
  }

  sparkTest("load FASTQ with no bases") {
    val readsPath = testFile("fastq_nobases.fq")
    val reads = sc.loadAlignments(readsPath)

    assert(reads.dataset.count === 2)
    assert(reads.rdd.map(_.getSequence.length).reduce(_ + _) === 0)
  }

  sparkTest("convert malformed FASTQ (no quality scores) => SAM => well-formed FASTQ => SAM") {
    val noqualPath = Thread.currentThread().getContextClassLoader.getResource("fastq_noqual.fq").getFile
    val tempBase = Files.createTempDirectory("noqual").toAbsolutePath.toString

    //read FASTQ (malformed)
    val rddA = sc.loadFastq(noqualPath, None, None, ValidationStringency.LENIENT)

    //write SAM (fixed and now well-formed)
    rddA.saveAsSam(tempBase + "/noqualA.sam")

    //read SAM
    val rddB = sc.loadAlignments(tempBase + "/noqualA.sam")

    //write FASTQ (well-formed)
    rddB.saveAsFastq(tempBase + "/noqualB.fastq")

    //read FASTQ (well-formed)
    val rddC = sc.loadFastq(tempBase + "/noqualB.fastq", None, None, ValidationStringency.STRICT)

    val noqualA = rddA.rdd.collect()
    val noqualB = rddB.rdd.collect()
    val noqualC = rddC.rdd.collect()
    noqualA.indices.foreach {
      case i: Int =>
        val (readA, readB, readC) = (noqualA(i), noqualB(i), noqualC(i))
        assert(readA.getQuality != "*")
        assert(readB.getQuality == "B" * readB.getSequence.length)
        assert(readB.getQuality == readC.getQuality)
    }
  }

  sparkTest("round trip from ADAM to FASTQ and back to ADAM produces equivalent Read values") {
    val reads12Path = Thread.currentThread().getContextClassLoader.getResource("fastq_sample1.fq").getFile
    val rdd12A = sc.loadAlignments(reads12Path)

    val tempFile = Files.createTempDirectory("reads12")
    rdd12A.saveAsFastq(tempFile.toAbsolutePath.toString + "/reads12.fq")

    val rdd12B = sc.loadAlignments(tempFile.toAbsolutePath.toString + "/reads12.fq")

    assert(rdd12B.rdd.count() === rdd12A.rdd.count())

    val reads12A = rdd12A.rdd.collect()
    val reads12B = rdd12B.rdd.collect()

    reads12A.indices.foreach {
      case i: Int =>
        val (readA, readB) = (reads12A(i), reads12B(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQuality === readB.getQuality)
        assert(readA.getReadName === readB.getReadName)
    }
  }

  sparkTest("round trip from ADAM to paired-FASTQ and back to ADAM produces equivalent Read values") {
    val path1 = testFile("proper_pairs_1.fq")
    val path2 = testFile("proper_pairs_2.fq")
    val rddA = sc.loadAlignments(path1).reassembleReadPairs(sc.loadAlignments(path2).rdd,
      validationStringency = ValidationStringency.STRICT)

    assert(rddA.rdd.count() == 6)

    val tempFile = Files.createTempDirectory("reads")
    val tempPath1 = tempFile.toAbsolutePath.toString + "/reads1.fq"
    val tempPath2 = tempFile.toAbsolutePath.toString + "/reads2.fq"

    rddA.saveAsPairedFastq(tempPath1, tempPath2, validationStringency = ValidationStringency.STRICT)

    val rddB = sc.loadAlignments(tempPath1).reassembleReadPairs(sc.loadAlignments(tempPath2).rdd,
      validationStringency = ValidationStringency.STRICT)

    assert(rddB.rdd.count() === rddA.rdd.count())

    val readsA = rddA.rdd.collect()
    val readsB = rddB.rdd.collect()

    readsA.indices.foreach {
      case i: Int =>
        val (readA, readB) = (readsA(i), readsB(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQuality === readB.getQuality)
        assert(readA.getReadName === readB.getReadName)
    }
  }

  sparkTest("writing a small file with tags should produce the expected result") {
    val samPath = testFile("tag.sam")
    val ardd = sc.loadBam(samPath)

    val newSamPath = tmpFile("tag.sam")
    ardd.saveAsSam(newSamPath,
      asSingleFile = true)

    val brdd = sc.loadBam(newSamPath)

    assert(ardd.rdd.count === brdd.rdd.count)
    val aRecord = ardd.rdd.first
    val bRecord = brdd.rdd.first
    val aAttrs = AttributeUtils.parseAttributes(aRecord.getAttributes)
    val bAttrs = AttributeUtils.parseAttributes(bRecord.getAttributes)
    assert(aAttrs.length === 10)
    assert(bAttrs.length === 10)
    val bAttrsSet = bAttrs.map(_.tag).toSet
    assert(aAttrs.forall(attr => bAttrsSet.contains(attr.tag)))
  }

  sparkTest("writing a small sorted file as SAM should produce the expected result") {
    val unsortedPath = testFile("unsorted.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.rdd

    val actualSortedPath = tmpFile("sorted.sam")
    ardd.sortReadsByReferencePosition()
      .saveAsSam(actualSortedPath,
        isSorted = true,
        asSingleFile = true)

    checkFiles(testFile("sorted.sam"), actualSortedPath)
  }

  sparkTest("writing unordered sam from unordered sam") {
    val unsortedPath = testFile("unordered.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.rdd

    val actualUnorderedPath = tmpFile("unordered.sam")
    ardd.saveAsSam(actualUnorderedPath,
      isSorted = false,
      asSingleFile = true)

    checkFiles(unsortedPath, actualUnorderedPath)
  }

  sparkTest("writing ordered sam from unordered sam") {
    val unsortedPath = testFile("unordered.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.sortReadsByReferencePosition

    val actualSortedPath = tmpFile("ordered.sam")
    reads.saveAsSam(actualSortedPath,
      isSorted = true,
      asSingleFile = true)

    checkFiles(testFile("ordered.sam"), actualSortedPath)
  }

  def testBQSR(asSam: Boolean, filename: String) {
    val inputPath = testFile("bqsr1.sam")
    val tempFile = Files.createTempDirectory("bqsr1")
    val rRdd = sc.loadAlignments(inputPath)
    rRdd.rdd.cache()
    rRdd.saveAsSam("%s/%s".format(tempFile.toAbsolutePath.toString, filename),
      asType = if (asSam) Some(SAMFormat.SAM) else Some(SAMFormat.BAM),
      asSingleFile = true)
    val rdd2 = sc.loadAlignments("%s/%s".format(tempFile.toAbsolutePath.toString, filename))
    rdd2.rdd.cache()

    val (fsp1, fsf1) = rRdd.flagStat()
    val (fsp2, fsf2) = rdd2.flagStat()

    assert(rRdd.rdd.count === rdd2.rdd.count)
    assert(fsp1 === fsp2)
    assert(fsf1 === fsf2)

    val jrdd = rRdd.rdd.map(r => ((r.getReadName, r.getReadInFragment, r.getReadMapped), r))
      .join(rdd2.rdd.map(r => ((r.getReadName, r.getReadInFragment, r.getReadMapped), r)))
      .cache()

    assert(rRdd.rdd.count === jrdd.count)

    jrdd.map(kv => kv._2)
      .collect
      .foreach(p => {
        val (p1, p2) = p

        assert(p1.getReadInFragment === p2.getReadInFragment)
        assert(p1.getReadName === p2.getReadName)
        assert(p1.getSequence === p2.getSequence)
        assert(p1.getQuality === p2.getQuality)
        assert(p1.getOriginalQuality === p2.getOriginalQuality)
        assert(p1.getReadGroupSampleId === p2.getReadGroupSampleId)
        assert(p1.getReadGroupId === p2.getReadGroupId)
        assert(p1.getFailedVendorQualityChecks === p2.getFailedVendorQualityChecks)
        assert(p1.getBasesTrimmedFromStart === p2.getBasesTrimmedFromStart)
        assert(p1.getBasesTrimmedFromEnd === p2.getBasesTrimmedFromEnd)

        assert(p1.getReadMapped === p2.getReadMapped)
        // note: BQSR1.sam has reads that are unmapped, but where the mapping flags are set
        // that is why we split this check out
        // the SAM spec doesn't say anything particularly meaningful about this, other than
        // that some fields should be disregarded if the read is not mapped
        if (p1.getReadMapped && p2.getReadMapped) {
          assert(p1.getDuplicateRead === p2.getDuplicateRead)
          assert(p1.getReferenceName === p2.getReferenceName)
          assert(p1.getStart === p2.getStart)
          assert(p1.getEnd === p2.getEnd)
          assert(p1.getCigar === p2.getCigar)
          assert(p1.getOriginalCigar === p2.getOriginalCigar)
          assert(p1.getPrimaryAlignment === p2.getPrimaryAlignment)
          assert(p1.getSecondaryAlignment === p2.getSecondaryAlignment)
          assert(p1.getSupplementaryAlignment === p2.getSupplementaryAlignment)
          assert(p1.getReadNegativeStrand === p2.getReadNegativeStrand)
        }

        assert(p1.getReadPaired === p2.getReadPaired)
        // a variety of fields are undefined if the reads are not paired
        if (p1.getReadPaired && p2.getReadPaired) {
          assert(p1.getInsertSize === p2.getInsertSize)
          assert(p1.getProperPair === p2.getProperPair)

          // same caveat about read alignment applies to mates
          assert(p1.getMateMapped === p2.getMateMapped)
          if (p1.getMateMapped && p2.getMateMapped) {
            assert(p1.getMateNegativeStrand === p2.getMateNegativeStrand)
            assert(p1.getMateReferenceName === p2.getMateReferenceName)
            assert(p1.getMateAlignmentStart === p2.getMateAlignmentStart)
          }
        }
      })
  }

  sparkTest("write single sam file back") {
    testBQSR(true, "bqsr1.sam")
  }

  sparkTest("write single bam file back") {
    testBQSR(false, "bqsr1.bam")
  }

  sparkTest("saveAsParquet with save args, sequence dictionary, and read group dictionary") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation()
    reads.saveAsParquet(TestSaveArgs(outputPath))
    val unfilteredReads = sc.loadAlignments(outputPath)
    assert(unfilteredReads.rdd.count === 20)
    val regionToLoad = ReferenceRegion("1", 100000000L, 200000000L)
    val filteredReads = sc.loadParquetAlignments(outputPath,
      optPredicate = Some(regionToLoad.toPredicate))
    assert(filteredReads.rdd.count === 8)
  }

  sparkTest("load parquet to sql, save, re-read from avro") {
    def testMetadata(arRdd: AlignmentRecordDataset) {
      val sequenceRdd = arRdd.addSequence(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.sequences.containsReferenceName("aSequence"))

      val rgDataset = arRdd.addReadGroup(ReadGroup("test", "aRg"))
      assert(rgDataset.readGroups("aRg").sampleId === "test")
    }

    val inputPath = testFile("small.sam")
    val outputPath = tmpLocation()
    val rrdd = sc.loadAlignments(inputPath)
    testMetadata(rrdd)
    val rdd = rrdd.transformDataset(ds => {
      // all reads are on 1, so this is effectively a no-op
      import ds.sqlContext.implicits._
      val df = ds.toDF()
      df.where(df("referenceName") === "1")
        .as[AlignmentRecordProduct]
    })
    testMetadata(rdd)
    assert(rdd.dataset.count === 20)
    assert(rdd.rdd.count === 20)
    rdd.saveAsParquet(outputPath)
    val rdd2 = sc.loadAlignments(outputPath)
    testMetadata(rdd2)
    assert(rdd2.rdd.count === 20)
    assert(rdd2.dataset.count === 20)
    val outputPath2 = tmpLocation()
    rdd.transform(rdd => rdd) // no-op but force to rdd
      .saveAsParquet(outputPath2)
    val rdd3 = sc.loadAlignments(outputPath2)
    assert(rdd3.rdd.count === 20)
    assert(rdd3.dataset.count === 20)
  }

  sparkTest("load from sam, save as partitioned parquet, and re-read from partitioned parquet") {
    def testMetadata(arRdd: AlignmentRecordDataset) {
      val sequenceRdd = arRdd.addSequence(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.sequences.containsReferenceName("aSequence"))

      val rgDataset = arRdd.addReadGroup(ReadGroup("test", "aRg"))
      assert(rgDataset.readGroups("aRg").sampleId === "test")
    }

    val inputPath = testFile("multi_chr.sam")
    val outputPath = tmpLocation()
    val rrdd = sc.loadAlignments(inputPath)
    testMetadata(rrdd)

    rrdd.saveAsPartitionedParquet(outputPath, partitionSize = 1000000)
    val rdd2 = sc.loadPartitionedParquetAlignments(outputPath)
    testMetadata(rdd2)
    assert(rdd2.rdd.count === 3)
    assert(rdd2.dataset.count === 3)

    val rdd3 = sc.loadPartitionedParquetAlignments(outputPath, List(ReferenceRegion("1", 240000000L, 241000000L), ReferenceRegion("2", 189000000L, 190000000L)))
    assert(rdd3.dataset.count === 2)
    assert(rdd3.rdd.count === 2)

    // ADAM-1967: ensure that ReferenceRegion.all works
    assert(sc.loadPartitionedParquetAlignments(outputPath,
      List(ReferenceRegion.all("1"))).dataset.count === 2)

    //test explicit transform to Alignment Record Product
    val rdd = rrdd.transformDataset(ds => {
      import ds.sqlContext.implicits._
      val df = ds.toDF()
      df.as[AlignmentRecordProduct]
    })
    testMetadata(rdd)
    assert(rdd.dataset.count === 3)
    assert(rdd.rdd.count === 3)
  }

  sparkTest("save as SAM format") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".sam")
    reads.save(TestSaveArgs(outputPath))
    assert(new File(outputPath).exists())
  }

  sparkTest("save as sorted SAM format") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".sam")
    reads.save(TestSaveArgs(outputPath), true)
    assert(new File(outputPath).exists())
  }

  sparkTest("save as BAM format") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".bam")
    reads.save(TestSaveArgs(outputPath))
    assert(new File(outputPath).exists())
  }

  sparkTest("save as sorted BAM format") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".bam")
    reads.save(TestSaveArgs(outputPath), true)
    assert(new File(outputPath).exists())
  }

  sparkTest("save as FASTQ format") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".fq")
    reads.save(TestSaveArgs(outputPath))
    assert(new File(outputPath).exists())
  }

  sparkTest("save as ADAM parquet format") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".adam")
    reads.save(TestSaveArgs(outputPath))
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam SAM format") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath, asType = Some(SAMFormat.SAM))
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam SAM format single file") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath,
      asType = Some(SAMFormat.SAM),
      asSingleFile = true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam sorted SAM format single file") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".sam")
    reads.saveAsSam(outputPath,
      asType = Some(SAMFormat.SAM),
      asSingleFile = true,
      isSorted = true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam BAM format") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".bam")
    reads.saveAsSam(outputPath, asType = Some(SAMFormat.BAM))
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam BAM format single file") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".bam")
    reads.saveAsSam(outputPath,
      asType = Some(SAMFormat.BAM),
      asSingleFile = true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam sorted BAM format single file") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".bam")
    reads.saveAsSam(outputPath,
      asType = Some(SAMFormat.BAM),
      asSingleFile = true,
      isSorted = true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, fileName2Opt = None)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq as single file") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, fileName2Opt = None, asSingleFile = true)
    val outputFile = new File(outputPath)
    assert(outputFile.exists() && !outputFile.isDirectory)
  }

  sparkTest("saveAsFastq with original base qualities") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, fileName2Opt = None, outputOriginalBaseQualities = true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq sorted by read name") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, fileName2Opt = None, outputOriginalBaseQualities = false, sort = true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq sorted by read name with original base qualities") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath = tmpLocation(".fq")
    reads.saveAsFastq(outputPath, fileName2Opt = None, outputOriginalBaseQualities = true, sort = true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq paired FASTQ") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath1 = tmpLocation("_1.fq")
    val outputPath2 = tmpLocation("_2.fq")
    reads.saveAsFastq(outputPath1, Some(outputPath2))
    assert(new File(outputPath1).exists())
    assert(new File(outputPath2).exists())
  }

  sparkTest("saveAsPairedFastq") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath1 = tmpLocation("_1.fq")
    val outputPath2 = tmpLocation("_2.fq")
    reads.saveAsPairedFastq(outputPath1, outputPath2)
    assert(new File(outputPath1).exists())
    assert(new File(outputPath2).exists())
  }

  sparkTest("saveAsPairedFastq as single files") {
    val inputPath = testFile("small.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    val outputPath1 = tmpLocation("_1.fq")
    val outputPath2 = tmpLocation("_2.fq")
    reads.saveAsPairedFastq(outputPath1, outputPath2, asSingleFile = true)
    val outputFile1 = new File(outputPath1)
    assert(outputFile1.exists() && !outputFile1.isDirectory())
    val outputFile2 = new File(outputPath2)
    assert(outputFile2.exists() && !outputFile2.isDirectory())
  }

  sparkTest("don't lose any reads when piping as SAM") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)
    val records = ardd.rdd.count

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordDataset = ardd.pipe[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset, SAMInFormatter](Seq("tee", "/dev/null"))

    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
  }

  sparkTest("lose all records when a command times out") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordDataset = ardd.pipe[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset, SAMInFormatter](Seq("sleep", "10"), optTimeout = Some(5))
    val newRecords = pipedRdd.rdd.count
    assert(newRecords === 0)
  }

  sparkTest("lose no records without a timeout") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script reads the reads into a temp file, which is then read to
    // stdout, then we sleep for 10 sec, then we read to stdout again
    val scriptPath = testFile("timeout.py")
    val pipedRdd: AlignmentRecordDataset = ardd.pipe[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset, SAMInFormatter](Seq("python", "$0"),
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(newRecords === (2 * ardd.rdd.count))
  }

  sparkTest("lose some records when a command times out") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script reads the reads into a temp file, which is then read to
    // stdout, then we sleep for 10 sec, then we read to stdout again
    val scriptPath = testFile("timeout.py")
    val pipedRdd: AlignmentRecordDataset = ardd.pipe[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset, SAMInFormatter](Seq("python", "$0"),
      optTimeout = Some(5),
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(newRecords === ardd.rdd.count)
  }

  sparkTest("don't lose any reads when piping as SAM using java pipe") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)
    val records = ardd.rdd.count

    val pipedRdd = ardd.pipe[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset, SAMInFormatter](
      Seq("tee", "/dev/null"),
      (List.empty[String]: java.util.List[String]),
      (Map.empty[String, String]: java.util.Map[String, String]),
      0,
      classOf[SAMInFormatter],
      new AnySAMOutFormatter,
      new SameTypeFunction2)
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
  }

  sparkTest("don't lose any reads when piping as BAM") {
    val reads12Path = testFile("reads12.sam")
    val ardd = sc.loadBam(reads12Path)
    val records = ardd.rdd.count

    implicit val tFormatter = BAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordDataset = ardd.pipe[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset, BAMInFormatter](Seq("tee", "/dev/null"))
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
  }

  sparkTest("don't lose any reads when piping fastq to sam") {
    // write suffixes at end of reads
    sc.hadoopConfiguration.setBoolean(AlignmentRecordDataset.WRITE_SUFFIXES, true)

    val fragmentsPath = testFile("interleaved_fastq_sample1.ifq")
    val ardd = sc.loadFragments(fragmentsPath).toReads
    val records = ardd.rdd.count
    assert(records === 6)
    assert(ardd.dataset.count === 6)
    assert(ardd.dataset.rdd.count === 6)

    implicit val tFormatter = FASTQInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script converts interleaved fastq to unaligned sam
    val scriptPath = testFile("fastq_to_usam.py")

    val pipedRdd: AlignmentRecordDataset = ardd.pipe[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset, FASTQInFormatter](Seq("python", "$0"),
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(records === newRecords)
  }

  sparkTest("can properly set environment variables inside of a pipe") {
    val reads12Path = testFile("reads12.sam")
    val smallPath = testFile("small.sam")
    val scriptPath = testFile("env_test_command.sh")
    val ardd = sc.loadBam(reads12Path)
    val reads12Records = ardd.rdd.count
    val smallRecords = sc.loadBam(smallPath).rdd.count
    val writePath = tmpLocation("reads12.sam")

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val pipedRdd: AlignmentRecordDataset = ardd.pipe[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset, SAMInFormatter](Seq("/bin/bash", scriptPath),
      environment = Map(("INPUT_PATH" -> smallPath),
        ("OUTPUT_PATH" -> writePath)))
    val newRecords = pipedRdd.rdd.count
    assert(smallRecords === newRecords)
  }

  ignore("read vcf from alignment record pipe") {
    val readsPath = testFile("small.1.sam")
    val vcfPath = testFile("small.vcf")
    val scriptPath = testFile("test_command.sh")
    val tempPath = tmpLocation(".sam")
    val ardd = sc.loadBam(readsPath)

    implicit val tFormatter = SAMInFormatter
    implicit val uFormatter = new VCFOutFormatter(sc.hadoopConfiguration)

    val pipedRdd: VariantContextDataset = ardd.pipe[VariantContext, VariantContextProduct, VariantContextDataset, SAMInFormatter](Seq("/bin/bash", "$0", tempPath, "$1"),
      files = Seq(scriptPath, vcfPath))
    val newRecords = pipedRdd.rdd.count
    assert(newRecords === 6)

    val tempBam = sc.loadBam(tempPath)
    assert(tempBam.rdd.count === ardd.rdd.count)
  }

  sparkTest("use broadcast join to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = reads.broadcastRegionJoin(targets)

    assert(jRdd.rdd.count === 5)
    assert(jRdd.dataset.count === 5)
  }

  sparkTest("use broadcast join against to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
    val bcastTargets = sc.loadFeatures(targetsPath).broadcast()

    val jRdd = reads.broadcastRegionJoinAgainst(bcastTargets)

    assert(jRdd.rdd.count === 5)
    assert(jRdd.dataset.count === 5)
  }

  sparkTest("use right outer broadcast join to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = reads.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(jRdd.dataset.count === 6)
    assert(c.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
  }

  sparkTest("use right outer broadcast join against to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val bcastReads = sc.loadAlignments(readsPath).broadcast()
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = targets.rightOuterBroadcastRegionJoinAgainst(bcastReads)

    val c = jRdd.rdd.collect
    assert(jRdd.dataset.count === 6)
    assert(c.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
  }

  sparkTest("use shuffle join with feature spanning partitions") {
    def makeReadAndRegion(partition: Int,
                          contigName: String,
                          start: Long,
                          end: Long): ((ReferenceRegion, Int), AlignmentRecord) = {
      ((ReferenceRegion(contigName, start, end),
        partition),
        AlignmentRecord.newBuilder
        .setReadMapped(true)
        .setReferenceName(contigName)
        .setStart(start)
        .setEnd(end)
        .build)
    }

    val sd = SequenceDictionary(SequenceRecord("chr1", 51L),
      SequenceRecord("chr2", 51L))

    val reads = RDDBoundAlignmentRecordDataset(sc.parallelize(Seq(makeReadAndRegion(0, "chr1", 10L, 20L),
      makeReadAndRegion(1, "chr1", 40L, 50L),
      makeReadAndRegion(1, "chr2", 10L, 20L),
      makeReadAndRegion(1, "chr2", 20L, 30L),
      makeReadAndRegion(2, "chr2", 40L, 50L)))
      .repartitionAndSortWithinPartitions(ManualRegionPartitioner(3))
      .map(_._2),
      sd,
      ReadGroupDictionary.empty,
      Seq.empty,
      Some(Array(
        Some(ReferenceRegion("chr1", 10L, 20L), ReferenceRegion("chr1", 10L, 20L)),
        Some(ReferenceRegion("chr1", 40L, 50L), ReferenceRegion("chr2", 20L, 30L)),
        Some(ReferenceRegion("chr2", 40L, 50L), ReferenceRegion("chr2", 40L, 50L)))))

    val features = FeatureDataset(sc.parallelize(Seq(Feature.newBuilder
      .setReferenceName("chr2")
      .setStart(20L)
      .setEnd(50L)
      .build)), sd, Seq.empty)

    val jRdd = reads.shuffleRegionJoin(features)
    val joined = jRdd.rdd.collect

    assert(jRdd.dataset.count === 2)
    assert(joined.size === 2)
    assert(joined.exists(_._1.getStart == 20L))
    assert(joined.exists(_._1.getStart == 40L))
  }

  sparkTest("use shuffle join to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.shuffleRegionJoin(targets)
    val jRdd0 = reads.shuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    assert(jRdd.rdd.count === 5)
    assert(jRdd.dataset.count === 5)
    assert(jRdd0.rdd.count === 5)

    val joinedReads: AlignmentRecordDataset = jRdd
      .transmute[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset]((rdd: RDD[(AlignmentRecord, Feature)]) => {
        rdd.map(_._1)
      })
    val tempPath = tmpLocation(".sam")
    joinedReads.saveAsSam(tempPath)
    assert(sc.loadAlignments(tempPath).rdd.count === 5)
  }

  sparkTest("use shuffle join with flankSize to pull down reads mapped close to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))

    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.shuffleRegionJoin(targets, flankSize = 20000000L)
    val jRdd0 = reads.shuffleRegionJoin(targets, optPartitions = Some(4), flankSize = 20000000L)

    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    assert(jRdd.rdd.count === 17)
    assert(jRdd.dataset.count === 17)
    assert(jRdd0.rdd.count === 17)
  }

  sparkTest("use right outer shuffle join to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(jRdd.dataset.count === 6)
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
    assert(c0.count(_._1.isDefined) === 5)
  }

  sparkTest("use left outer shuffle join to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(jRdd.dataset.count === 20)
    assert(c.count(_._2.isEmpty) === 15)
    assert(c0.count(_._2.isEmpty) === 15)
    assert(c.count(_._2.isDefined) === 5)
    assert(c0.count(_._2.isDefined) === 5)
  }

  sparkTest("use full outer shuffle join to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = reads.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(jRdd.dataset.count === 21)
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 5)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 5)
  }

  sparkTest("use shuffle join with group by to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = reads.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(jRdd.dataset.count === 5)
    assert(c.size === 5)
    assert(c0.size === 5)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  sparkTest("use right outer shuffle join with group by to pull down reads mapped to targets") {
    val readsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val reads = sc.loadAlignments(readsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = reads.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = reads.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect

    assert(jRdd.dataset.count === 21)
    assert(c.count(_._1.isDefined) === 20)
    assert(c0.count(_._1.isDefined) === 20)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }

  sparkTest("cannot provide empty quality score bins") {
    val reads = sc.loadAlignments(testFile("small.sam"))
    intercept[IllegalArgumentException] {
      reads.binQualityScores(Seq.empty)
    }
  }

  sparkTest("cannot provide bins with a gap") {
    val reads = sc.loadAlignments(testFile("small.sam"))
    intercept[IllegalArgumentException] {
      reads.binQualityScores(Seq(QualityScoreBin(0, 10, 5),
        QualityScoreBin(11, 21, 16)))
    }
  }

  sparkTest("cannot provide overlapping bins") {
    val reads = sc.loadAlignments(testFile("small.sam"))
    intercept[IllegalArgumentException] {
      reads.binQualityScores(Seq(QualityScoreBin(0, 10, 5),
        QualityScoreBin(9, 19, 14)))
    }
  }

  sparkTest("binning quality scores in reads succeeds even if reads have no quality scores") {
    val reads = sc.loadAlignments(testFile("small.sam"))
    val binnedReads = reads.binQualityScores(Seq(QualityScoreBin(0, 20, 10)))
    val numQualities = binnedReads.rdd.flatMap(read => {
      Option(read.getQuality)
    }).flatMap(s => s)
      .count
    assert(numQualities === 0)
  }

  sparkTest("bin quality scores in reads") {
    val reads = sc.loadAlignments(testFile("bqsr1.sam"))
    val binnedReads = reads.binQualityScores(Seq(QualityScoreBin(0, 20, 10),
      QualityScoreBin(20, 40, 30),
      QualityScoreBin(40, 60, 50)))
    val qualityScoreCounts = binnedReads.rdd.flatMap(read => {
      read.getQuality
    }).map(s => s.toInt - 33)
      .countByValue

    assert(qualityScoreCounts(30) === 92899)
    assert(qualityScoreCounts(10) === 7101)
  }

  sparkTest("union two read files together") {
    val reads1 = sc.loadAlignments(testFile("bqsr1.sam"))
    val reads2 = sc.loadAlignments(testFile("small.sam"))
    val union = reads1.union(reads2)
    assert(union.rdd.count === (reads1.rdd.count + reads2.rdd.count))
    // all of the contigs small.sam has are in bqsr1.sam
    assert(union.sequences.size === reads1.sequences.size)
    // small.sam has no read groups
    assert(union.readGroups.size === reads1.readGroups.size)
  }

  sparkTest("test k-mer counter") {
    val smallPath = testFile("small.sam")
    val reads = sc.loadAlignments(smallPath)
    val kmerCounts = reads.countKmers(6)
    assert(kmerCounts.count === 1040)
    assert(kmerCounts.filter(p => p._1 == "CCAAGA" && p._2 == 3).count === 1)
  }

  sparkTest("test dataset based k-mer counter") {
    val smallPath = testFile("small.sam")
    val reads = sc.loadAlignments(smallPath)
    val kmerCounts = reads.countKmersAsDataset(6)
    assert(kmerCounts.count === 1040)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    assert(kmerCounts.toDF().where($"kmer" === "CCAAGA" && $"count" === 3).count === 1)
  }

  sparkTest("transform reads to contig genomic dataset") {
    val reads = sc.loadAlignments(testFile("small.sam"))

    def checkSave(ncRdd: NucleotideContigFragmentDataset) {
      val tempPath = tmpLocation(".fa")
      ncRdd.saveAsFasta(tempPath)

      assert(sc.loadContigFragments(tempPath).rdd.count.toInt === 20)
    }

    val features: NucleotideContigFragmentDataset = reads.transmute[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset](
      (rdd: RDD[AlignmentRecord]) => {
        rdd.map(AlignmentRecordDatasetSuite.ncfFn)
      })

    checkSave(features)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val featuresDs: NucleotideContigFragmentDataset = reads.transmuteDataset[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset](
      (ds: Dataset[AlignmentRecordProduct]) => {
        ds.map(r => {
          NucleotideContigFragmentProduct.fromAvro(
            AlignmentRecordDatasetSuite.ncfFn(r.toAvro))
        })
      })

    checkSave(featuresDs)
  }

  sparkTest("transform reads to coverage genomic dataset") {
    val reads = sc.loadAlignments(testFile("small.sam"))

    def checkSave(coverage: CoverageDataset) {
      val tempPath = tmpLocation(".bed")
      coverage.save(tempPath, false, false)

      assert(sc.loadCoverage(tempPath).rdd.count === 20)
    }

    val coverage: CoverageDataset = reads.transmute[Coverage, Coverage, CoverageDataset](
      (rdd: RDD[AlignmentRecord]) => {
        rdd.map(AlignmentRecordDatasetSuite.covFn)
      })

    checkSave(coverage)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val coverageDs: CoverageDataset = reads.transmuteDataset[Coverage, Coverage, CoverageDataset](
      (ds: Dataset[AlignmentRecordProduct]) => {
        ds.map(r => AlignmentRecordDatasetSuite.covFn(r.toAvro))
      })

    checkSave(coverageDs)
  }

  sparkTest("transform reads to feature genomic dataset") {
    val reads = sc.loadAlignments(testFile("small.sam"))

    def checkSave(features: FeatureDataset) {
      val tempPath = tmpLocation(".bed")
      features.saveAsBed(tempPath)

      assert(sc.loadFeatures(tempPath).rdd.count === 20)
    }

    val features: FeatureDataset = reads.transmute[Feature, FeatureProduct, FeatureDataset](
      (rdd: RDD[AlignmentRecord]) => {
        rdd.map(AlignmentRecordDatasetSuite.featFn)
      })

    checkSave(features)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val featuresDs: FeatureDataset = reads.transmuteDataset[Feature, FeatureProduct, FeatureDataset](
      (ds: Dataset[AlignmentRecordProduct]) => {
        ds.map(r => {
          FeatureProduct.fromAvro(
            AlignmentRecordDatasetSuite.featFn(r.toAvro))
        })
      })

    checkSave(featuresDs)
  }

  sparkTest("transform reads to fragment genomic dataset") {
    val reads = sc.loadAlignments(testFile("small.sam"))

    def checkSave(fragments: FragmentDataset) {
      val tempPath = tmpLocation(".adam")
      fragments.saveAsParquet(tempPath)

      assert(sc.loadFragments(tempPath).rdd.count === 20)
    }

    val fragments: FragmentDataset = reads.transmute[Fragment, FragmentProduct, FragmentDataset](
      (rdd: RDD[AlignmentRecord]) => {
        rdd.map(AlignmentRecordDatasetSuite.fragFn)
      })

    checkSave(fragments)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val fragmentsDs: FragmentDataset = reads.transmuteDataset[Fragment, FragmentProduct, FragmentDataset](
      (ds: Dataset[AlignmentRecordProduct]) => {
        ds.map(r => {
          FragmentProduct.fromAvro(
            AlignmentRecordDatasetSuite.fragFn(r.toAvro))
        })
      })

    checkSave(fragmentsDs)
  }

  sparkTest("transform reads to genotype genomic dataset") {
    val reads = sc.loadAlignments(testFile("small.sam"))

    def checkSave(genotypes: GenotypeDataset) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 20)
    }

    val genotypes: GenotypeDataset = reads.transmute[Genotype, GenotypeProduct, GenotypeDataset](
      (rdd: RDD[AlignmentRecord]) => {
        rdd.map(AlignmentRecordDatasetSuite.genFn)
      })

    checkSave(genotypes)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val genotypesDs: GenotypeDataset = reads.transmuteDataset[Genotype, GenotypeProduct, GenotypeDataset](
      (ds: Dataset[AlignmentRecordProduct]) => {
        ds.map(r => {
          GenotypeProduct.fromAvro(
            AlignmentRecordDatasetSuite.genFn(r.toAvro))
        })
      })

    checkSave(genotypesDs)
  }

  sparkTest("transform reads to variant genomic dataset") {
    val reads = sc.loadAlignments(testFile("small.sam"))

    def checkSave(variants: VariantDataset) {
      val tempPath = tmpLocation(".adam")
      variants.saveAsParquet(tempPath)

      assert(sc.loadVariants(tempPath).rdd.count === 20)
    }

    val variants: VariantDataset = reads.transmute[Variant, VariantProduct, VariantDataset](
      (rdd: RDD[AlignmentRecord]) => {
        rdd.map(AlignmentRecordDatasetSuite.varFn)
      })

    checkSave(variants)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val variantsDs: VariantDataset = reads.transmuteDataset[Variant, VariantProduct, VariantDataset](
      (ds: Dataset[AlignmentRecordProduct]) => {
        ds.map(r => {
          VariantProduct.fromAvro(
            AlignmentRecordDatasetSuite.varFn(r.toAvro))
        })
      })

    checkSave(variantsDs)
  }

  test("cannot have a null processing step ID") {
    intercept[IllegalArgumentException] {
      AlignmentRecordDataset.processingStepToSam(ProcessingStep.newBuilder.build)
    }
  }

  test("convert a processing description to htsjdk") {
    val htsjdkPg = AlignmentRecordDataset.processingStepToSam(
      ProcessingStep.newBuilder()
        .setId("pg")
        .setProgramName("myProgram")
        .setVersion("1")
        .setPreviousId("ppg")
        .setCommandLine("myProgram run")
        .build)
    assert(htsjdkPg.getId === "pg")
    assert(htsjdkPg.getCommandLine === "myProgram run")
    assert(htsjdkPg.getProgramName === "myProgram")
    assert(htsjdkPg.getProgramVersion === "1")
    assert(htsjdkPg.getPreviousProgramGroupId === "ppg")
  }

  sparkTest("GenomicDataset.sort does not fail on unmapped reads") {
    val inputPath = testFile("unmapped.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    assert(reads.rdd.count === 200)

    val sorted = reads.sort(stringency = ValidationStringency.SILENT)
    assert(sorted.rdd.count === 102)
  }

  sparkTest("GenomicDataset.sortLexicographically does not fail on unmapped reads") {
    val inputPath = testFile("unmapped.sam")
    val reads: AlignmentRecordDataset = sc.loadAlignments(inputPath)
    assert(reads.rdd.count === 200)

    val sorted = reads.sortLexicographically(
      stringency = ValidationStringency.SILENT)
    assert(sorted.rdd.count === 102)
  }

  sparkTest("left normalize indels") {
    val reads = Seq(
      AlignmentRecord.newBuilder()
        .setReadMapped(false)
        .build(),
      AlignmentRecord.newBuilder()
        .setReadMapped(true)
        .setSequence("AAAAACCCCCGGGGGTTTTT")
        .setStart(0L)
        .setCigar("10M2D10M")
        .setMismatchingPositions("10^CC10")
        .build(),
      AlignmentRecord.newBuilder()
        .setReadMapped(true)
        .setSequence("AAAAACCCCCGGGGGTTTTT")
        .setStart(0L)
        .setCigar("10M10D10M")
        .setMismatchingPositions("10^ATATATATAT10")
        .build(),
      AlignmentRecord.newBuilder()
        .setSequence("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        .setReadMapped(true)
        .setCigar("29M10D31M")
        .setStart(5L)
        .setMismatchingPositions("29^GGGGGGGGGG10G0G0G0G0G0G0G0G0G0G11")
        .build())

    // obviously, this isn't unaligned, but, we don't use the metadata here
    val rdd = AlignmentRecordDataset.unaligned(sc.parallelize(reads))
      .leftNormalizeIndels()

    val normalized = rdd.rdd.collect

    assert(normalized.size === 4)
    val cigars = normalized.flatMap(r => {
      Option(r.getCigar)
    }).toSet
    assert(cigars("5M2D15M"))
    assert(cigars("10M10D10M"))
    assert(cigars("29M10D31M"))
  }

  sparkTest("running base recalibration with downsampling doesn't drop reads") {
    val readsFilepath = testFile("bqsr1.sam")
    val snpsFilepath = testFile("bqsr1.vcf")

    val rdd = sc.loadAlignments(readsFilepath)
    val variants = sc.loadVariants(snpsFilepath)
    val snps = sc.broadcast(SnpTable(variants))

    val recalReadsNoSampling = rdd.recalibrateBaseQualities(snps)
    val recalReadsWithSampling = rdd.recalibrateBaseQualities(snps,
      optSamplingFraction = Some(0.1))

    assert(recalReadsNoSampling.rdd.count === recalReadsWithSampling.rdd.count)
  }

  sparkTest("filter RDD bound alignments by MAPQ") {
    val alignments = sc.loadAlignments(testFile("small.sam"))
    assert(alignments.filterByMappingQuality(40).rdd.count() === 17)
  }

  sparkTest("filter dataset bound alignments by MAPQ") {
    val alignments = sc.loadAlignments(testFile("small.sam"))
    val alignmentsDs = alignments.transformDataset(ds => ds)
    assert(alignmentsDs.filterByMappingQuality(40).dataset.count() === 17)
  }

  sparkTest("filter RDD bound unaligned alignments") {
    val alignments = sc.loadAlignments(testFile("unmapped.sam"))
    assert(alignments.filterUnalignedReads().rdd.count() === 102)
  }

  sparkTest("filter dataset bound unaligned alignments") {
    val alignments = sc.loadAlignments(testFile("unmapped.sam"))
    val alignmentsDs = alignments.transformDataset(ds => ds)
    assert(alignmentsDs.filterUnalignedReads().dataset.count() === 102)
  }

  sparkTest("filter RDD bound unpaired alignments") {
    val pairedAlignments = sc.loadAlignments(testFile("NA12878.sam"))
    assert(pairedAlignments.filterUnpairedReads().rdd.count() === 565)

    val unpairedAlignments = sc.loadAlignments(testFile("small.sam"))
    assert(unpairedAlignments.filterUnpairedReads().rdd.count() === 0)
  }

  sparkTest("filter dataset bound unpaired alignments") {
    val pairedAlignments = sc.loadAlignments(testFile("NA12878.sam"))
    val pairedAlignmentsDs = pairedAlignments.transformDataset(ds => ds)
    assert(pairedAlignmentsDs.filterUnpairedReads().dataset.count() === 565)

    val unpairedAlignments = sc.loadAlignments(testFile("small.sam"))
    val unpairedAlignmentsDs = unpairedAlignments.transformDataset(ds => ds)
    assert(unpairedAlignmentsDs.filterUnpairedReads().dataset.count() === 0)
  }

  sparkTest("filter RDD bound duplicate alignments") {
    val alignments = sc.loadAlignments(testFile("small.sam"))
    assert(alignments.filterDuplicateReads().rdd.count() === 20)
  }

  sparkTest("filter dataset bound duplicate alignments") {
    val alignments = sc.loadAlignments(testFile("small.sam"))
    val alignmentsDs = alignments.transformDataset(ds => ds)
    assert(alignmentsDs.filterDuplicateReads().dataset.count() === 20)
  }

  sparkTest("filter RDD bound alignments to primary alignments") {
    val alignments = sc.loadAlignments(testFile("small.sam"))
    assert(alignments.filterToPrimaryAlignments().rdd.count() === 20)
  }

  sparkTest("filter dataset bound alignments to primary alignments") {
    val alignments = sc.loadAlignments(testFile("small.sam"))
    val alignmentsDs = alignments.transformDataset(ds => ds)
    assert(alignmentsDs.filterToPrimaryAlignments().dataset.count() === 20)
  }

  sparkTest("filter RDD bound alignments to read group") {
    val alignments = sc.loadAlignments(testFile("NA12878.sam"))
    assert(alignments.filterToReadGroup("20FUK.1").rdd.count() === 31)
  }

  sparkTest("filter dataset bound alignments to read group") {
    val alignments = sc.loadAlignments(testFile("NA12878.sam"))
    val alignmentsDs = alignments.transformDataset(ds => ds)
    assert(alignmentsDs.filterToReadGroup("20FUK.1").dataset.count() === 31)
  }

  sparkTest("filter RDD bound alignments to read groups") {
    val alignments = sc.loadAlignments(testFile("NA12878.sam"))
    assert(alignments.filterToReadGroups(Seq("20FUK.1", "20FUK.2")).rdd.count() === 62)
  }

  sparkTest("filter dataset bound alignments to read groups") {
    val alignments = sc.loadAlignments(testFile("NA12878.sam"))
    val alignmentsDs = alignments.transformDataset(ds => ds)
    assert(alignmentsDs.filterToReadGroups(Seq("20FUK.1", "20FUK.2")).dataset.count() === 62)
  }

  sparkTest("filter RDD bound alignments to sample") {
    val alignments = sc.loadAlignments(testFile("NA12878.sam"))
    assert(alignments.filterToSample("NA12878").rdd.count() === 565)
    assert(alignments.filterToSample("not a sample").rdd.count() === 0)
  }

  sparkTest("filter dataset bound alignments to sample") {
    val alignments = sc.loadAlignments(testFile("NA12878.sam"))
    val alignmentsDs = alignments.transformDataset(ds => ds)
    assert(alignmentsDs.filterToSample("NA12878").dataset.count() === 565)
    assert(alignmentsDs.filterToSample("not a sample").dataset.count() === 0)
  }

  sparkTest("filter RDD bound alignments to samples") {
    val alignments = sc.loadAlignments(testFile("NA12878.sam"))
    assert(alignments.filterToSamples(Seq("NA12878", "not a sample")).rdd.count() === 565)
    assert(alignments.filterToSamples(Seq("not a sample")).rdd.count() === 0)
  }

  sparkTest("filter dataset bound alignments to samples") {
    val alignments = sc.loadAlignments(testFile("NA12878.sam"))
    val alignmentsDs = alignments.transformDataset(ds => ds)
    assert(alignmentsDs.filterToSamples(Seq("NA12878", "not a sample")).dataset.count() === 565)
    assert(alignmentsDs.filterToSamples(Seq("not a sample")).dataset.count() === 0)
  }

  sparkTest("sort by read name") {
    val unsortedPath = testFile("unsorted.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.rdd

    val actualSortedPath = tmpFile("readname_sorted.sam")
    ardd.sortReadsByReadName()
      .saveAsSam(actualSortedPath,
        asType = None,
        asSingleFile = true,
        sortOrder = SAMFileHeader.SortOrder.queryname,
        deferMerging = false,
        disableFastConcat = false)

    checkFiles(testFile("readname_sorted.sam"), actualSortedPath)
  }

  sparkTest("transform dataset via java API") {
    val reads = sc.loadBam(testFile("small.sam")).sort()

    val transformed = reads.transformDataset(new JFunction[Dataset[AlignmentRecordProduct], Dataset[AlignmentRecordProduct]]() {
      override def call(ds: Dataset[AlignmentRecordProduct]): Dataset[AlignmentRecordProduct] = {
        ds
      }
    })

    assert(reads.dataset.first().start.get === transformed.dataset.first().start.get)
  }
}
