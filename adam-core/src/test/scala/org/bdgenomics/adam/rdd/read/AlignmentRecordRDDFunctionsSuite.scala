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
import htsjdk.samtools.ValidationStringency
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
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

class AlignmentRecordRDDFunctionsSuite extends ADAMFunSuite {

  sparkTest("sorting reads") {
    val random = new Random("sorting".hashCode)
    val numReadsToCreate = 1000
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = AlignmentRecord.newBuilder().setReadMapped(mapped)
      if (mapped) {
        val contigName = random.nextInt(numReadsToCreate / 10).toString
        val start = random.nextInt(1000000)
        builder.setContigName(contigName).setStart(start).setEnd(start)
      }
      builder.setReadName((0 until 20).map(i => (random.nextInt(100) + 64)).mkString)
      builder.build()
    }
    val rdd = sc.parallelize(reads)
    val sortedReads = rdd.sortReadsByReferencePosition().collect().zipWithIndex
    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)
    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))
    // Make sure that we appropriately sorted the reads
    val expectedSortedReads = mapped.sortWith(
      (a, b) => a._1.getContigName < b._1.getContigName && a._1.getStart < b._1.getStart)
    assert(expectedSortedReads === mapped)
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
        builder.setContigName(contigName).setStart(start).setEnd(start)
      }
      builder.setReadName((0 until 20).map(i => (random.nextInt(100) + 64)).mkString)
      builder.build()
    }
    val contigNames = reads.filter(_.getReadMapped).map(_.getContigName).toSet
    val sd = new SequenceDictionary(contigNames.toSeq
      .zipWithIndex
      .map(kv => {
        val (name, index) = kv
        SequenceRecord(name, Int.MaxValue, referenceIndex = Some(index))
      }).toVector)

    val rdd = sc.parallelize(reads)
    val sortedReads = rdd.sortReadsByReferencePositionAndIndex(sd).collect().zipWithIndex
    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)

    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))

    def toIndex(r: AlignmentRecord): Int = {
      sd(r.getContigName).get.referenceIndex.get
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

  sparkTest("characterizeTags counts integer tag values correctly") {
    val tagCounts: Map[String, Long] = Map("XT" -> 10L, "XU" -> 9L, "XV" -> 8L)
    val readItr: Iterable[AlignmentRecord] =
      for ((tagName, tagCount) <- tagCounts; i <- 0 until tagCount.toInt)
        yield AlignmentRecord.newBuilder().setAttributes("%s:i:%d".format(tagName, i)).build()

    val reads = sc.parallelize(readItr.toSeq)
    val mapCounts: Map[String, Long] = Map(reads.characterizeTags().collect(): _*)

    assert(mapCounts === tagCounts)
  }

  sparkTest("withTag returns only those records which have the appropriate tag") {
    val r1 = AlignmentRecord.newBuilder().setAttributes("XX:i:3").build()
    val r2 = AlignmentRecord.newBuilder().setAttributes("XX:i:4\tYY:i:10").build()
    val r3 = AlignmentRecord.newBuilder().setAttributes("YY:i:20").build()

    val rdd = sc.parallelize(Seq(r1, r2, r3))
    assert(rdd.count() === 3)

    val rddXX = rdd.filterRecordsWithTag("XX")
    assert(rddXX.count() === 2)

    val collected = rddXX.collect()
    assert(collected.contains(r1))
    assert(collected.contains(r2))
  }

  sparkTest("withTag, when given a tag name that doesn't exist in the input, returns an empty RDD") {
    val r1 = AlignmentRecord.newBuilder().setAttributes("XX:i:3").build()
    val r2 = AlignmentRecord.newBuilder().setAttributes("XX:i:4\tYY:i:10").build()
    val r3 = AlignmentRecord.newBuilder().setAttributes("YY:i:20").build()

    val rdd = sc.parallelize(Seq(r1, r2, r3))
    assert(rdd.count() === 3)

    val rddXX = rdd.filterRecordsWithTag("ZZ")
    assert(rddXX.count() === 0)
  }

  sparkTest("characterizeTagValues counts distinct values of a tag") {
    val r1 = AlignmentRecord.newBuilder().setAttributes("XX:i:3").build()
    val r2 = AlignmentRecord.newBuilder().setAttributes("XX:i:4\tYY:i:10").build()
    val r3 = AlignmentRecord.newBuilder().setAttributes("YY:i:20").build()
    val r4 = AlignmentRecord.newBuilder().setAttributes("XX:i:4").build()

    val rdd = sc.parallelize(Seq(r1, r2, r3, r4))
    val tagValues = rdd.characterizeTagValues("XX")

    assert(tagValues.keys.size === 2)
    assert(tagValues(4) === 2)
    assert(tagValues(3) === 1)
  }

  sparkTest("characterizeTags counts tags in a SAM file correctly") {
    val filePath = getClass.getClassLoader.getResource("reads12.sam").getFile
    val sam: RDD[AlignmentRecord] = sc.loadAlignments(filePath)

    val mapCounts: Map[String, Long] = Map(sam.characterizeTags().collect(): _*)
    assert(mapCounts("NM") === 200)
    assert(mapCounts("AS") === 200)
    assert(mapCounts("XS") === 200)
  }

  sparkTest("round trip from ADAM to SAM and back to ADAM produces equivalent Read values") {
    val reads12Path = Thread.currentThread().getContextClassLoader.getResource("reads12.sam").getFile
    val ardd = sc.loadBam(reads12Path)
    val rdd12A = ardd.rdd
    val sd = ardd.sequences
    val rgd = ardd.recordGroups

    val tempFile = Files.createTempDirectory("reads12")
    rdd12A.saveAsSam(tempFile.toAbsolutePath.toString + "/reads12.sam",
      sd,
      rgd,
      asSam = true)

    val rdd12B: RDD[AlignmentRecord] = sc.loadBam(tempFile.toAbsolutePath.toString + "/reads12.sam/part-r-00000")

    assert(rdd12B.count() === rdd12A.count())

    val reads12A = rdd12A.collect()
    val reads12B = rdd12B.collect()

    reads12A.indices.foreach {
      case i: Int =>
        val (readA, readB) = (reads12A(i), reads12B(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getCigar === readB.getCigar)
    }
  }

  sparkTest("SAM conversion sets read mapped flag properly") {
    val filePath = getClass.getClassLoader.getResource("reads12.sam").getFile
    val sam: RDD[AlignmentRecord] = sc.loadAlignments(filePath)

    sam.collect().foreach(r => assert(r.getReadMapped))
  }

  sparkTest("convert malformed FASTQ (no quality scores) => SAM => well-formed FASTQ => SAM") {
    val noqualPath = Thread.currentThread().getContextClassLoader.getResource("fastq_noqual.fq").getFile
    val tempBase = Files.createTempDirectory("noqual").toAbsolutePath.toString

    //read FASTQ (malformed)
    val rddA: RDD[AlignmentRecord] = sc.loadFastq(noqualPath, None, None, ValidationStringency.LENIENT)

    //write SAM (fixed and now well-formed)
    rddA.saveAsSam(tempBase + "/noqualA.sam",
      SequenceDictionary.empty,
      RecordGroupDictionary.empty)

    //read SAM
    val rddB: RDD[AlignmentRecord] = sc.loadAlignments(tempBase + "/noqualA.sam")

    //write FASTQ (well-formed)
    rddB.saveAsFastq(tempBase + "/noqualB.fastq")

    //read FASTQ (well-formed)
    val rddC: RDD[AlignmentRecord] = sc.loadFastq(tempBase + "/noqualB.fastq", None, None, ValidationStringency.STRICT)

    val noqualA = rddA.collect()
    val noqualB = rddB.collect()
    val noqualC = rddC.collect()
    noqualA.indices.foreach {
      case i: Int =>
        val (readA, readB, readC) = (noqualA(i), noqualB(i), noqualC(i))
        assert(readA.getQual != "*")
        assert(readB.getQual == "B" * readB.getSequence.length)
        assert(readB.getQual == readC.getQual)
    }

  }

  sparkTest("round trip from ADAM to FASTQ and back to ADAM produces equivalent Read values") {
    val reads12Path = Thread.currentThread().getContextClassLoader.getResource("fastq_sample1.fq").getFile
    val rdd12A: RDD[AlignmentRecord] = sc.loadAlignments(reads12Path)

    val tempFile = Files.createTempDirectory("reads12")
    rdd12A.saveAsFastq(tempFile.toAbsolutePath.toString + "/reads12.fq")

    val rdd12B: RDD[AlignmentRecord] = sc.loadAlignments(tempFile.toAbsolutePath.toString + "/reads12.fq")

    assert(rdd12B.count() === rdd12A.count())

    val reads12A = rdd12A.collect()
    val reads12B = rdd12B.collect()

    reads12A.indices.foreach {
      case i: Int =>
        val (readA, readB) = (reads12A(i), reads12B(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getReadName === readB.getReadName)
    }
  }

  sparkTest("round trip from ADAM to paired-FASTQ and back to ADAM produces equivalent Read values") {
    val path1 = resourcePath("proper_pairs_1.fq")
    val path2 = resourcePath("proper_pairs_2.fq")
    val rddA = sc.loadAlignments(path1).rdd.reassembleReadPairs(sc.loadAlignments(path2).rdd,
      validationStringency = ValidationStringency.STRICT)

    assert(rddA.count() == 6)

    val tempFile = Files.createTempDirectory("reads")
    val tempPath1 = tempFile.toAbsolutePath.toString + "/reads1.fq"
    val tempPath2 = tempFile.toAbsolutePath.toString + "/reads2.fq"

    rddA.saveAsPairedFastq(tempPath1, tempPath2, validationStringency = ValidationStringency.STRICT)

    val rddB: RDD[AlignmentRecord] = sc.loadAlignments(tempPath1).rdd.reassembleReadPairs(sc.loadAlignments(tempPath2).rdd,
      validationStringency = ValidationStringency.STRICT)

    assert(rddB.count() === rddA.count())

    val readsA = rddA.collect()
    val readsB = rddB.collect()

    readsA.indices.foreach {
      case i: Int =>
        val (readA, readB) = (readsA(i), readsB(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getReadName === readB.getReadName)
    }
  }

  sparkTest("writing a small sorted file as SAM should produce the expected result") {
    val unsortedPath = resourcePath("unsorted.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.rdd
    val sd = ardd.sequences
    val rgd = ardd.recordGroups

    val actualSortedPath = tmpFile("sorted.sam")
    reads.sortReadsByReferencePosition()
      .saveAsSam(actualSortedPath,
        sd.stripIndices,
        rgd,
        isSorted = true,
        asSingleFile = true)

    checkFiles(resourcePath("sorted.sam"), actualSortedPath)
  }

  sparkTest("writing unordered sam from unordered sam") {
    val unsortedPath = resourcePath("unordered.sam")
    val ardd = sc.loadBam(unsortedPath)
    val reads = ardd.rdd
    val sd = ardd.sequences
    val rgd = ardd.recordGroups

    val actualUnorderedPath = tmpFile("unordered.sam")
    reads.saveAsSam(actualUnorderedPath,
      sd,
      rgd,
      isSorted = false,
      asSingleFile = true)

    checkFiles(unsortedPath, actualUnorderedPath)
  }

  sparkTest("writing ordered sam from unordered sam") {
    val unsortedPath = resourcePath("unordered.sam")
    val ardd = sc.loadBam(unsortedPath)
    val usReads = ardd.rdd
    val sd = ardd.sequences
    val rgd = ardd.recordGroups
    val reads = usReads.sortReadsByReferencePosition

    val actualSortedPath = tmpFile("ordered.sam")
    reads.saveAsSam(actualSortedPath,
      sd.stripIndices,
      rgd,
      isSorted = true,
      asSingleFile = true)

    checkFiles(resourcePath("ordered.sam"), actualSortedPath)
  }

  def testBQSR(asSam: Boolean, filename: String) {
    val inputPath = resourcePath("bqsr1.sam")
    val tempFile = Files.createTempDirectory("bqsr1")
    val rRdd = sc.loadAlignments(inputPath)
    val rdd = rRdd.rdd
    val sd = rRdd.sequences
    val rgd = rRdd.recordGroups
    rdd.cache()
    rdd.saveAsSam("%s/%s".format(tempFile.toAbsolutePath.toString, filename),
      sd,
      rgd,
      asSam = true,
      asSingleFile = true)
    val rdd2 = sc.loadAlignments("%s/%s".format(tempFile.toAbsolutePath.toString, filename)).rdd
    rdd2.cache()

    val (fsp1, fsf1) = rdd.flagStat()
    val (fsp2, fsf2) = rdd2.flagStat()

    assert(rdd.count === rdd2.count)
    assert(fsp1 === fsp2)
    assert(fsf1 === fsf2)

    val jrdd = rdd.map(r => ((r.getReadName, r.getReadInFragment, r.getReadMapped), r))
      .join(rdd2.map(r => ((r.getReadName, r.getReadInFragment, r.getReadMapped), r)))
      .cache()

    assert(rdd.count === jrdd.count)

    jrdd.map(kv => kv._2)
      .collect
      .foreach(p => {
        val (p1, p2) = p

        assert(p1.getReadInFragment === p2.getReadInFragment)
        assert(p1.getReadName === p2.getReadName)
        assert(p1.getSequence === p2.getSequence)
        assert(p1.getQual === p2.getQual)
        assert(p1.getOrigQual === p2.getOrigQual)
        assert(p1.getRecordGroupSample === p2.getRecordGroupSample)
        assert(p1.getRecordGroupName === p2.getRecordGroupName)
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
          assert(p1.getContigName === p2.getContigName)
          assert(p1.getStart === p2.getStart)
          assert(p1.getEnd === p2.getEnd)
          assert(p1.getCigar === p2.getCigar)
          assert(p1.getOldCigar === p2.getOldCigar)
          assert(p1.getPrimaryAlignment === p2.getPrimaryAlignment)
          assert(p1.getSecondaryAlignment === p2.getSecondaryAlignment)
          assert(p1.getSupplementaryAlignment === p2.getSupplementaryAlignment)
          assert(p1.getReadNegativeStrand === p2.getReadNegativeStrand)
        }

        assert(p1.getReadPaired === p2.getReadPaired)
        // a variety of fields are undefined if the reads are not paired
        if (p1.getReadPaired && p2.getReadPaired) {
          assert(p1.getInferredInsertSize === p2.getInferredInsertSize)
          assert(p1.getProperPair === p2.getProperPair)

          // same caveat about read alignment applies to mates
          assert(p1.getMateMapped === p2.getMateMapped)
          if (p1.getMateMapped && p2.getMateMapped) {
            assert(p1.getMateNegativeStrand === p2.getMateNegativeStrand)
            assert(p1.getMateContigName === p2.getMateContigName)
            assert(p1.getMateAlignmentStart === p2.getMateAlignmentStart)
            assert(p1.getMateAlignmentEnd === p2.getMateAlignmentEnd)
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

  def tempLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("AlignmentRecordRDDFunctionsSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  sparkTest("saveAsParquet with save args, sequence dictionary, and record group dictionary") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation()
    reads.rdd.saveAsParquet(TestSaveArgs(outputPath), reads.sequences, reads.recordGroups)
    assert(new File(outputPath).exists())
  }

  sparkTest("save as SAM format") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".sam")
    reads.rdd.save(TestSaveArgs(outputPath), reads.sequences, reads.recordGroups)
    assert(new File(outputPath).exists())
  }

  sparkTest("save as sorted SAM format") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".sam")
    reads.rdd.save(TestSaveArgs(outputPath), reads.sequences, reads.recordGroups, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("save as BAM format") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".bam")
    reads.rdd.save(TestSaveArgs(outputPath), reads.sequences, reads.recordGroups)
    assert(new File(outputPath).exists())
  }

  sparkTest("save as sorted BAM format") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".bam")
    reads.rdd.save(TestSaveArgs(outputPath), reads.sequences, reads.recordGroups, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("save as FASTQ format") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".fq")
    reads.rdd.save(TestSaveArgs(outputPath), reads.sequences, reads.recordGroups)
    assert(new File(outputPath).exists())
  }

  sparkTest("save as ADAM parquet format") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".adam")
    reads.rdd.save(TestSaveArgs(outputPath), reads.sequences, reads.recordGroups)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam SAM format") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".sam")
    reads.rdd.saveAsSam(outputPath, reads.sequences, reads.recordGroups, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam SAM format single file") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".sam")
    reads.rdd.saveAsSam(outputPath, reads.sequences, reads.recordGroups, true, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam sorted SAM format single file") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".sam")
    reads.rdd.saveAsSam(outputPath, reads.sequences, reads.recordGroups, true, true, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam BAM format") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".bam")
    reads.rdd.saveAsSam(outputPath, reads.sequences, reads.recordGroups, false)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam BAM format single file") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".bam")
    reads.rdd.saveAsSam(outputPath, reads.sequences, reads.recordGroups, false, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsSam sorted BAM format single file") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".bam")
    reads.rdd.saveAsSam(outputPath, reads.sequences, reads.recordGroups, false, true, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".fq")
    reads.rdd.saveAsFastq(outputPath, None)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq with original base qualities") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".fq")
    reads.rdd.saveAsFastq(outputPath, None, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq sorted by read name") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".fq")
    reads.rdd.saveAsFastq(outputPath, None, false, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq sorted by read name with original base qualities") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath = tempLocation(".fq")
    reads.rdd.saveAsFastq(outputPath, None, true, true)
    assert(new File(outputPath).exists())
  }

  sparkTest("saveAsFastq paired FASTQ") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath1 = tempLocation("_1.fq")
    val outputPath2 = tempLocation("_2.fq")
    reads.rdd.saveAsFastq(outputPath1, Some(outputPath2))
    assert(new File(outputPath1).exists())
    assert(new File(outputPath2).exists())
  }

  sparkTest("saveAsPairedFastq") {
    val inputPath = resourcePath("small.sam")
    val reads: AlignmentRecordRDD = sc.loadAlignments(inputPath)
    val outputPath1 = tempLocation("_1.fq")
    val outputPath2 = tempLocation("_2.fq")
    reads.rdd.saveAsPairedFastq(outputPath1, outputPath2)
    assert(new File(outputPath1).exists())
    assert(new File(outputPath2).exists())
  }
}
