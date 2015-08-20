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

import java.nio.file.Files
import htsjdk.samtools.ValidationStringency
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import scala.io.Source
import scala.util.Random

class AlignmentRecordRDDFunctionsSuite extends ADAMFunSuite {

  sparkTest("sorting reads") {
    val random = new Random("sorting".hashCode)
    val numReadsToCreate = 1000
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = AlignmentRecord.newBuilder().setReadMapped(mapped)
      if (mapped) {
        val contig = Contig.newBuilder
          .setContigName(random.nextInt(numReadsToCreate / 10).toString)
          .build
        val start = random.nextInt(1000000)
        builder.setContig(contig).setStart(start).setEnd(start)
      }
      builder.setReadName((0 until 20).map(i => (random.nextInt(100) + 64)).mkString)
      builder.build()
    }
    val rdd = sc.parallelize(reads)
    val sortedReads = rdd.adamSortReadsByReferencePosition().collect().zipWithIndex
    val (mapped, unmapped) = sortedReads.partition(_._1.getReadMapped)
    // Make sure that all the unmapped reads are placed at the end
    assert(unmapped.forall(p => p._2 > mapped.takeRight(1)(0)._2))
    // Make sure that we appropriately sorted the reads
    val expectedSortedReads = mapped.sortWith(
      (a, b) => a._1.getContig.getContigName.toString < b._1.getContig.getContigName.toString && a._1.getStart < b._1.getStart)
    assert(expectedSortedReads === mapped)
  }

  sparkTest("characterizeTags counts integer tag values correctly") {
    val tagCounts: Map[String, Long] = Map("XT" -> 10L, "XU" -> 9L, "XV" -> 8L)
    val readItr: Iterable[AlignmentRecord] =
      for ((tagName, tagCount) <- tagCounts; i <- 0 until tagCount.toInt)
        yield AlignmentRecord.newBuilder().setAttributes("%s:i:%d".format(tagName, i)).build()

    val reads = sc.parallelize(readItr.toSeq)
    val mapCounts: Map[String, Long] = Map(reads.adamCharacterizeTags().collect(): _*)

    assert(mapCounts === tagCounts)
  }

  sparkTest("withTag returns only those records which have the appropriate tag") {
    val r1 = AlignmentRecord.newBuilder().setAttributes("XX:i:3").build()
    val r2 = AlignmentRecord.newBuilder().setAttributes("XX:i:4\tYY:i:10").build()
    val r3 = AlignmentRecord.newBuilder().setAttributes("YY:i:20").build()

    val rdd = sc.parallelize(Seq(r1, r2, r3))
    assert(rdd.count() === 3)

    val rddXX = rdd.adamFilterRecordsWithTag("XX")
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

    val rddXX = rdd.adamFilterRecordsWithTag("ZZ")
    assert(rddXX.count() === 0)
  }

  sparkTest("characterizeTagValues counts distinct values of a tag") {
    val r1 = AlignmentRecord.newBuilder().setAttributes("XX:i:3").build()
    val r2 = AlignmentRecord.newBuilder().setAttributes("XX:i:4\tYY:i:10").build()
    val r3 = AlignmentRecord.newBuilder().setAttributes("YY:i:20").build()
    val r4 = AlignmentRecord.newBuilder().setAttributes("XX:i:4").build()

    val rdd = sc.parallelize(Seq(r1, r2, r3, r4))
    val tagValues = rdd.adamCharacterizeTagValues("XX")

    assert(tagValues.keys.size === 2)
    assert(tagValues(4) === 2)
    assert(tagValues(3) === 1)
  }

  sparkTest("characterizeTags counts tags in a SAM file correctly") {
    val filePath = getClass.getClassLoader.getResource("reads12.sam").getFile
    val sam: RDD[AlignmentRecord] = sc.loadAlignments(filePath)

    val mapCounts: Map[String, Long] = Map(sam.adamCharacterizeTags().collect(): _*)
    assert(mapCounts("NM") === 200)
    assert(mapCounts("AS") === 200)
    assert(mapCounts("XS") === 200)
  }

  sparkTest("round trip from ADAM to SAM and back to ADAM produces equivalent Read values") {
    val reads12Path = Thread.currentThread().getContextClassLoader.getResource("reads12.sam").getFile
    val rdd12A: RDD[AlignmentRecord] = sc.loadAlignments(reads12Path)

    val tempFile = Files.createTempDirectory("reads12")
    rdd12A.adamSAMSave(tempFile.toAbsolutePath.toString + "/reads12.sam", asSam = true)

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

  sparkTest("round trip from ADAM to FASTQ and back to ADAM produces equivalent Read values") {
    val reads12Path = Thread.currentThread().getContextClassLoader.getResource("fastq_sample1.fq").getFile
    val rdd12A: RDD[AlignmentRecord] = sc.loadAlignments(reads12Path)

    val tempFile = Files.createTempDirectory("reads12")
    rdd12A.adamSaveAsFastq(tempFile.toAbsolutePath.toString + "/reads12.fq")

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
    val rddA = sc.loadAlignments(path1).adamRePairReads(sc.loadAlignments(path2),
      validationStringency = ValidationStringency.STRICT)

    assert(rddA.count() == 6)

    val tempFile = Files.createTempDirectory("reads")
    val tempPath1 = tempFile.toAbsolutePath.toString + "/reads1.fq"
    val tempPath2 = tempFile.toAbsolutePath.toString + "/reads2.fq"

    rddA.adamSaveAsPairedFastq(tempPath1, tempPath2, validationStringency = ValidationStringency.STRICT)

    val rddB: RDD[AlignmentRecord] = sc.loadAlignments(tempPath1).adamRePairReads(sc.loadAlignments(tempPath2),
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
    val reads = sc.loadBam(unsortedPath)

    val actualSortedPath = tmpFile("sorted.sam")
    reads.adamSortReadsByReferencePosition().adamSAMSave(actualSortedPath, isSorted = true, asSingleFile = true)

    checkFiles(resourcePath("sorted.sam"), actualSortedPath)
  }

  sparkTest("writing unordered sam from unordered sam") {
    val unsortedPath = resourcePath("unordered.sam")
    val reads = sc.loadBam(unsortedPath)

    val actualUnorderedPath = tmpFile("unordered.sam")
    reads.adamSAMSave(actualUnorderedPath, isSorted = false, asSingleFile = true)

    checkFiles(unsortedPath, actualUnorderedPath)
  }

  sparkTest("writing ordered sam from unordered sam") {
    val unsortedPath = resourcePath("unordered.sam")
    val reads = sc.loadBam(unsortedPath).adamSortReadsByReferencePosition

    val actualSortedPath = tmpFile("ordered.sam")
    reads.adamSAMSave(actualSortedPath, isSorted = true, asSingleFile = true)

    checkFiles(resourcePath("ordered.sam"), actualSortedPath)
  }

  val chr1 = Contig.newBuilder().setContigName("chr1").build()
  val chr2 = Contig.newBuilder().setContigName("chr2").build()

  def makeFrags(frags: (Contig, Int, String)*): RDD[NucleotideContigFragment] =
    sc.parallelize(
      for {
        (contig, start, seq) <- frags
      } yield NucleotideContigFragment.newBuilder().setContig(contig).setFragmentStartPosition(start.toLong).setFragmentSequence(seq).build()
    )

  def makeReads(reads: (Contig, Int, Int, String, String)*): RDD[AlignmentRecord] =
    sc.parallelize(
      for {
        (contig, start, end, seq, cigar) <- reads
      } yield AlignmentRecord
        .newBuilder
        .setContig(contig)
        .setStart(start.toLong)
        .setEnd(end.toLong)
        .setSequence(seq)
        .setCigar(cigar)
        .setReadMapped(true)
        .build()
    )

  sparkTest("test adding MDTags over boundary") {

    val frags = makeFrags(
      (chr1, 0, "TTTTTTTTTT"),
      (chr1, 10, "A")
    )

    val reads = makeReads(
      (chr1, 9, 11, "TA", "2M"),
      (chr1, 9, 11, "TG", "2M"),
      (chr1, 9, 11, "GA", "2M"),
      (chr1, 9, 11, "TAA", "2M1I"),
      (chr1, 9, 11, "A", "1D1M"),
      (chr1, 9, 11, "G", "1D1M")
    )

    val taggedReads = reads.adamAddMDTags(frags).collect.map(_.getMismatchingPositions)
    assert(taggedReads.length === 6)
    assert(taggedReads(0) === "2")
    assert(taggedReads(1) === "1A0")
    assert(taggedReads(2) === "0T1")
    assert(taggedReads(3) === "2")
    assert(taggedReads(4) === "0^T1")
    assert(taggedReads(5) === "0^T0A0")
  }

  sparkTest("test adding MDTags; reads span full contig") {
    val frags = makeFrags(
      (chr1, 0, "AAAAAAAAAA"),
      (chr1, 10, "CCCCCCCCCC"),
      (chr1, 20, "GGGGGGGG")
    )

    val reads = makeReads(
      // Full reference contig
      (chr1, 0, 28, "AAAAAAAAAACCCCCCCCCCGGGGGGGG", "28M"),
      (chr1, 0, 28, "TAAAAAAAAACCCCCCCCCCGGGGGGGG", "28M"),
      (chr1, 0, 28, "AAAAAAAAAACCCCCCCCCCGGGGGGGT", "28M"),
      (chr1, 0, 28, "AAAAAAAAATTCCCCCCCCCGGGGGGGG", "28M"),
      (chr1, 0, 28, "AAAAAAAAAACCCCCCCCCTTGGGGGGG", "28M")
    )

    val taggedReads = reads.adamAddMDTags(frags).collect.map(_.getMismatchingPositions)
    assert(taggedReads.length === 5)

    assert(taggedReads(0) === "28")
    assert(taggedReads(1) === "0A27")
    assert(taggedReads(2) === "27G0")
    assert(taggedReads(3) === "9A0C17")
    assert(taggedReads(4) === "19C0G7")
  }

  sparkTest("test adding MDTags; reads start inside first fragment") {
    val frags = makeFrags(
      (chr1, 0, "AAAAAAAAAA"),
      (chr1, 10, "CCCCCCCCCC"),
      (chr1, 20, "GGGGGGGG")
    )

    val reads = makeReads(
      // Start inside the first fragment
      (chr1, 1, 28, "AAAAAAAAACCCCCCCCCCGGGGGGGG", "27M"),
      (chr1, 1, 28, "TAAAAAAAACCCCCCCCCCGGGGGGGG", "27M"),
      (chr1, 1, 28, "AAAAAAAAACCCCCCCCCCGGGGGGGT", "27M"),
      (chr1, 1, 28, "AAAAAAAATTCCCCCCCCCGGGGGGGG", "27M"),
      (chr1, 1, 28, "AAAAAAAAACCCCCCCCCTTGGGGGGG", "27M")
    )

    val taggedReads = reads.adamAddMDTags(frags).collect.map(_.getMismatchingPositions)
    assert(taggedReads.length === 5)

    assert(taggedReads(0) === "27")
    assert(taggedReads(1) === "0A26")
    assert(taggedReads(2) === "26G0")
    assert(taggedReads(3) === "8A0C17")
    assert(taggedReads(4) === "18C0G7")
  }

  sparkTest("test adding MDTags; reads end inside last fragment") {
    val frags = makeFrags(
      (chr1, 0, "AAAAAAAAAA"),
      (chr1, 10, "CCCCCCCCCC"),
      (chr1, 20, "GGGGGGGG")
    )

    val reads = makeReads(
      // End inside the last fragment
      (chr1, 0, 27, "AAAAAAAAAACCCCCCCCCCGGGGGGG", "27M"),
      (chr1, 0, 27, "TAAAAAAAAACCCCCCCCCCGGGGGGG", "27M"),
      (chr1, 0, 27, "AAAAAAAAAACCCCCCCCCCGGGGGGT", "27M"),
      (chr1, 0, 27, "AAAAAAAAATTCCCCCCCCCGGGGGGG", "27M"),
      (chr1, 0, 27, "AAAAAAAAAACCCCCCCCCTTGGGGGG", "27M")
    )

    val taggedReads = reads.adamAddMDTags(frags).collect.map(_.getMismatchingPositions)
    assert(taggedReads.length === 5)

    assert(taggedReads(0) === "27")
    assert(taggedReads(1) === "0A26")
    assert(taggedReads(2) === "26G0")
    assert(taggedReads(3) === "9A0C16")
    assert(taggedReads(4) === "19C0G6")
  }

  sparkTest("test adding MDTags; reads start inside first fragment and end inside last fragment") {
    val frags = makeFrags(
      (chr1, 0, "AAAAAAAAAA"),
      (chr1, 10, "CCCCCCCCCC"),
      (chr1, 20, "GGGGGGGG")
    )

    val reads = makeReads(
      (chr1, 1, 27, "AAAAAAAAACCCCCCCCCCGGGGGGG", "26M"),
      (chr1, 1, 27, "TAAAAAAAACCCCCCCCCCGGGGGGG", "26M"),
      (chr1, 1, 27, "AAAAAAAAACCCCCCCCCCGGGGGGT", "26M"),
      (chr1, 1, 27, "AAAAAAAATTCCCCCCCCCGGGGGGG", "26M"),
      (chr1, 1, 27, "AAAAAAAAACCCCCCCCCTTGGGGGG", "26M")
    )

    val taggedReads = reads.adamAddMDTags(frags).collect.map(_.getMismatchingPositions)
    assert(taggedReads.length === 5)

    assert(taggedReads(0) === "26")
    assert(taggedReads(1) === "0A25")
    assert(taggedReads(2) === "25G0")
    assert(taggedReads(3) === "8A0C16")
    assert(taggedReads(4) === "18C0G6")
  }

  sparkTest("test adding MDTags; reads start and end in middle fragements") {
    val frags = makeFrags(
      (chr1, 0, "TTTTTTTTTT"),
      (chr1, 10, "AAAAAAAAAA"),
      (chr1, 20, "CCCCCCCCCC"),
      (chr1, 30, "GGGGGGGGGG"),
      (chr1, 40, "TTTTTTTTTT")
    )

    val reads = makeReads(
      (chr1, 15, 35, "AAAAACCCCCCCCCCGGGGG", "20M"),
      (chr1, 15, 35, "TAAAACCCCCCCCCCGGGGG", "20M"),
      (chr1, 15, 35, "AAAAACCCCCCCCCCGGGGT", "20M"),
      (chr1, 15, 35, "AAAATTCCCCCCCCCGGGGG", "20M"),
      (chr1, 15, 35, "AAAAACCCCCCCCCTTGGGG", "20M")
    )

    val taggedReads = reads.adamAddMDTags(frags).collect.map(_.getMismatchingPositions)
    assert(taggedReads.length === 5)

    assert(taggedReads(0) === "20")
    assert(taggedReads(1) === "0A19")
    assert(taggedReads(2) === "19G0")
    assert(taggedReads(3) === "4A0C14")
    assert(taggedReads(4) === "14C0G4")
  }
}
