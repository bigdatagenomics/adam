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
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
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

    val rdd12B: RDD[AlignmentRecord] = sc.adamBamLoad(tempFile.toAbsolutePath.toString + "/reads12.sam/part-r-00000")

    assert(rdd12B.count() === rdd12A.count())

    val reads12A = rdd12A.collect()
    val reads12B = rdd12B.collect()

    (0 until reads12A.length) foreach {
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

    (0 until reads12A.length) foreach {
      case i: Int =>
        val (readA, readB) = (reads12A(i), reads12B(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getReadName === readB.getReadName)
    }
  }

  sparkTest("round trip from ADAM to paired-FASTQ and back to ADAM produces equivalent Read values") {
    val path1 = ClassLoader.getSystemClassLoader.getResource("proper_pairs_1.fq").getFile
    val path2 = ClassLoader.getSystemClassLoader.getResource("proper_pairs_2.fq").getFile
    val rddA = sc.loadAlignments(path1).adamRePairReads(sc.loadAlignments(path2),
      validationStringency = ValidationStringency.STRICT)

    assert(rddA.count() == 6)

    rddA.foreach(read => {
      if (read.getFirstOfPair == read.getSecondOfPair)
        throw new Exception(
          "Exactly one of first-,second-of-pair should be true for %s: %s %s".format(
            read.toString,
            read.getFirstOfPair,
            read.getSecondOfPair
          )
        )
    })

    val tempFile = Files.createTempDirectory("reads")
    val tempPath1 = tempFile.toAbsolutePath.toString + "/reads1.fq"
    val tempPath2 = tempFile.toAbsolutePath.toString + "/reads2.fq"

    rddA.adamSaveAsPairedFastq(tempPath1, tempPath2, validationStringency = ValidationStringency.STRICT)

    val rddB: RDD[AlignmentRecord] = sc.loadAlignments(tempPath1).adamRePairReads(sc.loadAlignments(tempPath2),
      validationStringency = ValidationStringency.STRICT)

    assert(rddB.count() === rddA.count())

    val readsA = rddA.collect()
    val readsB = rddB.collect()

    (0 until readsA.length) foreach {
      case i: Int =>
        val (readA, readB) = (readsA(i), readsB(i))
        assert(readA.getSequence === readB.getSequence)
        assert(readA.getQual === readB.getQual)
        assert(readA.getReadName === readB.getReadName)
    }
  }
}
