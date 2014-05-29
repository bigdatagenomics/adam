/**
 * Copyright 2013-2014. Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.avro.{
  ADAMContig,
  ADAMGenotype,
  ADAMNucleotideContigFragment,
  ADAMPileup,
  ADAMVariant,
  ADAMRecord,
  Base
}
import org.bdgenomics.adam.models.{
  ADAMVariantContext,
  ReferenceRegion
}
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.apache.spark.rdd.RDD
import scala.util.Random
import java.io.File

class ADAMRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("can convert pileups to rods, bases at different pos, same reference") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 2)
    assert(rods.filter(_.position.pos == 0L).count === 1)
    assert(rods.filter(_.position.pos == 0L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos, different reference") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val contig1 = ADAMContig.newBuilder
      .setContigName("chr1")
      .build

    val p0 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setContig(contig1)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 2)
    assert(rods.filter(_.position.referenceName == "chr0").count === 1)
    assert(rods.filter(_.position.referenceName == "chr0").flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.referenceName == "chr1").count === 1)
    assert(rods.filter(_.position.referenceName == "chr1").flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()

    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos, split by different sample") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.A)
      .setRecordGroupSample("0")
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .setRecordGroupSample("1")
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
    assert(rods.filter(_.isSingleSample).count === 0)

    val split = rods.adamSplitRodsBySamples()

    assert(split.count === 2)
    assert(split.filter(_.position.pos == 1L).count === 2)
    assert(split.filter(_.isSingleSample).count === 2)
  }

  sparkTest("can convert pileups to rods, bases at same pos, split by same sample") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.A)
      .setRecordGroupSample("1")
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .setRecordGroupSample("1")
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
    assert(rods.filter(_.isSingleSample).count === 1)

    val split = rods.adamSplitRodsBySamples()

    assert(split.count === 1)
    assert(split.filter(_.isSingleSample).count === 1)
  }

  sparkTest("check coverage, bases at different pos") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = ADAMPileup.newBuilder()
      .setPosition(0L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val coverage = pileups.adamPileupsToRods(1)
      .adamRodCoverage()

    // floating point, so apply tolerance
    assert(coverage > 0.99 && coverage < 1.01)
  }

  sparkTest("check coverage, bases at same pos") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()
    val p1 = ADAMPileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[ADAMPileup] = sc.parallelize(List(p0, p1))

    val coverage = pileups.adamPileupsToRods(1)
      .adamRodCoverage()

    // floating point, so apply tolerance
    assert(coverage > 1.99 && coverage < 2.01)
  }

  sparkTest("sorting reads") {
    val random = new Random("sorting".hashCode)
    val numReadsToCreate = 1000
    val reads = for (i <- 0 until numReadsToCreate) yield {
      val mapped = random.nextBoolean()
      val builder = ADAMRecord.newBuilder().setReadMapped(mapped)
      if (mapped) {
        val contig = ADAMContig.newBuilder
          .setContigName(random.nextInt(numReadsToCreate / 10).toString)
          .build
        builder.setContig(contig).setStart(random.nextInt(1000000))
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

  sparkTest("convert an RDD of reads into rods") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val r0 = ADAMRecord.newBuilder
      .setStart(1L)
      .setContig(contig)
      .setSequence("ACG")
      .setMapq(30)
      .setCigar("3M")
      .setMismatchingPositions("3")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setQual("!#$")
      .build()
    val r1 = ADAMRecord.newBuilder
      .setStart(2L)
      .setContig(contig)
      .setSequence("CG")
      .setMapq(40)
      .setCigar("2M")
      .setMismatchingPositions("2")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setQual("%&")
      .build()
    val r2 = ADAMRecord.newBuilder
      .setStart(3L)
      .setContig(contig)
      .setSequence("G")
      .setMapq(50)
      .setCigar("1M")
      .setMismatchingPositions("1")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setQual("%")
      .build()

    val reads = sc.parallelize(List(r0, r1, r2))

    val rods = reads.adamRecords2Rods()

    assert(rods.count === 3)
    assert(rods.collect.forall(_.position.referenceName == "chr0"))
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).first.pileups.length === 1)
    assert(rods.filter(_.position.pos == 1L).first.pileups.forall(_.getReadBase == Base.A))
    assert(rods.filter(_.position.pos == 2L).count === 1)
    assert(rods.filter(_.position.pos == 2L).first.pileups.length === 2)
    assert(rods.filter(_.position.pos == 2L).first.pileups.forall(_.getReadBase == Base.C))
    assert(rods.filter(_.position.pos == 3L).count === 1)
    assert(rods.filter(_.position.pos == 3L).first.pileups.length === 3)
    assert(rods.filter(_.position.pos == 3L).first.pileups.forall(_.getReadBase == Base.G))
  }

  sparkTest("convert an RDD of reads into rods, different references") {
    val contig0 = ADAMContig.newBuilder
      .setContigName("chr0")
      .build

    val contig1 = ADAMContig.newBuilder
      .setContigName("chr1")
      .build

    val r0 = ADAMRecord.newBuilder
      .setStart(1L)
      .setContig(contig0)
      .setSequence("AC")
      .setMapq(30)
      .setCigar("2M")
      .setMismatchingPositions("2")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setQual("!#$")
      .build()
    val r1 = ADAMRecord.newBuilder
      .setStart(2L)
      .setContig(contig0)
      .setSequence("C")
      .setMapq(40)
      .setCigar("1M")
      .setMismatchingPositions("1")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setQual("%&")
      .build()
    val r2 = ADAMRecord.newBuilder
      .setStart(2L)
      .setContig(contig1)
      .setSequence("G")
      .setMapq(50)
      .setCigar("1M")
      .setMismatchingPositions("1")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setQual("%")
      .build()

    val reads = sc.parallelize(List(r0, r1, r2))

    val rods = reads.adamRecords2Rods()

    assert(rods.count === 3)
    assert(rods.filter(_.position.referenceName == "chr0").count === 2)
    assert(rods.filter(_.position.referenceName == "chr1").count === 1)
    assert(rods.filter(_.position.pos == 1L).filter(_.position.referenceName == "chr0").count === 1)
    assert(rods.filter(_.position.pos == 1L).filter(_.position.referenceName == "chr0").first.pileups.length === 1)
    assert(rods.filter(_.position.pos == 1L).filter(_.position.referenceName == "chr0").first.pileups.forall(_.getReadBase == Base.A))
    assert(rods.filter(_.position.pos == 2L).filter(_.position.referenceName == "chr0").count === 1)
    assert(rods.filter(_.position.pos == 2L).filter(_.position.referenceName == "chr0").first.pileups.length === 2)
    assert(rods.filter(_.position.pos == 2L).filter(_.position.referenceName == "chr0").first.pileups.forall(_.getReadBase == Base.C))
    assert(rods.filter(_.position.pos == 2L).filter(_.position.referenceName == "chr1").count === 1)
    assert(rods.filter(_.position.pos == 2L).filter(_.position.referenceName == "chr1").first.pileups.length === 1)
    assert(rods.filter(_.position.pos == 2L).filter(_.position.referenceName == "chr1").first.pileups.forall(_.getReadBase == Base.G))
  }

  sparkTest("generate sequence dict from fasta") {
    val contig0 = ADAMContig.newBuilder
      .setContigName("chr0")
      .setContigLength(1000L)
      .setReferenceURL("http://bigdatagenomics.github.io/chr0.fa")
      .build

    val contig1 = ADAMContig.newBuilder
      .setContigName("chr1")
      .setContigLength(900L)
      .build

    val ctg0 = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig0)
      .build()
    val ctg1 = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig1)
      .build()

    val rdd = sc.parallelize(List(ctg0, ctg1))

    val dict = rdd.adamGetSequenceDictionary()

    assert(dict.containsRefName("chr0"))
    val chr0 = dict("chr0").get
    assert(chr0.length === 1000L)
    assert(chr0.url == Some("http://bigdatagenomics.github.io/chr0.fa"))
    assert(dict.containsRefName("chr1"))
    val chr1 = dict("chr1").get
    assert(chr1.length === 900L)
  }

  sparkTest("recover samples from variant context") {
    val variant0 = ADAMVariant.newBuilder()
      .setPosition(0L)
      .setVariantAllele("A")
      .setReferenceAllele("T")
      .setContig(ADAMContig.newBuilder.setContigName("chr0").build)
      .build()
    val variant1 = ADAMVariant.newBuilder()
      .setPosition(0L)
      .setVariantAllele("C")
      .setReferenceAllele("T")
      .setContig(ADAMContig.newBuilder.setContigName("chr0").build)
      .build()
    val genotype0 = ADAMGenotype.newBuilder()
      .setVariant(variant0)
      .setSampleId("me")
      .build()
    val genotype1 = ADAMGenotype.newBuilder()
      .setVariant(variant1)
      .setSampleId("you")
      .build()

    val vc = ADAMVariantContext.buildFromGenotypes(List(genotype0, genotype1))
    val samples = sc.parallelize(List(vc)).adamGetCallsetSamples()

    assert(samples.filter(_ == "you").length === 1)
    assert(samples.filter(_ == "me").length === 1)
  }

  sparkTest("characterizeTags counts integer tag values correctly") {
    val tagCounts: Map[String, Long] = Map("XT" -> 10L, "XU" -> 9L, "XV" -> 8L)
    val readItr: Iterable[ADAMRecord] =
      for ((tagName, tagCount) <- tagCounts; i <- 0 until tagCount.toInt)
        yield ADAMRecord.newBuilder().setAttributes("%s:i:%d".format(tagName, i)).build()

    val reads = sc.parallelize(readItr.toSeq)
    val mapCounts: Map[String, Long] = Map(reads.adamCharacterizeTags().collect(): _*)

    assert(mapCounts === tagCounts)
  }

  sparkTest("withTag returns only those records which have the appropriate tag") {
    val r1 = ADAMRecord.newBuilder().setAttributes("XX:i:3").build()
    val r2 = ADAMRecord.newBuilder().setAttributes("XX:i:4\tYY:i:10").build()
    val r3 = ADAMRecord.newBuilder().setAttributes("YY:i:20").build()

    val rdd = sc.parallelize(Seq(r1, r2, r3))
    assert(rdd.count() === 3)

    val rddXX = rdd.adamFilterRecordsWithTag("XX")
    assert(rddXX.count() === 2)

    val collected = rddXX.collect()
    assert(collected.contains(r1))
    assert(collected.contains(r2))
  }

  sparkTest("withTag, when given a tag name that doesn't exist in the input, returns an empty RDD") {
    val r1 = ADAMRecord.newBuilder().setAttributes("XX:i:3").build()
    val r2 = ADAMRecord.newBuilder().setAttributes("XX:i:4\tYY:i:10").build()
    val r3 = ADAMRecord.newBuilder().setAttributes("YY:i:20").build()

    val rdd = sc.parallelize(Seq(r1, r2, r3))
    assert(rdd.count() === 3)

    val rddXX = rdd.adamFilterRecordsWithTag("ZZ")
    assert(rddXX.count() === 0)
  }

  sparkTest("characterizeTagValues counts distinct values of a tag") {
    val r1 = ADAMRecord.newBuilder().setAttributes("XX:i:3").build()
    val r2 = ADAMRecord.newBuilder().setAttributes("XX:i:4\tYY:i:10").build()
    val r3 = ADAMRecord.newBuilder().setAttributes("YY:i:20").build()
    val r4 = ADAMRecord.newBuilder().setAttributes("XX:i:4").build()

    val rdd = sc.parallelize(Seq(r1, r2, r3, r4))
    val tagValues = rdd.adamCharacterizeTagValues("XX")

    assert(tagValues.keys.size === 2)
    assert(tagValues(4) === 2)
    assert(tagValues(3) === 1)
  }

  sparkTest("characterizeTags counts tags in a SAM file correctly") {
    val filePath = getClass.getClassLoader.getResource("reads12.sam").getFile
    val sam: RDD[ADAMRecord] = sc.adamLoad(filePath)

    val mapCounts: Map[String, Long] = Map(sam.adamCharacterizeTags().collect(): _*)
    assert(mapCounts("NM") === 200)
    assert(mapCounts("AS") === 200)
    assert(mapCounts("XS") === 200)
  }

  sparkTest("recover reference string from a single contig fragment") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val sequence = "ACTGTAC"
    val fragment = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence(sequence)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(1)
      .build()
    val region = ReferenceRegion(fragment).get

    val rdd = sc.parallelize(List(fragment))

    assert(rdd.adamGetReferenceString(region) === "ACTGTAC")
  }

  sparkTest("recover trimmed reference string from a single contig fragment") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val sequence = "ACTGTAC"
    val fragment = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence(sequence)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(1)
      .build()
    val region = new ReferenceRegion("chr1", 1L, 6L)

    val rdd = sc.parallelize(List(fragment))

    assert(rdd.adamGetReferenceString(region) === "CTGTA")
  }

  sparkTest("recover reference string from multiple contig fragments") {
    val contig1 = ADAMContig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val contig2 = ADAMContig.newBuilder
      .setContigName("chr2")
      .setContigLength(11L)
      .build

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.drop(3).take(5) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG
    val fragment0 = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig1)
      .setFragmentSequence(sequence0)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(1)
      .build()
    val fragment1 = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig2)
      .setFragmentSequence(sequence1)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(2)
      .build()
    val fragment2 = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig2)
      .setFragmentSequence(sequence2)
      .setFragmentNumber(1)
      .setFragmentStartPosition(5L)
      .setNumberOfFragmentsInContig(2)
      .build()
    val region0 = ReferenceRegion(fragment0).get
    val region1 = ReferenceRegion(fragment1).get.merge(ReferenceRegion(fragment2).get)

    val rdd = sc.parallelize(List(fragment0, fragment1, fragment2))

    assert(rdd.adamGetReferenceString(region0) === "ACTGTAC")
    assert(rdd.adamGetReferenceString(region1) === "GTACTCTCATG")
  }

  sparkTest("recover trimmed reference string from multiple contig fragments") {
    val contig1 = ADAMContig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val contig2 = ADAMContig.newBuilder
      .setContigName("chr2")
      .setContigLength(11L)
      .build

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.drop(3).take(5) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG
    val fragment0 = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig1)
      .setFragmentSequence(sequence0)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(1)
      .build()
    val fragment1 = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig2)
      .setFragmentSequence(sequence1)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(2)
      .build()
    val fragment2 = ADAMNucleotideContigFragment.newBuilder()
      .setContig(contig2)
      .setFragmentSequence(sequence2)
      .setFragmentNumber(1)
      .setFragmentStartPosition(5L)
      .setNumberOfFragmentsInContig(2)
      .build()
    val region0 = new ReferenceRegion("chr1", 1L, 6L)
    val region1 = new ReferenceRegion("chr2", 3L, 9L)

    val rdd = sc.parallelize(List(fragment0, fragment1, fragment2))

    assert(rdd.adamGetReferenceString(region0) === "CTGTA")
    assert(rdd.adamGetReferenceString(region1) === "CTCTCA")
  }

  sparkTest("round trip from ADAM to SAM and back to ADAM produces equivalent ADAMRecord values") {
    val reads12Path = Thread.currentThread().getContextClassLoader.getResource("reads12.sam").getFile
    val rdd12A: RDD[ADAMRecord] = sc.adamLoad(reads12Path)

    val tempFile = File.createTempFile("reads12", "adam")
    rdd12A.adamSAMSave(tempFile.getAbsolutePath, asSam = true)

    val rdd12B: RDD[ADAMRecord] = sc.adamLoad(tempFile.getAbsolutePath)

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

}
