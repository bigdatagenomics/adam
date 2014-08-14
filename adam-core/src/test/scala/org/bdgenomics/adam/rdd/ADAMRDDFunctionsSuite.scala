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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ VariantContext, ReferenceRegion }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro._

class ADAMRDDFunctionsSuite extends SparkFunSuite {

  sparkTest("can convert pileups to rods, bases at different pos, same reference") {
    val contig = Contig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = Pileup.newBuilder()
      .setPosition(0L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()
    val p1 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[Pileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 2)
    assert(rods.filter(_.position.pos == 0L).count === 1)
    assert(rods.filter(_.position.pos == 0L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos, different reference") {
    val contig = Contig.newBuilder
      .setContigName("chr0")
      .build

    val contig1 = Contig.newBuilder
      .setContigName("chr1")
      .build

    val p0 = Pileup.newBuilder()
      .setPosition(0L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()
    val p1 = Pileup.newBuilder()
      .setPosition(0L)
      .setContig(contig1)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[Pileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 2)
    assert(rods.filter(_.position.referenceName == "chr0").count === 1)
    assert(rods.filter(_.position.referenceName == "chr0").flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.referenceName == "chr1").count === 1)
    assert(rods.filter(_.position.referenceName == "chr1").flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos") {
    val contig = Contig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()

    val p1 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[Pileup] = sc.parallelize(List(p0, p1))

    val rods = pileups.adamPileupsToRods(1)

    assert(rods.count === 1)
    assert(rods.filter(_.position.pos == 1L).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).count === 2)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.A).count === 1)
    assert(rods.filter(_.position.pos == 1L).flatMap(_.pileups).filter(_.getReadBase == Base.C).count === 1)
  }

  sparkTest("can convert pileups to rods, bases at same pos, split by different sample") {
    val contig = Contig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.A)
      .setRecordGroupSample("0")
      .build()
    val p1 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .setRecordGroupSample("1")
      .build()

    val pileups: RDD[Pileup] = sc.parallelize(List(p0, p1))

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
    val contig = Contig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.A)
      .setRecordGroupSample("1")
      .build()
    val p1 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .setRecordGroupSample("1")
      .build()

    val pileups: RDD[Pileup] = sc.parallelize(List(p0, p1))

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
    val contig = Contig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = Pileup.newBuilder()
      .setPosition(0L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()
    val p1 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[Pileup] = sc.parallelize(List(p0, p1))

    val coverage = pileups.adamPileupsToRods(1)
      .adamRodCoverage()

    // floating point, so apply tolerance
    assert(coverage > 0.99 && coverage < 1.01)
  }

  sparkTest("check coverage, bases at same pos") {
    val contig = Contig.newBuilder
      .setContigName("chr0")
      .build

    val p0 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.A)
      .build()
    val p1 = Pileup.newBuilder()
      .setPosition(1L)
      .setContig(contig)
      .setReadBase(Base.C)
      .build()

    val pileups: RDD[Pileup] = sc.parallelize(List(p0, p1))

    val coverage = pileups.adamPileupsToRods(1)
      .adamRodCoverage()

    // floating point, so apply tolerance
    assert(coverage > 1.99 && coverage < 2.01)
  }

  sparkTest("generate sequence dict from fasta") {
    val contig0 = Contig.newBuilder
      .setContigName("chr0")
      .setContigLength(1000L)
      .setReferenceURL("http://bigdatagenomics.github.io/chr0.fa")
      .build

    val contig1 = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(900L)
      .build

    val ctg0 = NucleotideContigFragment.newBuilder()
      .setContig(contig0)
      .build()
    val ctg1 = NucleotideContigFragment.newBuilder()
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
    val variant0 = Variant.newBuilder()
      .setStart(0L)
      .setAlternateAllele("A")
      .setReferenceAllele("T")
      .setContig(Contig.newBuilder.setContigName("chr0").build)
      .build()
    val variant1 = Variant.newBuilder()
      .setStart(0L)
      .setAlternateAllele("C")
      .setReferenceAllele("T")
      .setContig(Contig.newBuilder.setContigName("chr0").build)
      .build()
    val genotype0 = Genotype.newBuilder()
      .setVariant(variant0)
      .setSampleId("me")
      .build()
    val genotype1 = Genotype.newBuilder()
      .setVariant(variant1)
      .setSampleId("you")
      .build()

    val vc = VariantContext.buildFromGenotypes(List(genotype0, genotype1))
    val samples = sc.parallelize(List(vc)).adamGetCallsetSamples()

    assert(samples.count(_ == "you") === 1)
    assert(samples.count(_ == "me") === 1)
  }

  sparkTest("recover reference string from a single contig fragment") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val sequence = "ACTGTAC"
    val fragment = NucleotideContigFragment.newBuilder()
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
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val sequence = "ACTGTAC"
    val fragment = NucleotideContigFragment.newBuilder()
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
    val contig1 = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val contig2 = Contig.newBuilder
      .setContigName("chr2")
      .setContigLength(11L)
      .build

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.drop(3).take(5) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG
    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContig(contig1)
      .setFragmentSequence(sequence0)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(1)
      .build()
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContig(contig2)
      .setFragmentSequence(sequence1)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(2)
      .build()
    val fragment2 = NucleotideContigFragment.newBuilder()
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
    val contig1 = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val contig2 = Contig.newBuilder
      .setContigName("chr2")
      .setContigLength(11L)
      .build

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.drop(3).take(5) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG
    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContig(contig1)
      .setFragmentSequence(sequence0)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(1)
      .build()
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContig(contig2)
      .setFragmentSequence(sequence1)
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(2)
      .build()
    val fragment2 = NucleotideContigFragment.newBuilder()
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
}
