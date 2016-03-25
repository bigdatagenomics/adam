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
package org.bdgenomics.adam.rdd.contig

import com.google.common.io.Files
import java.io.File
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._

import scala.collection.mutable.ListBuffer

class NucleotideContigFragmentRDDFunctionsSuite extends ADAMFunSuite {

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

    val dict = rdd.getSequenceDictionary()

    assert(dict.containsRefName("chr0"))
    val chr0 = dict("chr0").get
    assert(chr0.length === 1000L)
    assert(chr0.url == Some("http://bigdatagenomics.github.io/chr0.fa"))
    assert(dict.containsRefName("chr1"))
    val chr1 = dict("chr1").get
    assert(chr1.length === 900L)
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

    assert(rdd.getReferenceString(region) === "ACTGTAC")
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

    assert(rdd.getReferenceString(region) === "CTGTA")
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

    assert(rdd.getReferenceString(region0) === "ACTGTAC")
    assert(rdd.getReferenceString(region1) === "GTACTCTCATG")
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

    assert(rdd.getReferenceString(region0) === "CTGTA")
    assert(rdd.getReferenceString(region1) === "CTCTCA")
  }

  sparkTest("testing nondeterminism from reduce when recovering referencestring") {
    val contig1 = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(1000L)
      .build

    var fragments: ListBuffer[NucleotideContigFragment] = new ListBuffer[NucleotideContigFragment]()
    for (a <- 0L to 1000L) {
      val seq = "A"
      val frag = NucleotideContigFragment.newBuilder()
        .setContig(contig1)
        .setFragmentStartPosition(0L + a)
        .setFragmentSequence(seq)
        .build()
      fragments += frag
    }
    var passed = true
    val rdd = sc.parallelize(fragments.toList)
    try {
      val result = rdd.getReferenceString(new ReferenceRegion("chr1", 0L, 1000L))
    } catch {
      case e: AssertionError => passed = false
    }
    assert(passed == true)
  }

  sparkTest("save single contig fragment as FASTA text file") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val fragment = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(0)
      .setNumberOfFragmentsInContig(1)
      .build

    val rdd = sc.parallelize(List(fragment))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.coalesce(1).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save single contig fragment with description as FASTA text file") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val fragment = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setDescription("description")
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(0)
      .setNumberOfFragmentsInContig(1)
      .build

    val rdd = sc.parallelize(List(fragment))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.coalesce(1).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1 description")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save single contig fragment with null fields as FASTA text file") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val fragment = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(null)
      .setFragmentStartPosition(null)
      .setNumberOfFragmentsInContig(null)
      .build

    val rdd = sc.parallelize(List(fragment))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.coalesce(1).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save single contig fragment with null fragment number as FASTA text file") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val fragment = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(null)
      .setFragmentStartPosition(null)
      .setNumberOfFragmentsInContig(1)
      .build

    val rdd = sc.parallelize(List(fragment))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.coalesce(1).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save single contig fragment with null number of fragments in contig as FASTA text file") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val fragment = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(0)
      .setFragmentStartPosition(null)
      .setNumberOfFragmentsInContig(null)
      .build

    val rdd = sc.parallelize(List(fragment))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.coalesce(1).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save multiple contig fragments from same contig as FASTA text file") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(21L)
      .build

    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(0)
      .setNumberOfFragmentsInContig(3)
      .build
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("GCATATC")
      .setFragmentNumber(1)
      .setNumberOfFragmentsInContig(3)
      .build
    val fragment2 = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("CTGATCG")
      .setFragmentNumber(2)
      .setNumberOfFragmentsInContig(3)
      .build

    val rdd = sc.parallelize(List(fragment0, fragment1, fragment2))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.coalesce(1).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 6)
    assert(fastaLines(0) === ">chr1 fragment 1 of 3")
    assert(fastaLines(1) === "ACTGTAC")
    assert(fastaLines(2) === ">chr1 fragment 2 of 3")
    assert(fastaLines(3) === "GCATATC")
    assert(fastaLines(4) === ">chr1 fragment 3 of 3")
    assert(fastaLines(5) === "CTGATCG")
  }

  sparkTest("save multiple contig fragments with description from same contig as FASTA text file") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(21L)
      .build

    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setDescription("description")
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(0)
      .setNumberOfFragmentsInContig(3)
      .build
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setDescription("description")
      .setFragmentSequence("GCATATC")
      .setFragmentNumber(1)
      .setNumberOfFragmentsInContig(3)
      .build
    val fragment2 = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setDescription("description")
      .setFragmentSequence("CTGATCG")
      .setFragmentNumber(2)
      .setNumberOfFragmentsInContig(3)
      .build

    val rdd = sc.parallelize(List(fragment0, fragment1, fragment2))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.coalesce(1).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 6)
    assert(fastaLines(0) === ">chr1 description fragment 1 of 3")
    assert(fastaLines(1) === "ACTGTAC")
    assert(fastaLines(2) === ">chr1 description fragment 2 of 3")
    assert(fastaLines(3) === "GCATATC")
    assert(fastaLines(4) === ">chr1 description fragment 3 of 3")
    assert(fastaLines(5) === "CTGATCG")
  }

  sparkTest("merge single contig fragment null fragment number") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val fragment = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(null)
      .setFragmentStartPosition(null)
      .setNumberOfFragmentsInContig(null)
      .build

    val rdd = sc.parallelize(List(fragment))
    val merged = rdd.mergeFragments()

    assert(merged.count == 1L)
    assert(merged.first.getFragmentSequence() === "ACTGTAC")
  }

  sparkTest("merge single contig fragment number zero") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(7L)
      .build

    val fragment = NucleotideContigFragment.newBuilder()
      .setContig(contig)
      .setFragmentSequence("ACTGTAC")
      .setFragmentNumber(0)
      .setFragmentStartPosition(0L)
      .setNumberOfFragmentsInContig(1)
      .build

    val rdd = sc.parallelize(List(fragment))
    val merged = rdd.mergeFragments()

    assert(merged.count == 1L)
    assert(merged.first.getFragmentSequence() === "ACTGTAC")
  }

  sparkTest("merge multiple contig fragments") {
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

    val rdd = sc.parallelize(List(fragment2, fragment1, fragment0))
    val merged = rdd.mergeFragments()

    assert(merged.count == 2L)

    val collect = merged.collect
    assert(collect(0).getFragmentSequence() === "ACTGTAC")
    assert(collect(1).getFragmentSequence() === "GTACTCTCATG")
  }
}
