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

import java.io.File

import com.google.common.io.Files
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset
import org.bdgenomics.adam.rdd.variant.{
  GenotypeDataset,
  VariantDataset,
  VariantContextDataset
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
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import scala.collection.mutable.ListBuffer

object NucleotideContigFragmentDatasetSuite extends Serializable {

  def covFn(ncf: NucleotideContigFragment): Coverage = {
    Coverage(ncf.getContigName,
      ncf.getStart,
      ncf.getEnd,
      1)
  }

  def featFn(ncf: NucleotideContigFragment): Feature = {
    Feature.newBuilder
      .setReferenceName(ncf.getContigName)
      .setStart(ncf.getStart)
      .setEnd(ncf.getEnd)
      .build
  }

  def fragFn(ncf: NucleotideContigFragment): Fragment = {
    Fragment.newBuilder
      .setName(ncf.getContigName)
      .build
  }

  def genFn(ncf: NucleotideContigFragment): Genotype = {
    Genotype.newBuilder
      .setReferenceName(ncf.getContigName)
      .setStart(ncf.getStart)
      .setEnd(ncf.getEnd)
      .build
  }

  def readFn(ncf: NucleotideContigFragment): AlignmentRecord = {
    AlignmentRecord.newBuilder
      .setReferenceName(ncf.getContigName)
      .setStart(ncf.getStart)
      .setEnd(ncf.getEnd)
      .build
  }

  def varFn(ncf: NucleotideContigFragment): Variant = {
    Variant.newBuilder
      .setReferenceName(ncf.getContigName)
      .setStart(ncf.getStart)
      .setEnd(ncf.getEnd)
      .build
  }

  def vcFn(ncf: NucleotideContigFragment): VariantContext = {
    VariantContext(Variant.newBuilder
      .setReferenceName(ncf.getContigName)
      .setStart(ncf.getStart)
      .setEnd(ncf.getEnd)
      .build)
  }
}

class NucleotideContigFragmentDatasetSuite extends ADAMFunSuite {

  sparkTest("union two ncf genomic datasets together") {
    val fragments1 = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 10000L)
    val fragments2 = sc.loadFasta(testFile("artificial.fa"))
    val union = fragments1.union(fragments2)
    assert(union.rdd.count === (fragments1.rdd.count + fragments2.rdd.count))
    assert(union.sequences.size === 2)
  }

  sparkTest("round trip a ncf to parquet") {
    def testMetadata(fRdd: NucleotideContigFragmentDataset) {
      val sequenceRdd = fRdd.addSequence(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.sequences.containsReferenceName("aSequence"))
    }

    val fragments1 = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)
    assert(fragments1.rdd.count === 8L)
    assert(fragments1.dataset.count === 8L)
    testMetadata(fragments1)

    // save using dataset path
    val output1 = tmpFile("ctg.adam")
    val dsBound = fragments1.transformDataset(ds => ds)
    testMetadata(dsBound)
    dsBound.saveAsParquet(output1)
    val fragments2 = sc.loadContigFragments(output1)
    testMetadata(fragments2)
    assert(fragments2.rdd.count === 8L)
    assert(fragments2.dataset.count === 8L)

    // save using rdd path
    val output2 = tmpFile("ctg.adam")
    val rddBound = fragments2.transform(rdd => rdd)
    testMetadata(rddBound)
    rddBound.saveAsParquet(output2)
    val fragments3 = sc.loadContigFragments(output2)
    assert(fragments3.rdd.count === 8L)
    assert(fragments3.dataset.count === 8L)
  }

  sparkTest("round trip a ncf to partitioned parquet") {
    def testMetadata(fRdd: NucleotideContigFragmentDataset) {
      val sequenceRdd = fRdd.addSequence(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.sequences.containsReferenceName("aSequence"))
    }

    val fragments1 = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)
    assert(fragments1.rdd.count === 8L)
    assert(fragments1.dataset.count === 8L)
    testMetadata(fragments1)

    // save using dataset path
    val output1 = tmpFile("ctg.adam")
    val dsBound = fragments1.transformDataset(ds => ds)
    testMetadata(dsBound)
    dsBound.saveAsPartitionedParquet(output1)
    val fragments2 = sc.loadPartitionedParquetContigFragments(output1)
    testMetadata(fragments2)
    assert(fragments2.rdd.count === 8L)
    assert(fragments2.dataset.count === 8L)

    // save using rdd path
    val output2 = tmpFile("ctg.adam")
    val rddBound = fragments2.transform(rdd => rdd)
    testMetadata(rddBound)
    rddBound.saveAsPartitionedParquet(output2)
    val fragments3 = sc.loadPartitionedParquetContigFragments(output2)
    assert(fragments3.rdd.count === 8L)
    assert(fragments3.dataset.count === 8L)
  }

  sparkTest("save fasta back as a single file") {
    val origFasta = testFile("artificial.fa")
    val tmpFasta = tmpFile("test.fa")
    sc.loadFasta(origFasta)
      .saveAsFasta(tmpFasta, asSingleFile = true, lineWidth = 70)
    checkFiles(origFasta, tmpFasta)
  }

  sparkTest("generate sequence dict from fasta") {

    val ctg0 = NucleotideContigFragment.newBuilder()
      .setContigName("chr0")
      .setContigLength(1000L)
      .build()
    val ctg1 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(900L)
      .build()

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(ctg0, ctg1)))

    assert(rdd.sequences.containsReferenceName("chr0"))
    val chr0 = rdd.sequences("chr0").get
    assert(chr0.length === 1000L)
    assert(rdd.sequences.containsReferenceName("chr1"))
    val chr1 = rdd.sequences("chr1").get
    assert(chr1.length === 900L)
  }

  sparkTest("recover reference string from a single contig fragment") {

    val sequence = "ACTGTAC"
    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence(sequence)
      .setIndex(0)
      .setStart(0L)
      .setEnd(7L)
      .setFragments(1)
      .build()
    val region = ReferenceRegion(fragment).get

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))

    assert(rdd.extract(region) === "ACTGTAC")
  }

  sparkTest("recover trimmed reference string from a single contig fragment") {

    val sequence = "ACTGTAC"
    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence(sequence)
      .setIndex(0)
      .setStart(0L)
      .setEnd(7L)
      .setFragments(1)
      .build()
    val region = new ReferenceRegion("chr1", 1L, 6L)

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))

    assert(rdd.extract(region) === "CTGTA")
  }

  sparkTest("recover reference string from multiple contig fragments") {

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.drop(3).take(5) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG
    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence(sequence0)
      .setIndex(0)
      .setStart(0L)
      .setEnd(7L)
      .setFragments(1)
      .build()
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContigName("chr2")
      .setContigLength(11L)
      .setSequence(sequence1)
      .setIndex(0)
      .setStart(0L)
      .setEnd(5L)
      .setFragments(2)
      .build()
    val fragment2 = NucleotideContigFragment.newBuilder()
      .setContigName("chr2")
      .setContigLength(11L)
      .setSequence(sequence2)
      .setIndex(1)
      .setStart(5L)
      .setEnd(12L)
      .setFragments(2)
      .build()
    val region0 = ReferenceRegion(fragment0).get
    val region1 = ReferenceRegion(fragment1).get.merge(ReferenceRegion(fragment2).get)

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment0,
      fragment1,
      fragment2)))

    assert(rdd.extract(region0) === "ACTGTAC")
    assert(rdd.extract(region1) === "GTACTCTCATG")
  }

  sparkTest("extract sequences based on the list of reference regions") {
    val test = "test"

    def dnas2fragments(dnas: Seq[String]): List[NucleotideContigFragment] = {
      val (_, frags) = dnas.foldLeft((0L, List.empty[NucleotideContigFragment])) {
        case ((start, acc), str) =>
          val fragment = NucleotideContigFragment.newBuilder()
            .setContigName("test")
            .setStart(start)
            .setLength(str.length: Long)
            .setSequence(str)
            .setEnd(start + str.length)
            .build()
          (start + str.length, fragment :: acc)
      }
      frags.reverse
    }

    val dnas: Seq[String] = Vector(
      "ACAGCTGATCTCCAGATATGACCATGGGTT",
      "CAGCTGATCTCCAGATATGACCATGGGTTT",
      "CCAGAAGTTTGAGCCACAAACCCATGGTCA"
    )

    val merged = dnas.reduce(_ + _)

    val record = SequenceRecord("test", merged.length)

    val dic = new SequenceDictionary(Vector(record))
    val frags = sc.parallelize(dnas2fragments(dnas))
    val fragments = NucleotideContigFragmentDataset(frags, dic)

    val byRegion = fragments.rdd.keyBy(ReferenceRegion(_))

    val regions = List(
      new ReferenceRegion(test, 0, 5),
      new ReferenceRegion(test, 25, 35),
      new ReferenceRegion(test, 40, 50),
      new ReferenceRegion(test, 50, 70)
    )

    val results: Set[(ReferenceRegion, String)] = fragments.extractRegions(regions).collect().toSet
    val seqs = regions.zip(List("ACAGC", "GGGTTCAGCT", "CCAGATATGA", "CCATGGGTTTCCAGAAGTTT")).toSet
    assert(seqs === results)
  }

  sparkTest("recover trimmed reference string from multiple contig fragments") {

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.drop(3).take(5) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG
    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence(sequence0)
      .setIndex(0)
      .setStart(0L)
      .setEnd(7L)
      .setFragments(1)
      .build()
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContigName("chr2")
      .setContigLength(11L)
      .setSequence(sequence1)
      .setIndex(0)
      .setStart(0L)
      .setEnd(5L)
      .setFragments(2)
      .build()
    val fragment2 = NucleotideContigFragment.newBuilder()
      .setContigName("chr2")
      .setContigLength(11L)
      .setSequence(sequence2)
      .setIndex(1)
      .setStart(5L)
      .setEnd(11L)
      .setFragments(2)
      .build()
    val region0 = new ReferenceRegion("chr1", 1L, 6L)
    val region1 = new ReferenceRegion("chr2", 3L, 9L)

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment0,
      fragment1,
      fragment2)))

    assert(rdd.extract(region0) === "CTGTA")
    assert(rdd.extract(region1) === "CTCTCA")
  }

  sparkTest("testing nondeterminism from reduce when recovering referencestring") {

    var fragments: ListBuffer[NucleotideContigFragment] = new ListBuffer[NucleotideContigFragment]()
    for (a <- 0L to 1000L) {
      val seq = "A"
      val frag = NucleotideContigFragment.newBuilder()
        .setContigName("chr1")
        .setContigLength(1000L)
        .setStart(a)
        .setEnd(a + 1L)
        .setSequence(seq)
        .build()
      fragments += frag
    }
    var passed = true
    val rdd = NucleotideContigFragmentDataset(sc.parallelize(fragments.toList))
    try {
      val result = rdd.extract(new ReferenceRegion("chr1", 0L, 1000L))
    } catch {
      case e: AssertionError => passed = false
    }
    assert(passed == true)
  }

  sparkTest("save single contig fragment as FASTA text file") {

    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence("ACTGTAC")
      .setIndex(0)
      .setFragments(1)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save single contig fragment with description as FASTA text file") {

    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setDescription("description")
      .setSequence("ACTGTAC")
      .setIndex(0)
      .setFragments(1)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1 description")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save single contig fragment with null fields as FASTA text file") {

    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence("ACTGTAC")
      .setIndex(null)
      .setStart(null)
      .setEnd(null)
      .setFragments(null)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save single contig fragment with null fragment number as FASTA text file") {

    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence("ACTGTAC")
      .setIndex(null)
      .setStart(null)
      .setEnd(null)
      .setFragments(1)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile)
    val fastaLines = scala.io.Source.fromFile(new File(outputFastaFile + "/part-00000")).getLines().toSeq

    assert(fastaLines.length === 2)
    assert(fastaLines(0) === ">chr1")
    assert(fastaLines(1) === "ACTGTAC")
  }

  sparkTest("save single contig fragment with null number of fragments in contig as FASTA text file") {

    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence("ACTGTAC")
      .setIndex(0)
      .setStart(null)
      .setEnd(null)
      .setFragments(null)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))

    def validate(fileName: String) {
      val fastaLines = scala.io.Source.fromFile(new File(fileName + "/part-00000")).getLines().toSeq

      assert(fastaLines.length === 2)
      assert(fastaLines(0) === ">chr1")
      assert(fastaLines(1) === "ACTGTAC")
    }

    val outputFastaFile = tmpFile("test.fa")
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile)
    validate(outputFastaFile)

    val outputFastaFile2 = tmpFile("test2.fa")
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile2)
    validate(outputFastaFile2)
  }

  sparkTest("save multiple contig fragments from same contig as FASTA text file") {

    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(21L)
      .setSequence("ACTGTAC")
      .setIndex(0)
      .setFragments(3)
      .build
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(21L)
      .setSequence("GCATATC")
      .setIndex(1)
      .setFragments(3)
      .build
    val fragment2 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(21L)
      .setSequence("CTGATCG")
      .setIndex(2)
      .setFragments(3)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment0, fragment1, fragment2)))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile)
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

    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(21L)
      .setDescription("description")
      .setSequence("ACTGTAC")
      .setIndex(0)
      .setFragments(3)
      .build
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(21L)
      .setDescription("description")
      .setSequence("GCATATC")
      .setIndex(1)
      .setFragments(3)
      .build
    val fragment2 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(21L)
      .setDescription("description")
      .setSequence("CTGATCG")
      .setIndex(2)
      .setFragments(3)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment0,
      fragment1,
      fragment2)))

    val outputDir = Files.createTempDir()
    val outputFastaFile = outputDir.getAbsolutePath + "/test.fa"
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile)
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

    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence("ACTGTAC")
      .setIndex(null)
      .setStart(null)
      .setEnd(null)
      .setFragments(null)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))
    val merged = rdd.mergeFragments()

    assert(merged.rdd.count == 1L)
    assert(merged.rdd.first.getSequence() === "ACTGTAC")
  }

  sparkTest("merge single contig fragment number zero") {

    val fragment = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence("ACTGTAC")
      .setIndex(0)
      .setStart(0L)
      .setEnd(7L)
      .setFragments(1)
      .build

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment)))
    val merged = rdd.mergeFragments()

    assert(merged.rdd.count == 1L)
    assert(merged.rdd.first.getSequence() === "ACTGTAC")
  }

  sparkTest("merge multiple contig fragments") {

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.drop(3).take(5) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG
    val fragment0 = NucleotideContigFragment.newBuilder()
      .setContigName("chr1")
      .setContigLength(7L)
      .setSequence(sequence0)
      .setIndex(0)
      .setStart(0L)
      .setEnd(sequence0.length - 1L)
      .setFragments(1)
      .build()
    val fragment1 = NucleotideContigFragment.newBuilder()
      .setContigName("chr2")
      .setContigLength(11L)
      .setSequence(sequence1)
      .setIndex(0)
      .setStart(0L)
      .setEnd(sequence1.length - 1L)
      .setFragments(2)
      .build()
    val fragment2 = NucleotideContigFragment.newBuilder()
      .setContigName("chr2")
      .setContigLength(11L)
      .setSequence(sequence2)
      .setIndex(1)
      .setStart(5L)
      .setEnd(sequence2.length - 1L)
      .setFragments(2)
      .build()

    val rdd = NucleotideContigFragmentDataset(sc.parallelize(List(fragment2,
      fragment1,
      fragment0)))
    val merged = rdd.mergeFragments()

    assert(merged.rdd.count == 2L)

    val collect = merged.rdd.collect
    assert(collect(0).getSequence() === "ACTGTAC")
    assert(collect(1).getSequence() === "GTACTCTCATG")
  }

  sparkTest("save as parquet and apply predicate pushdown") {
    val fragments1 = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)
    assert(fragments1.rdd.count === 8)
    val output = tmpFile("contigs.adam")
    fragments1.saveAsParquet(output)
    val fragments2 = sc.loadContigFragments(output)
    assert(fragments2.rdd.count === 8)
    val fragments3 = sc.loadContigFragments(output,
      optPredicate = Some(
        // ReferenceRegion.toPredicate uses referenceName instead of contigName
        (BinaryColumn("contigName") === "HLA-DQB1*05:01:01:02") &&
          (LongColumn("end") > 500L) &&
          (LongColumn("start") <= 1500L)
      )
    )
    assert(fragments3.rdd.count === 2)
  }

  sparkTest("load fasta sequences from GFF3 file") {
    val sequences = sc.loadFasta(testFile("ctg123.fasta.gff3"))
    assert(sequences.rdd.count() === 4)
  }

  sparkTest("transform contigs to coverage genomic dataset") {
    val contigs = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)

    def checkSave(coverage: CoverageDataset) {
      val tempPath = tmpLocation(".bed")
      coverage.save(tempPath, false, false)

      assert(sc.loadCoverage(tempPath).rdd.count === 8)
    }

    val coverage = contigs.transmute[Coverage, Coverage, CoverageDataset](
      (rdd: RDD[NucleotideContigFragment]) => {
        rdd.map(NucleotideContigFragmentDatasetSuite.covFn)
      })

    checkSave(coverage)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val coverageDs: CoverageDataset = contigs.transmuteDataset[Coverage, Coverage, CoverageDataset](
      (ds: Dataset[NucleotideContigFragmentProduct]) => {
        ds.map(r => NucleotideContigFragmentDatasetSuite.covFn(r.toAvro))
      })

    checkSave(coverageDs)
  }

  sparkTest("transform contigs to feature genomic dataset") {
    val contigs = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)

    def checkSave(features: FeatureDataset) {
      val tempPath = tmpLocation(".bed")
      features.saveAsBed(tempPath)

      assert(sc.loadFeatures(tempPath).rdd.count === 8)
    }

    val features: FeatureDataset = contigs.transmute[Feature, FeatureProduct, FeatureDataset](
      (rdd: RDD[NucleotideContigFragment]) => {
        rdd.map(NucleotideContigFragmentDatasetSuite.featFn)
      })

    checkSave(features)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val featuresDs: FeatureDataset = contigs.transmuteDataset[Feature, FeatureProduct, FeatureDataset](
      (ds: Dataset[NucleotideContigFragmentProduct]) => {
        ds.map(r => {
          FeatureProduct.fromAvro(
            NucleotideContigFragmentDatasetSuite.featFn(r.toAvro))
        })
      })

    checkSave(featuresDs)
  }

  sparkTest("transform contigs to fragment genomic dataset") {
    val contigs = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)

    def checkSave(fragments: FragmentDataset) {
      val tempPath = tmpLocation(".adam")
      fragments.saveAsParquet(tempPath)

      assert(sc.loadFragments(tempPath).rdd.count === 8)
    }

    val fragments: FragmentDataset = contigs.transmute[Fragment, FragmentProduct, FragmentDataset](
      (rdd: RDD[NucleotideContigFragment]) => {
        rdd.map(NucleotideContigFragmentDatasetSuite.fragFn)
      })

    checkSave(fragments)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val fragmentsDs: FragmentDataset = contigs.transmuteDataset[Fragment, FragmentProduct, FragmentDataset](
      (ds: Dataset[NucleotideContigFragmentProduct]) => {
        ds.map(r => {
          FragmentProduct.fromAvro(
            NucleotideContigFragmentDatasetSuite.fragFn(r.toAvro))
        })
      })

    checkSave(fragmentsDs)
  }

  sparkTest("transform contigs to read genomic dataset") {
    val contigs = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)

    def checkSave(reads: AlignmentRecordDataset) {
      val tempPath = tmpLocation(".adam")
      reads.saveAsParquet(tempPath)

      assert(sc.loadAlignments(tempPath).rdd.count === 8)
    }

    val reads: AlignmentRecordDataset = contigs.transmute[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset](
      (rdd: RDD[NucleotideContigFragment]) => {
        rdd.map(NucleotideContigFragmentDatasetSuite.readFn)
      })

    checkSave(reads)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val readsDs: AlignmentRecordDataset = contigs.transmuteDataset[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset](
      (ds: Dataset[NucleotideContigFragmentProduct]) => {
        ds.map(r => {
          AlignmentRecordProduct.fromAvro(
            NucleotideContigFragmentDatasetSuite.readFn(r.toAvro))
        })
      })

    checkSave(readsDs)
  }

  sparkTest("transform contigs to genotype genomic dataset") {
    val contigs = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)

    def checkSave(genotypes: GenotypeDataset) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 8)
    }

    val genotypes: GenotypeDataset = contigs.transmute[Genotype, GenotypeProduct, GenotypeDataset](
      (rdd: RDD[NucleotideContigFragment]) => {
        rdd.map(NucleotideContigFragmentDatasetSuite.genFn)
      })

    checkSave(genotypes)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val genotypesDs: GenotypeDataset = contigs.transmuteDataset[Genotype, GenotypeProduct, GenotypeDataset](
      (ds: Dataset[NucleotideContigFragmentProduct]) => {
        ds.map(r => {
          GenotypeProduct.fromAvro(
            NucleotideContigFragmentDatasetSuite.genFn(r.toAvro))
        })
      })

    checkSave(genotypesDs)
  }

  sparkTest("transform contigs to variant genomic dataset") {
    val contigs = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)

    def checkSave(variants: VariantDataset) {
      val tempPath = tmpLocation(".adam")
      variants.saveAsParquet(tempPath)

      assert(sc.loadVariants(tempPath).rdd.count === 8)
    }

    val variants: VariantDataset = contigs.transmute[Variant, VariantProduct, VariantDataset](
      (rdd: RDD[NucleotideContigFragment]) => {
        rdd.map(NucleotideContigFragmentDatasetSuite.varFn)
      })

    checkSave(variants)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val variantsDs: VariantDataset = contigs.transmuteDataset[Variant, VariantProduct, VariantDataset](
      (ds: Dataset[NucleotideContigFragmentProduct]) => {
        ds.map(r => {
          VariantProduct.fromAvro(
            NucleotideContigFragmentDatasetSuite.varFn(r.toAvro))
        })
      })

    checkSave(variantsDs)
  }

  sparkTest("transform contigs to variant context genomic dataset") {
    val contigs = sc.loadFasta(testFile("HLA_DQB1_05_01_01_02.fa"), 1000L)

    def checkSave(variantContexts: VariantContextDataset) {
      assert(variantContexts.rdd.count === 8)
    }

    val variantContexts: VariantContextDataset = contigs.transmute[VariantContext, VariantContextProduct, VariantContextDataset](
      (rdd: RDD[NucleotideContigFragment]) => {
        rdd.map(NucleotideContigFragmentDatasetSuite.vcFn)
      })

    checkSave(variantContexts)
  }
}
