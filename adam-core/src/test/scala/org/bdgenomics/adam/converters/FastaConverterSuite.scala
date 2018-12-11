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
package org.bdgenomics.adam.converters

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import java.io.File

class FastaConverterSuite extends ADAMFunSuite {

  val converter = new FastaConverter(1000)

  sparkTest("find contig index") {
    val headerLines = sc.parallelize(Seq(
      (0L, ">1 dna:chromosome chromosome:GRCh37:1:1:249250621:1"),
      (252366306L, ">2 dna:chromosome chromosome:GRCh37:2:1:243199373:1"),
      (699103487L, ">4 dna:chromosome chromosome:GRCh37:4:1:191154276:1"),
      (892647244L, ">5 dna:chromosome chromosome:GRCh37:5:1:180915260:1"),
      (498605724L, ">3 dna:chromosome chromosome:GRCh37:3:1:198022430:1")))
    val descLines = FastaConverter.getDescriptionLines(headerLines)
    val headerIndices: List[Long] = descLines.keys.toList

    assert(0 === FastaConverter.findReferenceIndex(252366300L, headerIndices))
    assert(892647244L === FastaConverter.findReferenceIndex(892647249L, headerIndices))
    assert(252366306L === FastaConverter.findReferenceIndex(498605720L, headerIndices))
  }

  test("convert a single record without naming information") {
    val contig = converter.convert(None, 0, Seq("AAATTTGCGC"), None)

    assert(contig.head.getSequence.map(_.toString).reduce(_ + _) === "AAATTTGCGC")
    assert(contig.head.getContigLength === 10)
    assert(contig.head.getContigName === null)
    assert(contig.head.getDescription === null)
  }

  test("convert a single record with naming information") {
    val contig = converter.convert(Some("chr2"), 1, Seq("NNNN"), Some("hg19"))

    assert(contig.head.getSequence.map(_.toString).reduce(_ + _) === "NNNN")
    assert(contig.head.getContigLength === 4)
    assert(contig.head.getContigName === "chr2")
    assert(contig.head.getDescription === "hg19")
  }

  sparkTest("convert single fasta sequence") {
    val fasta = List((0L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAA"),
      (1L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (2L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (3L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (4L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (5L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (6L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (7L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (8L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (9L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (10L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (11L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (12L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (13L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (14L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (15L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
    val rdd = sc.parallelize(fasta.toSeq)

    val adamFasta = FastaConverter(rdd)
    assert(adamFasta.count === 1)

    val fastaElement = adamFasta.first()
    val fastaSequence = fasta.map(_._2).reduce(_ + _)
    val convertedSequence = fastaElement.getSequence.map(_.toString).reduce(_ + _)

    assert(convertedSequence === fastaSequence)
    assert(fastaElement.getContigLength() == fastaSequence.length)
    assert(fastaElement.getContigName === null)
    assert(fastaElement.getDescription === null)
  }

  sparkTest("convert fasta with multiple sequences") {
    val fasta1 = List((0L, ">chr1"),
      (1L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAA"),
      (2L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (3L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (4L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (5L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (6L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (7L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (8L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (9L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (10L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (11L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (12L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (13L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (14L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (15L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (16L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
    val fasta2 = List((17L, ">chr2"),
      (18L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCTTTTTTTTTTCCCCCCCCCCTTTTTTTTTTCCCCCC"),
      (19L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (20L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (21L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (22L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (23L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (24L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (25L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (26L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (27L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (28L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (29L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (30L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (31L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (32L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (33L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"))
    val fasta = fasta1 ::: fasta2
    val rdd = sc.parallelize(fasta.toSeq)

    val adamFasta = FastaConverter(rdd)
    assert(adamFasta.count === 2)

    val fastaElement1 = adamFasta.filter(_.getContigName == "chr1").first()
    val fastaSequence1 = fasta1.drop(1).map(_._2).reduce(_ + _)
    val convertedSequence1 = fastaElement1.getSequence.map(_.toString).reduce(_ + _)

    assert(convertedSequence1 === fastaSequence1)
    assert(fastaElement1.getContigLength() == fastaSequence1.length)
    assert(fastaElement1.getContigName().toString === "chr1")
    assert(fastaElement1.getDescription === null)

    val fastaElement2 = adamFasta.filter(_.getContigName == "chr2").first()
    val fastaSequence2 = fasta2.drop(1).map(_._2).reduce(_ + _)
    val convertedSequence2 = fastaElement2.getSequence.map(_.toString).reduce(_ + _)

    assert(convertedSequence2 === fastaSequence2)
    assert(fastaElement2.getContigLength() == fastaSequence2.length)
    assert(fastaElement2.getContigName().toString === "chr2")
    assert(fastaElement2.getDescription === null)
  }

  sparkTest("convert fasta with multiple sequences; short fragment") {
    val fasta1 = List((0L, ">chr1"),
      (1L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAA"),
      (2L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (3L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (4L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (5L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (6L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (7L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (8L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (9L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (10L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (11L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (12L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (13L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (14L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (15L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
      (16L, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
    val fasta2 = List((17L, ">chr2"),
      (18L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCTTTTTTTTTTCCCCCCCCCCTTTTTTTTTTCCCCCC"),
      (19L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (20L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (21L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (22L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (23L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (24L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (25L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (26L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (27L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (28L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (29L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (30L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (31L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (32L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
      (33L, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"))
    val fasta = fasta1 ::: fasta2
    val rdd = sc.parallelize(fasta.toSeq)

    val adamFasta = FastaConverter(rdd, maximumLength = 35)
    assert(adamFasta.count === 64)

    val fastaElement1 = adamFasta.filter(_.getContigName == "chr1").collect()
    val fastaSequence1 = fasta1.drop(1).map(_._2).mkString
    val seqs = fastaElement1.sortBy(_.getIndex)
    val convertedSequence1 = fastaElement1.sortBy(_.getIndex).map(_.getSequence.toString).mkString
    assert(seqs != null)
    assert(convertedSequence1 === fastaSequence1)

    val fastaElement2 = adamFasta.filter(_.getContigName == "chr2").collect()
    val fastaSequence2 = fasta2.drop(1).map(_._2).mkString
    val convertedSequence2 = fastaElement2.sortBy(_.getIndex).map(_.getSequence.toString).mkString

    assert(convertedSequence2 === fastaSequence2)
  }

  val chr1File = testFile("human_g1k_v37_chr1_59kb.fasta")

  sparkTest("convert reference fasta file") {
    //Loading "human_g1k_v37_chr1_59kb.fasta"
    val referenceSequences = sc.loadContigFragments(chr1File, maximumLength = 10).rdd.collect()
    assert(referenceSequences.forall(_.getContigName.toString == "1"))
    assert(referenceSequences.slice(0, referenceSequences.length - 2).forall(_.getSequence.length == 10))

    val reassembledSequence = referenceSequences.sortBy(_.getIndex).map(_.getSequence).mkString
    val originalSequence = scala.io.Source.fromFile(new File(chr1File)).getLines().filter(!_.startsWith(">")).mkString

    assert(reassembledSequence === originalSequence)
  }
}
