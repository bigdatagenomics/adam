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

    assert(0 === FastaConverter.findContigIndex(252366300L, headerIndices))
    assert(892647244L === FastaConverter.findContigIndex(892647249L, headerIndices))
    assert(252366306L === FastaConverter.findContigIndex(498605720L, headerIndices))
  }

  test("convert a single record without naming information") {
    val contig = converter.convert(None, 0, Seq("AAATTTGCGC"), None)

    assert(contig.head.getFragmentSequence.map(_.toString).reduce(_ + _) === "AAATTTGCGC")
    assert(contig.head.getContig.getContigLength === 10)
    assert(contig.head.getContig.getContigName === null)
    assert(contig.head.getDescription === null)
  }

  test("convert a single record with naming information") {
    val contig = converter.convert(Some("chr2"), 1, Seq("NNNN"), Some("hg19"))

    assert(contig.head.getFragmentSequence.map(_.toString).reduce(_ + _) === "NNNN")
    assert(contig.head.getContig.getContigLength === 4)
    assert(contig.head.getContig.getContigName === "chr2")
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
    val fastaFragmentSequence = fasta.map(_._2).reduce(_ + _)
    val convertedFragmentSequence = fastaElement.getFragmentSequence.map(_.toString).reduce(_ + _)

    assert(convertedFragmentSequence === fastaFragmentSequence)
    assert(fastaElement.getContig.getContigLength() == fastaFragmentSequence.length)
    assert(fastaElement.getContig.getContigName === null)
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

    val fastaElement1 = adamFasta.filter(_.getContig.getContigName == "chr1").first()
    val fastaFragmentSequence1 = fasta1.drop(1).map(_._2).reduce(_ + _)
    val convertedFragmentSequence1 = fastaElement1.getFragmentSequence.map(_.toString).reduce(_ + _)

    assert(convertedFragmentSequence1 === fastaFragmentSequence1)
    assert(fastaElement1.getContig.getContigLength() == fastaFragmentSequence1.length)
    assert(fastaElement1.getContig.getContigName().toString === "chr1")
    assert(fastaElement1.getDescription === null)

    val fastaElement2 = adamFasta.filter(_.getContig.getContigName == "chr2").first()
    val fastaFragmentSequence2 = fasta2.drop(1).map(_._2).reduce(_ + _)
    val convertedFragmentSequence2 = fastaElement2.getFragmentSequence.map(_.toString).reduce(_ + _)

    assert(convertedFragmentSequence2 === fastaFragmentSequence2)
    assert(fastaElement2.getContig.getContigLength() == fastaFragmentSequence2.length)
    assert(fastaElement2.getContig.getContigName().toString === "chr2")
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

    val adamFasta = FastaConverter(rdd, maxFragmentLength = 35)
    assert(adamFasta.count === 64)

    val fastaElement1 = adamFasta.filter(_.getContig.getContigName == "chr1").collect()
    val fastaFragmentSequence1 = fasta1.drop(1).map(_._2).mkString
    val seqs = fastaElement1.sortBy(_.getFragmentNumber)
    val convertedFragmentSequence1 = fastaElement1.sortBy(_.getFragmentNumber).map(_.getFragmentSequence.toString).mkString
    assert(seqs != null)
    assert(convertedFragmentSequence1 === fastaFragmentSequence1)

    val fastaElement2 = adamFasta.filter(_.getContig.getContigName == "chr2").collect()
    val fastaFragmentSequence2 = fasta2.drop(1).map(_._2).mkString
    val convertedFragmentSequence2 = fastaElement2.sortBy(_.getFragmentNumber).map(_.getFragmentSequence.toString).mkString

    assert(convertedFragmentSequence2 === fastaFragmentSequence2)
  }

  val chr1File = testFile("human_g1k_v37_chr1_59kb.fasta")

  sparkTest("convert reference fasta file") {
    //Loading "human_g1k_v37_chr1_59kb.fasta"
    val referenceSequences = sc.loadSequences(chr1File, fragmentLength = 10).rdd.collect()
    assert(referenceSequences.forall(_.getContig.getContigName.toString == "1"))
    assert(referenceSequences.slice(0, referenceSequences.length - 2).forall(_.getFragmentSequence.length == 10))

    val reassembledSequence = referenceSequences.sortBy(_.getFragmentNumber).map(_.getFragmentSequence).mkString
    val originalSequence = scala.io.Source.fromFile(new File(chr1File)).getLines().filter(!_.startsWith(">")).mkString

    assert(reassembledSequence === originalSequence)
  }
}
