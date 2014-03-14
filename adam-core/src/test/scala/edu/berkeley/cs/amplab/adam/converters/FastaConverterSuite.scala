/**
 * Copyright (c) 2014. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.converters

import edu.berkeley.cs.amplab.adam.avro.{ADAMNucleotideContigFragment, Base}
import edu.berkeley.cs.amplab.adam.rdd.ADAMContext._
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

class FastaConverterSuite extends SparkFunSuite {
  
  val converter = new FastaConverter(1000)

  test ("convert a single record without naming information") {
    val contig = converter.convert(None, 0, Seq("AAATTTGCGC"), None)

    assert(contig.head.getContigId === 0)
    assert(contig.head.getFragmentSequence.map(_.toString).reduce(_ + _) === "AAATTTGCGC")
    assert(contig.head.getContigLength === 10)
    assert(contig.head.getContigName === null)
    assert(contig.head.getDescription === null)
  }

  test ("convert a single record with naming information") {
    val contig = converter.convert(Some("chr2"), 1, Seq("NNNN"), Some("hg19"))

    assert(contig.head.getContigId === 1)
    assert(contig.head.getFragmentSequence.map(_.toString).reduce(_ + _) === "NNNN")
    assert(contig.head.getContigLength === 4)
    assert(contig.head.getContigName === "chr2")
    assert(contig.head.getDescription === "hg19")
  }

  sparkTest ("convert single fasta sequence") {
    val fasta = List(( 0, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAA"),
                     ( 1, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     ( 2, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     ( 3, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     ( 4, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     ( 5, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     ( 6, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     ( 7, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     ( 8, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     ( 9, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     (10, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     (11, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     (12, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     (13, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     (14, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                     (15, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
    val rdd = sc.parallelize(fasta.toSeq)

    val adamFasta = FastaConverter(rdd)
    assert(adamFasta.count === 1)

    val fastaElement = adamFasta.first()
    val fastaFragmentSequence = fasta.map(_._2).reduce(_ + _)
    val convertedFragmentSequence = fastaElement.getFragmentSequence.map(_.toString).reduce(_ + _)

    assert(fastaElement.getContigId() === 0)
    assert(convertedFragmentSequence === fastaFragmentSequence)
    assert(fastaElement.getContigLength() == fastaFragmentSequence.length)
    assert(fastaElement.getContigName === null)
    assert(fastaElement.getDescription === null)
  }

  sparkTest ("convert fasta with multiple sequences") {
    val fasta1 = List(( 0, ">chr1"),
                      ( 1, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAA"),
                      ( 2, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      ( 3, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      ( 4, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      ( 5, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      ( 6, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      ( 7, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      ( 8, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      ( 9, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      (10, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      (11, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      (12, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      (13, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      (14, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      (15, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
                      (16, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))
    val fasta2 = List((17, ">chr2"),
                      (18, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCTTTTTTTTTTCCCCCCCCCCTTTTTTTTTTCCCCCC"),
                      (19, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (20, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (21, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (22, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (23, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (24, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (25, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (26, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (27, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (28, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (29, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (30, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (31, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (32, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"),
                      (33, "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"))
    val fasta = fasta1 ::: fasta2
    val rdd = sc.parallelize(fasta.toSeq)

    val adamFasta = FastaConverter(rdd)
    assert(adamFasta.count === 2)

    val fastaElement1 = adamFasta.filter(_.getContigName == "chr1").first()
    val fastaFragmentSequence1 = fasta1.drop(1).map(_._2).reduce(_ + _)
    val convertedFragmentSequence1 = fastaElement1.getFragmentSequence.map(_.toString).reduce(_ + _)

    assert(fastaElement1.getContigId() === 0)
    assert(convertedFragmentSequence1 === fastaFragmentSequence1)
    assert(fastaElement1.getContigLength() == fastaFragmentSequence1.length)
    assert(fastaElement1.getContigName().toString === "chr1")
    assert(fastaElement1.getDescription === null)

    val fastaElement2 = adamFasta.filter(_.getContigName == "chr2").first()
    val fastaFragmentSequence2 = fasta2.drop(1).map(_._2).reduce(_ + _)
    val convertedFragmentSequence2 = fastaElement2.getFragmentSequence.map(_.toString).reduce(_ + _)

    assert(fastaElement2.getContigId() === 1)
    assert(convertedFragmentSequence2 === fastaFragmentSequence2)
    assert(fastaElement2.getContigLength() == fastaFragmentSequence2.length)
    assert(fastaElement2.getContigName().toString === "chr2")
    assert(fastaElement2.getDescription === null)
  }

}
