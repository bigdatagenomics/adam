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

import htsjdk.samtools.ValidationStringency
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.util.{ ADAMFunSuite, ReferenceContigMap }
import org.bdgenomics.formats.avro.{ AlignmentRecord, NucleotideContigFragment, Reference }

class MDTaggingSuite extends ADAMFunSuite {
  val chr1 =
    Reference
      .newBuilder()
      .setName("chr1")
      .setLength(100L)
      .build()
  val chr2 =
    Reference
      .newBuilder()
      .setName("chr2")
      .setLength(100L)
      .build()

  def makeFrags(frags: (Reference, Int, String)*): RDD[NucleotideContigFragment] =
    sc.parallelize(
      for {
        (reference, start, seq) <- frags
      } yield (
        NucleotideContigFragment.newBuilder
        .setContigLength(reference.getLength)
        .setContigName(reference.getName)
        .setStart(start.toLong)
        .setEnd(start.toLong + seq.length)
        .setSequence(seq).build()
      )
    )

  def makeReads(reads: ((Reference, Int, Int, String, String), String)*): (Map[Int, String], RDD[AlignmentRecord]) = {
    val (map, rs) =
      (for {
        (((contig, start, end, seq, cigar), mdTag), id) <- reads.zipWithIndex
      } yield (
        id -> mdTag,
        AlignmentRecord
        .newBuilder
        .setReferenceName(contig.getName)
        .setStart(start.toLong)
        .setEnd(end.toLong)
        .setSequence(seq)
        .setCigar(cigar)
        .setReadMapped(true)
        .setReadName(id.toString)
        .build()
      )).unzip

    (map.toMap, sc.parallelize(rs))
  }

  def testReads(fs: (Reference, Int, String)*)(rs: ((Reference, Int, Int, String, String), String)*)(accumulators: (Long, Long, Long, Long)): Unit = {
    val (expectedReadMap, reads) = makeReads(rs: _*)

    def check(mdTagging: MDTagging): Unit = {
      val actualReadMap = mdTagging.taggedReads.collect.map(r => r.getReadName.toInt -> r.getMismatchingPositions).toMap

      assert(actualReadMap == expectedReadMap)
      assert(
        (
          mdTagging.mdTagsAdded.value,
          mdTagging.mdTagsExtant.value,
          mdTagging.numUnmappedReads.value,
          mdTagging.incorrectMDTags.value
        ) === accumulators
      )
    }

    check(MDTagging(reads, ReferenceContigMap(makeFrags(fs: _*))))
  }

  sparkTest("test adding MDTags over boundary") {
    testReads(
      (chr1, 0, "TTTTTTTTTT"),
      (chr1, 10, "A")
    )(
        (chr1, 9, 11, "TA", "2M") → "2",
        (chr1, 9, 11, "TG", "2M") → "1A0",
        (chr1, 9, 11, "GA", "2M") → "0T1",
        (chr1, 9, 11, "TAA", "2M1I") → "2",
        (chr1, 9, 11, "A", "1D1M") → "0^T1",
        (chr1, 9, 11, "G", "1D1M") → "0^T0A0"
      )(
          (6, 0, 0, 0)
        )
  }

  sparkTest("test adding MDTags; reads span full contig") {
    testReads(
      (chr1, 0, "AAAAAAAAAA"),
      (chr1, 10, "CCCCCCCCCC"),
      (chr1, 20, "GGGGGGGG")
    )(
        (chr1, 0, 28, "AAAAAAAAAACCCCCCCCCCGGGGGGGG", "28M") → "28",
        (chr1, 0, 28, "TAAAAAAAAACCCCCCCCCCGGGGGGGG", "28M") → "0A27",
        (chr1, 0, 28, "AAAAAAAAAACCCCCCCCCCGGGGGGGT", "28M") → "27G0",
        (chr1, 0, 28, "AAAAAAAAATTCCCCCCCCCGGGGGGGG", "28M") → "9A0C17",
        (chr1, 0, 28, "AAAAAAAAAACCCCCCCCCTTGGGGGGG", "28M") → "19C0G7"
      )((5, 0, 0, 0))
  }

  sparkTest("test adding MDTags; reads start inside first fragment") {
    testReads(
      (chr1, 0, "AAAAAAAAAA"),
      (chr1, 10, "CCCCCCCCCC"),
      (chr1, 20, "GGGGGGGG")
    )(
        (chr1, 1, 28, "AAAAAAAAACCCCCCCCCCGGGGGGGG", "27M") → "27",
        (chr1, 1, 28, "TAAAAAAAACCCCCCCCCCGGGGGGGG", "27M") → "0A26",
        (chr1, 1, 28, "AAAAAAAAACCCCCCCCCCGGGGGGGT", "27M") → "26G0",
        (chr1, 1, 28, "AAAAAAAATTCCCCCCCCCGGGGGGGG", "27M") → "8A0C17",
        (chr1, 1, 28, "AAAAAAAAACCCCCCCCCTTGGGGGGG", "27M") → "18C0G7"
      )(5, 0, 0, 0)
  }

  sparkTest("test adding MDTags; reads end inside last fragment") {
    testReads(
      (chr1, 0, "AAAAAAAAAA"),
      (chr1, 10, "CCCCCCCCCC"),
      (chr1, 20, "GGGGGGGG")
    )(
        (chr1, 0, 27, "AAAAAAAAAACCCCCCCCCCGGGGGGG", "27M") → "27",
        (chr1, 0, 27, "TAAAAAAAAACCCCCCCCCCGGGGGGG", "27M") → "0A26",
        (chr1, 0, 27, "AAAAAAAAAACCCCCCCCCCGGGGGGT", "27M") → "26G0",
        (chr1, 0, 27, "AAAAAAAAATTCCCCCCCCCGGGGGGG", "27M") → "9A0C16",
        (chr1, 0, 27, "AAAAAAAAAACCCCCCCCCTTGGGGGG", "27M") → "19C0G6"
      )(5, 0, 0, 0)
  }

  sparkTest("test adding MDTags; reads start inside first fragment and end inside last fragment") {
    testReads(
      (chr1, 0, "AAAAAAAAAA"),
      (chr1, 10, "CCCCCCCCCC"),
      (chr1, 20, "GGGGGGGG")
    )(
        (chr1, 1, 27, "AAAAAAAAACCCCCCCCCCGGGGGGG", "26M") → "26",
        (chr1, 1, 27, "TAAAAAAAACCCCCCCCCCGGGGGGG", "26M") → "0A25",
        (chr1, 1, 27, "AAAAAAAAACCCCCCCCCCGGGGGGT", "26M") → "25G0",
        (chr1, 1, 27, "AAAAAAAATTCCCCCCCCCGGGGGGG", "26M") → "8A0C16",
        (chr1, 1, 27, "AAAAAAAAACCCCCCCCCTTGGGGGG", "26M") → "18C0G6"
      )(5, 0, 0, 0)
  }

  sparkTest("test adding MDTags; reads start and end in middle fragements") {
    testReads(
      (chr1, 0, "TTTTTTTTTT"),
      (chr1, 10, "AAAAAAAAAA"),
      (chr1, 20, "CCCCCCCCCC"),
      (chr1, 30, "GGGGGGGGGG"),
      (chr1, 40, "TTTTTTTTTT")
    )(
        (chr1, 15, 35, "AAAAACCCCCCCCCCGGGGG", "20M") → "20",
        (chr1, 15, 35, "TAAAACCCCCCCCCCGGGGG", "20M") → "0A19",
        (chr1, 15, 35, "AAAAACCCCCCCCCCGGGGT", "20M") → "19G0",
        (chr1, 15, 35, "AAAATTCCCCCCCCCGGGGG", "20M") → "4A0C14",
        (chr1, 15, 35, "AAAAACCCCCCCCCTTGGGG", "20M") → "14C0G4"
      )(5, 0, 0, 0)
  }

  sparkTest("try realigning a read on a missing contig, stringency == STRICT") {
    val read = (chr2, 15, 35, "AAAAACCCCCCCCCCGGGGG", "20M") → "20"
    val tagger = MDTagging(makeReads(read)._2,
      ReferenceContigMap(makeFrags((chr1, 0, "TTTTTTTTTT"))))
    intercept[Exception] {
      tagger.taggedReads.collect
    }
  }

  sparkTest("try realigning a read on a missing contig, stringency == LENIENT") {
    val read = (chr2, 15, 35, "AAAAACCCCCCCCCCGGGGG", "20M") → "20"
    val tagger = MDTagging(makeReads(read)._2,
      ReferenceContigMap(makeFrags((chr1, 0, "TTTTTTTTTT"))),
      validationStringency = ValidationStringency.LENIENT)
    assert(tagger.taggedReads.collect.size === 1)
  }
}
