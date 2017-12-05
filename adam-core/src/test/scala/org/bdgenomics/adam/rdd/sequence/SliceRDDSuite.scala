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
package org.bdgenomics.adam.rdd.sequence

import java.io.File

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  Alphabet,
  QualityScoreVariant,
  Slice,
  Strand
}

class SliceRDDSuite extends ADAMFunSuite {

  val s1 = Slice.newBuilder()
    .setName("name1")
    .setDescription("description")
    .setAlphabet(Alphabet.DNA)
    .setSequence("actg")
    .setStart(0L)
    .setEnd(3L)
    .setStrand(Strand.INDEPENDENT)
    .setLength(4L)
    .build

  val s2 = Slice.newBuilder()
    .setName("name2")
    .setDescription("description")
    .setAlphabet(Alphabet.DNA)
    .setSequence("aatt")
    .setStart(0L)
    .setEnd(3L)
    .setStrand(Strand.INDEPENDENT)
    .setLength(4L)
    .build

  val s3 = Slice.newBuilder()
    .setName("name2")
    .setDescription("description")
    .setAlphabet(Alphabet.DNA)
    .setSequence("ccgg")
    .setStart(4L)
    .setEnd(7L)
    .setStrand(Strand.INDEPENDENT)
    .setLength(4L)
    .build

  val sd = SequenceDictionary(
    SequenceRecord("name1", 4),
    SequenceRecord("name2", 4)
  )

  sparkTest("create a new slice rdd") {
    val slices: RDD[Slice] = sc.parallelize(Seq(s1, s2, s3))
    assert(SliceRDD(slices).rdd.count === 3)
  }

  sparkTest("create a new slice rdd with sequence dictionary") {
    val slices: RDD[Slice] = sc.parallelize(Seq(s1, s2, s3))
    assert(SliceRDD(slices, sd).rdd.count === 3)
  }

  sparkTest("merge slices into a sequence rdd") {
    val slices: SliceRDD = SliceRDD(sc.parallelize(Seq(s1, s2, s3)))
    val sequences = slices.merge()
    assert(sequences.rdd.count === 2)

    val seqs = sequences.rdd.collect
    val seq1 = seqs(0)
    val seq2 = seqs(1)

    assert(seq1.getLength === 4L)
    assert(seq2.getLength === 8L)
    assert(seq2.getSequence === "aattccgg")
  }

  def tempLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("SliceRDDSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  sparkTest("save as parquet") {
    val slices: SliceRDD = SliceRDD(sc.parallelize(Seq(s1, s2, s3)))
    val outputPath = tempLocation(".adam")
    slices.save(outputPath, asSingleFile = false)
  }

  sparkTest("save as fasta") {
    val slices: SliceRDD = SliceRDD(sc.parallelize(Seq(s1, s2, s3)))
    val outputPath = tempLocation(".fasta")
    slices.save(outputPath, asSingleFile = false)
  }

  sparkTest("save as single file fasta") {
    val slices: SliceRDD = SliceRDD(sc.parallelize(Seq(s1, s2, s3)))
    val outputPath = tempLocation(".fasta")
    slices.save(outputPath, asSingleFile = true)
  }

  sparkTest("convert slices to reads") {
    val slices: SliceRDD = SliceRDD(sc.parallelize(Seq(s1, s2)))
    val reads = slices.toReads.rdd.collect()
    assert(reads.length === 2)

    val r1 = reads(0)
    assert(r1.getName === "name1")
    assert(r1.getDescription === "description")
    assert(r1.getAlphabet === Alphabet.DNA)
    assert(r1.getLength === 4L)
    assert(r1.getSequence === "actg")
    assert(r1.getQualityScores === "BBBB")
    assert(r1.getQualityScoreVariant === QualityScoreVariant.FASTQ_SANGER)

    val r2 = reads(1)
    assert(r2.getName === "name2")
    assert(r2.getDescription === "description")
    assert(r2.getAlphabet === Alphabet.DNA)
    assert(r2.getLength === 4L)
    assert(r2.getSequence === "aatt")
    assert(r2.getQualityScores === "BBBB")
    assert(r2.getQualityScoreVariant === QualityScoreVariant.FASTQ_SANGER)
  }

  sparkTest("convert slices to sequences") {
    val slices: SliceRDD = SliceRDD(sc.parallelize(Seq(s1, s2)))
    val sequences = slices.toSequences.rdd.collect()
    assert(sequences.length === 2)

    val sequence1 = sequences(0)
    assert(sequence1.getName === "name1")
    assert(sequence1.getDescription === "description")
    assert(sequence1.getAlphabet === Alphabet.DNA)
    assert(sequence1.getLength === 4L)
    assert(sequence1.getSequence === "actg")

    val sequence2 = sequences(1)
    assert(sequence2.getName === "name2")
    assert(sequence2.getDescription === "description")
    assert(sequence2.getAlphabet === Alphabet.DNA)
    assert(sequence2.getLength === 4L)
    assert(sequence2.getSequence === "aatt")
  }
}
