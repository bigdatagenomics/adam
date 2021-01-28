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
package org.bdgenomics.adam.ds.sequence

import com.google.common.collect.ComparisonChain
import java.io.File
import java.util.Comparator
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  Alphabet,
  Sequence,
  Strand
}

class SequenceDatasetSuite extends ADAMFunSuite {

  val s1 = Sequence.newBuilder()
    .setName("name1")
    .setDescription("description")
    .setAlphabet(Alphabet.DNA)
    .setSequence("actg")
    .setLength(4L)
    .build

  val s2 = Sequence.newBuilder()
    .setName("name2")
    .setDescription("description")
    .setAlphabet(Alphabet.DNA)
    .setSequence("actg")
    .setLength(4L)
    .build

  val sd = SequenceDictionary(
    SequenceRecord("name1", 4),
    SequenceRecord("name2", 4)
  )

  def tempLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("SequenceDatasetSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  sparkTest("create a new sequence genomic dataset") {
    val sequences: RDD[Sequence] = sc.parallelize(Seq(s1, s2))
    assert(SequenceDataset(sequences).rdd.count === 2)
  }

  sparkTest("create a new sequence genomic dataset with sequence dictionary") {
    val sequences: RDD[Sequence] = sc.parallelize(Seq(s1, s2))
    assert(SequenceDataset(sequences, sd).rdd.count === 2)
  }

  sparkTest("save as parquet") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val outputPath = tempLocation(".adam")
    sequences.save(outputPath, asSingleFile = false, disableFastConcat = false)
  }

  sparkTest("round trip as parquet") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val outputPath = tempLocation(".adam")
    sequences.saveAsParquet(outputPath)

    val parquetSequences = sc.loadParquetSequences(outputPath)
    assert(parquetSequences.rdd.count === 2)
  }

  sparkTest("save as fasta") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val outputPath = tempLocation(".fasta")
    sequences.save(outputPath, asSingleFile = false, disableFastConcat = false)
  }

  sparkTest("save as single file fasta") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val outputPath = tempLocation(".fasta")
    sequences.save(outputPath, asSingleFile = true, disableFastConcat = false)
  }

  sparkTest("convert sequences to reads") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val reads = sequences.toReads.rdd.collect()
    assert(reads.length === 2)

    val r1 = reads(0)
    assert(r1.getName === "name1")
    assert(r1.getDescription === "description")
    assert(r1.getAlphabet === Alphabet.DNA)
    assert(r1.getLength === 4L)
    assert(r1.getSequence === "actg")
    assert(r1.getQualityScores === "BBBB")

    val r2 = reads(1)
    assert(r2.getName === "name2")
    assert(r2.getDescription === "description")
    assert(r2.getAlphabet === Alphabet.DNA)
    assert(r2.getLength === 4L)
    assert(r2.getSequence === "actg")
    assert(r2.getQualityScores === "BBBB")
  }

  sparkTest("convert sequences to slices") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val slices = sequences.toSlices.rdd.collect()
    assert(slices.length === 2)

    val slice1 = slices(0)
    assert(slice1.getName === "name1")
    assert(slice1.getDescription === "description")
    assert(slice1.getAlphabet === Alphabet.DNA)
    assert(slice1.getLength === 4L)
    assert(slice1.getTotalLength === 4L)
    assert(slice1.getSequence === "actg")
    assert(slice1.getStart === 0L)
    assert(slice1.getEnd === 4L)
    assert(slice1.getStrand === Strand.INDEPENDENT)

    val slice2 = slices(1)
    assert(slice2.getName === "name2")
    assert(slice2.getDescription === "description")
    assert(slice2.getAlphabet === Alphabet.DNA)
    assert(slice2.getLength === 4L)
    assert(slice2.getTotalLength === 4)
    assert(slice2.getSequence === "actg")
    assert(slice2.getStart === 0L)
    assert(slice2.getEnd === 4L)
    assert(slice2.getStrand === Strand.INDEPENDENT)
  }

  sparkTest("slice sequences to a maximum length") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val slices = sequences.slice(3L).rdd.collect()
    assert(slices.length === 4)

    slices.sortWith((v1, v2) => ComparisonChain.start()
      .compare(v1.getName, v2.getName)
      .compare(v1.getStart, v2.getStart)
      .result() < 0
    )

    val slice1 = slices(0)
    assert(slice1.getName === "name1")
    assert(slice1.getDescription === "description")
    assert(slice1.getAlphabet === Alphabet.DNA)
    assert(slice1.getLength === 3L)
    assert(slice1.getTotalLength === 4L)
    assert(slice1.getSequence === "act")
    assert(slice1.getStart === 0L)
    assert(slice1.getEnd === 3L)
    assert(slice1.getStrand === Strand.INDEPENDENT)
    assert(slice1.getIndex === 0)
    assert(slice1.getSlices === 2)

    val slice2 = slices(1)
    assert(slice2.getName === "name1")
    assert(slice2.getDescription === "description")
    assert(slice2.getAlphabet === Alphabet.DNA)
    assert(slice2.getLength === 1L)
    assert(slice2.getTotalLength === 4L)
    assert(slice2.getSequence === "g")
    assert(slice2.getStart === 3L)
    assert(slice2.getEnd === 4L)
    assert(slice2.getStrand === Strand.INDEPENDENT)
    assert(slice2.getIndex === 1)
    assert(slice2.getSlices === 2)

    val slice3 = slices(2)
    assert(slice3.getName === "name2")
    assert(slice3.getDescription === "description")
    assert(slice3.getAlphabet === Alphabet.DNA)
    assert(slice3.getLength === 3L)
    assert(slice3.getTotalLength === 4L)
    assert(slice3.getSequence === "act")
    assert(slice3.getStart === 0L)
    assert(slice3.getEnd === 3L)
    assert(slice3.getStrand === Strand.INDEPENDENT)
    assert(slice3.getIndex === 0)
    assert(slice3.getSlices === 2)

    val slice4 = slices(3)
    assert(slice4.getName === "name2")
    assert(slice4.getDescription === "description")
    assert(slice4.getAlphabet === Alphabet.DNA)
    assert(slice4.getLength === 1L)
    assert(slice4.getTotalLength === 4L)
    assert(slice4.getSequence === "g")
    assert(slice4.getStart === 3L)
    assert(slice4.getEnd === 4L)
    assert(slice4.getStrand === Strand.INDEPENDENT)
    assert(slice4.getIndex === 1)
    assert(slice4.getSlices === 2)
  }

  sparkTest("slice sequences shorter than maximum length") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val slices = sequences.slice(10L).rdd.collect()
    assert(slices.length === 2)

    val slice1 = slices(0)
    assert(slice1.getName === "name1")
    assert(slice1.getDescription === "description")
    assert(slice1.getAlphabet === Alphabet.DNA)
    assert(slice1.getLength === 4L)
    assert(slice1.getTotalLength === 4L)
    assert(slice1.getSequence === "actg")
    assert(slice1.getStart === 0L)
    assert(slice1.getEnd === 4L)
    assert(slice1.getStrand === Strand.INDEPENDENT)
    assert(slice1.getIndex === 0)
    assert(slice1.getSlices === 1)

    val slice2 = slices(1)
    assert(slice2.getName === "name2")
    assert(slice2.getDescription === "description")
    assert(slice2.getAlphabet === Alphabet.DNA)
    assert(slice2.getLength === 4L)
    assert(slice2.getTotalLength === 4L)
    assert(slice2.getSequence === "actg")
    assert(slice2.getStart === 0L)
    assert(slice2.getEnd === 4L)
    assert(slice2.getStrand === Strand.INDEPENDENT)
    assert(slice2.getIndex === 0)
    assert(slice2.getSlices === 1)
  }

  sparkTest("filter sequences by overlapping region") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val filtered = sequences.filterByOverlappingRegion(ReferenceRegion("name1", 1L, 3L)).rdd.collect()
    assert(filtered.length == 1)

    val sequence1 = filtered(0)
    assert(sequence1.getName === "name1")
    assert(sequence1.getDescription === "description")
    assert(sequence1.getAlphabet === Alphabet.DNA)
    assert(sequence1.getLength === 4L)
    assert(sequence1.getSequence === "actg")
  }

  sparkTest("filter sequences failing to overlap region") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    assert(sequences.filterByOverlappingRegion(ReferenceRegion("name1", 99L, 101L)).rdd.isEmpty)
  }

  sparkTest("filter sequences by overlapping regions") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val regions = List(ReferenceRegion("name1", 1L, 3L), ReferenceRegion("name2", 1L, 3L))
    val filtered = sequences.filterByOverlappingRegions(regions).rdd.collect()
    assert(filtered.length == 2)

    val sequence1 = filtered(0)
    assert(sequence1.getName === "name1")
    assert(sequence1.getDescription === "description")
    assert(sequence1.getAlphabet === Alphabet.DNA)
    assert(sequence1.getLength === 4L)
    assert(sequence1.getSequence === "actg")

    val sequence2 = filtered(1)
    assert(sequence2.getName === "name2")
    assert(sequence2.getDescription === "description")
    assert(sequence2.getAlphabet === Alphabet.DNA)
    assert(sequence2.getLength === 4L)
    assert(sequence2.getSequence === "actg")
  }

  sparkTest("filter sequences failing to overlap regions") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val regions = List(ReferenceRegion("name1", 99L, 101L), ReferenceRegion("name2", 99L, 101L))
    assert(sequences.filterByOverlappingRegions(regions).rdd.isEmpty)
  }

  sparkTest("slice sequences overlapping a smaller region") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val slices = sequences.slice(ReferenceRegion("name1", 1L, 3L)).rdd.collect()
    assert(slices.length === 1)

    val slice1 = slices(0)
    assert(slice1.getName === "name1")
    assert(slice1.getDescription === "description")
    assert(slice1.getAlphabet === Alphabet.DNA)
    assert(slice1.getLength === 2L)
    assert(slice1.getTotalLength === 4L)
    assert(slice1.getSequence === "ct")
    assert(slice1.getStart === 1L)
    assert(slice1.getEnd === 3L)
    assert(slice1.getStrand === Strand.INDEPENDENT)
  }

  sparkTest("slice sequences overlapping a larger region") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val slices = sequences.slice(ReferenceRegion("name1", 0L, 99L)).rdd.collect()
    assert(slices.length === 1)

    val slice1 = slices(0)
    assert(slice1.getName === "name1")
    assert(slice1.getDescription === "description")
    assert(slice1.getAlphabet === Alphabet.DNA)
    assert(slice1.getLength === 4L)
    assert(slice1.getTotalLength === 4L)
    assert(slice1.getSequence === "actg")
    assert(slice1.getStart === 0L)
    assert(slice1.getEnd === 4L)
    assert(slice1.getStrand === Strand.INDEPENDENT)
  }

  sparkTest("slice sequences failing to overlap a region") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val slices = sequences.slice(ReferenceRegion("name1", 99L, 101L)).rdd.collect()
    assert(slices.length === 0)
  }

  sparkTest("slice sequences overlapping smaller regions") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val regions = List(ReferenceRegion("name1", 1L, 3L), ReferenceRegion("name2", 1L, 3L))
    val slices = sequences.slice(regions).rdd.collect()
    assert(slices.length === 2)

    val slice1 = slices(0)
    assert(slice1.getName === "name1")
    assert(slice1.getDescription === "description")
    assert(slice1.getAlphabet === Alphabet.DNA)
    assert(slice1.getLength === 2L)
    assert(slice1.getTotalLength === 4L)
    assert(slice1.getSequence === "ct")
    assert(slice1.getStart === 1L)
    assert(slice1.getEnd === 3L)
    assert(slice1.getStrand === Strand.INDEPENDENT)

    val slice2 = slices(1)
    assert(slice2.getName === "name2")
    assert(slice2.getDescription === "description")
    assert(slice2.getAlphabet === Alphabet.DNA)
    assert(slice2.getLength === 2L)
    assert(slice2.getTotalLength === 4L)
    assert(slice2.getSequence === "ct")
    assert(slice2.getStart === 1L)
    assert(slice2.getEnd === 3L)
    assert(slice2.getStrand === Strand.INDEPENDENT)
  }

  sparkTest("slice sequences overlapping larger regions") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val regions = List(ReferenceRegion("name1", 0L, 99L), ReferenceRegion("name2", 0L, 99L))
    val slices = sequences.slice(regions).rdd.collect()
    assert(slices.length === 2)

    val slice1 = slices(0)
    assert(slice1.getName === "name1")
    assert(slice1.getDescription === "description")
    assert(slice1.getAlphabet === Alphabet.DNA)
    assert(slice1.getLength === 4L)
    assert(slice1.getTotalLength === 4L)
    assert(slice1.getSequence === "actg")
    assert(slice1.getStart === 0L)
    assert(slice1.getEnd === 4L)
    assert(slice1.getStrand === Strand.INDEPENDENT)

    val slice2 = slices(1)
    assert(slice2.getName === "name2")
    assert(slice2.getDescription === "description")
    assert(slice2.getAlphabet === Alphabet.DNA)
    assert(slice2.getLength === 4L)
    assert(slice2.getTotalLength === 4L)
    assert(slice2.getSequence === "actg")
    assert(slice2.getStart === 0L)
    assert(slice2.getEnd === 4L)
    assert(slice2.getStrand === Strand.INDEPENDENT)
  }

  sparkTest("slice sequences failing to overlap regions") {
    val sequences: SequenceDataset = SequenceDataset(sc.parallelize(Seq(s1, s2)))
    val regions = List(ReferenceRegion("name1", 99L, 101L), ReferenceRegion("name2", 99L, 101L))
    val slices = sequences.slice(regions).rdd.collect()
    assert(slices.length === 0)
  }
}
