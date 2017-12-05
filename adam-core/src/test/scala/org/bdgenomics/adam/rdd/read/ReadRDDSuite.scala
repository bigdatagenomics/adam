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

import java.io.File

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  Alphabet,
  Feature,
  QualityScoreVariant,
  Read,
  Strand
}

class ReadRDDSuite extends ADAMFunSuite {

  val r1 = Read.newBuilder()
    .setName("name1")
    .setDescription("description")
    .setAlphabet(Alphabet.DNA)
    .setLength(4L)
    .setSequence("actg")
    .setQualityScores("9999")
    .setQualityScoreVariant(QualityScoreVariant.FASTQ_SANGER)
    .build

  val r2 = Read.newBuilder()
    .setName("name2")
    .setDescription("description")
    .setAlphabet(Alphabet.DNA)
    .setLength(4L)
    .setSequence("actg")
    .setQualityScores("9999")
    .setQualityScoreVariant(QualityScoreVariant.FASTQ_SANGER)
    .build

  val sd = SequenceDictionary(
    SequenceRecord("name1", 4),
    SequenceRecord("name2", 4)
  )

  def tempLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("SequenceRDDSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  sparkTest("create a new read rdd") {
    val reads: RDD[Read] = sc.parallelize(Seq(r1, r2))
    assert(ReadRDD(reads).rdd.count === 2)
  }

  sparkTest("create a new read rdd with sequence dictionary") {
    val reads: RDD[Read] = sc.parallelize(Seq(r1, r2))
    assert(ReadRDD(reads, sd).rdd.count === 2)
  }

  sparkTest("save as parquet") {
    val reads: ReadRDD = ReadRDD(sc.parallelize(Seq(r1, r2)))
    val outputPath = tempLocation(".adam")
    reads.save(outputPath, asSingleFile = false)
  }

  sparkTest("save as fastq") {
    val reads: ReadRDD = ReadRDD(sc.parallelize(Seq(r1, r2)))
    val outputPath = tempLocation(".fastq")
    reads.save(outputPath, asSingleFile = false)
  }

  sparkTest("save as single file fastq") {
    val reads: ReadRDD = ReadRDD(sc.parallelize(Seq(r1, r2)))
    val outputPath = tempLocation(".fastq")
    reads.save(outputPath, asSingleFile = true)
  }

  sparkTest("filter read rdd by reference region") {
    val reads: ReadRDD = ReadRDD(sc.parallelize(Seq(r1, r2)))
    val filtered = reads.filterByOverlappingRegion(ReferenceRegion.all("name1"))
    assert(filtered.rdd.count() === 1)
  }

  sparkTest("broadcast region join reads and features") {
    val feature = Feature.newBuilder()
      .setContigName("name2")
      .setStart(0L)
      .setEnd(3L)
      .build

    val reads: ReadRDD = ReadRDD(sc.parallelize(Seq(r1, r2)))
    val features: FeatureRDD = FeatureRDD(sc.parallelize(Seq(feature)))

    val kv = reads.broadcastRegionJoin(features).rdd.first
    assert(kv._1 === r2)
    assert(kv._2 === feature)
  }

  sparkTest("shuffle region join reads and features") {
    val feature = Feature.newBuilder()
      .setContigName("name1")
      .setStart(0L)
      .setEnd(3L)
      .build

    val reads: ReadRDD = ReadRDD(sc.parallelize(Seq(r1, r2)))
    val features: FeatureRDD = FeatureRDD(sc.parallelize(Seq(feature)))

    val kv = reads.broadcastRegionJoin(features).rdd.first
    assert(kv._1 === r1)
    assert(kv._2 === feature)
  }

  sparkTest("convert reads to sequences") {
    val reads: ReadRDD = ReadRDD(sc.parallelize(Seq(r1, r2)))
    val sequences = reads.toSequences.rdd.collect()
    assert(sequences.length === 2)

    val s1 = sequences(0)
    assert(s1.getName === "name1")
    assert(s1.getDescription === "description")
    assert(s1.getAlphabet === Alphabet.DNA)
    assert(s1.getLength === 4L)
    assert(s1.getSequence === "actg")

    val s2 = sequences(1)
    assert(s2.getName === "name2")
    assert(s2.getDescription === "description")
    assert(s2.getAlphabet === Alphabet.DNA)
    assert(s2.getLength === 4L)
    assert(s2.getSequence === "actg")
  }

  sparkTest("convert reads to slices") {
    val reads: ReadRDD = ReadRDD(sc.parallelize(Seq(r1, r2)))
    val slices = reads.toSlices.rdd.collect()
    assert(slices.length === 2)

    val s1 = slices(0)
    assert(s1.getName === "name1")
    assert(s1.getDescription === "description")
    assert(s1.getAlphabet === Alphabet.DNA)
    assert(s1.getLength === 4L)
    assert(s1.getTotalLength === 4L)
    assert(s1.getSequence === "actg")
    assert(s1.getStart === 0L)
    assert(s1.getEnd === 4L)
    assert(s1.getStrand === Strand.INDEPENDENT)

    val s2 = slices(1)
    assert(s2.getName === "name2")
    assert(s2.getDescription === "description")
    assert(s2.getAlphabet === Alphabet.DNA)
    assert(s2.getLength === 4L)
    assert(s2.getTotalLength === 4L)
    assert(s2.getSequence === "actg")
    assert(s2.getStart === 0L)
    assert(s2.getEnd === 4L)
    assert(s2.getStrand === Strand.INDEPENDENT)
  }
}
