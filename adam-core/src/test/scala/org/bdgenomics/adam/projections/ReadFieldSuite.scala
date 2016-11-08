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
package org.bdgenomics.adam.projections

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.ReadField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  Alphabet,
  QualityScoreVariant,
  Read
}

class ReadFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet reads") {
    val path = tmpFile("reads.parquet")
    val rdd = sc.parallelize(Seq(Read.newBuilder()
      .setName("read 1")
      .setDescription("read 1")
      .setAlphabet(Alphabet.DNA)
      .setSequence("ACTG")
      .setLength(4)
      .setQualityScores("0123")
      .setQualityScoreVariant(QualityScoreVariant.FASTQ_SANGER)
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      name,
      description,
      alphabet,
      sequence,
      length,
      qualityScores,
      qualityScoreVariant
    )

    val reads: RDD[Read] = sc.loadParquet(path, projection = Some(projection))
    assert(reads.count() === 1)
    assert(reads.first.getName === "read 1")
    assert(reads.first.getDescription === "read 1")
    assert(reads.first.getAlphabet === Alphabet.DNA)
    assert(reads.first.getSequence === "ACTG")
    assert(reads.first.getLength === 4)
    assert(reads.first.getQualityScores === "0123")
    assert(reads.first.getQualityScoreVariant === QualityScoreVariant.FASTQ_SANGER)
  }
}
