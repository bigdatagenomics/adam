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
import org.bdgenomics.adam.projections.SequenceField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ Alphabet, Sequence }

class SequenceFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet sequences") {
    val path = tmpFile("sequences.parquet")
    val rdd = sc.parallelize(Seq(Sequence.newBuilder()
      .setName("6")
      .setDescription("Chromosome 6")
      .setAlphabet(Alphabet.DNA)
      .setSequence("ACTG")
      .setLength(4)
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      name,
      description,
      alphabet,
      sequence,
      length
    )

    val sequences: RDD[Sequence] = sc.loadParquet(path, projection = Some(projection));
    assert(sequences.count() === 1)
    assert(sequences.first.getName === "6")
    assert(sequences.first.getDescription === "Chromosome 6")
    assert(sequences.first.getAlphabet === Alphabet.DNA)
    assert(sequences.first.getSequence === "ACTG")
    assert(sequences.first.getLength === 4)
  }
}
