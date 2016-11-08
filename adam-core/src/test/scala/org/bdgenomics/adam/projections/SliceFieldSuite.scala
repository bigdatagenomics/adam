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
import org.bdgenomics.adam.projections.SliceField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  Alphabet,
  Slice,
  Strand
}

class SliceFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet slices") {
    val path = tmpFile("slices.parquet")
    val rdd = sc.parallelize(Seq(Slice.newBuilder()
      .setName("6")
      .setDescription("Chromosome 6")
      .setAlphabet(Alphabet.DNA)
      .setSequence("ACTG")
      .setStart(0)
      .setEnd(4)
      .setStrand(Strand.FORWARD)
      .setLength(4)
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      name,
      description,
      alphabet,
      sequence,
      start,
      end,
      strand,
      length
    )

    val slices: RDD[Slice] = sc.loadParquet(path, projection = Some(projection));
    assert(slices.count() === 1)
    assert(slices.first.getName === "6")
    assert(slices.first.getDescription === "Chromosome 6")
    assert(slices.first.getAlphabet === Alphabet.DNA)
    assert(slices.first.getSequence === "ACTG")
    assert(slices.first.getStart === 0)
    assert(slices.first.getEnd === 4)
    assert(slices.first.getStrand === Strand.FORWARD)
    assert(slices.first.getLength === 4)
  }
}
