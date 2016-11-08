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
import org.bdgenomics.adam.projections.ContigField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Contig

class ContigFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet contigs") {
    val path = tmpFile("contigs.parquet")
    val rdd = sc.parallelize(Seq(Contig.newBuilder()
      .setContigName("6")
      .setContigLength(170805979)
      .setContigMD5("013a29a149b249bb119d27368bb6bf52")
      .setReferenceURL("http://www.ebi.ac.uk/ena/data/view/GCA_000001405.22")
      .setAssembly("GRCh38")
      .setSpecies("Homo sapiens")
      .setReferenceIndex(0)
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      contigName,
      contigLength,
      contigMD5,
      referenceURL,
      assembly,
      species,
      referenceIndex
    )

    val contigs: RDD[Contig] = sc.loadParquet(path, projection = Some(projection))
    assert(contigs.count() === 1)
    assert(contigs.first.getContigName === "6")
    assert(contigs.first.getContigLength === 170805979)
    assert(contigs.first.getContigMD5 === "013a29a149b249bb119d27368bb6bf52")
    assert(contigs.first.getReferenceURL === "http://www.ebi.ac.uk/ena/data/view/GCA_000001405.22")
    assert(contigs.first.getAssembly === "GRCh38")
    assert(contigs.first.getSpecies === "Homo sapiens")
    assert(contigs.first.getReferenceIndex === 0)
  }
}
