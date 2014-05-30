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
package org.bdgenomics.adam.parquet_reimpl

import org.bdgenomics.adam.projections.ADAMFlatGenotypeField._
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.formats.avro.ADAMFlatGenotype
import org.scalatest.FunSuite

class ParquetListerSuite extends FunSuite {

  test("can list a single small parquet file") {
    val filename = "small_adam.fgenotype"
    val path = Thread.currentThread().getContextClassLoader.getResource(filename).getPath
    val lister = new ParquetLister[ADAMFlatGenotype]()

    val recs = lister.materialize(path).toSeq
    assert(recs.length === 15)
  }

  test("can list all the records in a directory") {
    val filename = "parquet_lister_dir"
    val path = Thread.currentThread().getContextClassLoader.getResource(filename).getPath
    val lister = new ParquetLister[ADAMFlatGenotype]()

    val recs = lister.materialize(path).toSeq
    assert(recs.length === 30)
  }

  test("directories with no parquet files should have 0 records") {
    val filename = "parquet_lister_dir_empty"
    val path = Thread.currentThread().getContextClassLoader.getResource(filename).getPath
    val lister = new ParquetLister[ADAMFlatGenotype]()

    val recs = lister.materialize(path).toSeq
    assert(recs.length === 0)
  }

  test("uses projections properly") {
    val proj = Projection(referenceAllele)
    val filename = "small_adam.fgenotype"
    val path = Thread.currentThread().getContextClassLoader.getResource(filename).getPath
    val lister = new ParquetLister[ADAMFlatGenotype](Some(proj))

    val recs = lister.materialize(path).toSeq
    assert(recs.length === 15)
    assert(recs.forall(_.referenceName == null))
    assert(recs.forall(_.referenceAllele != null))
  }

}
