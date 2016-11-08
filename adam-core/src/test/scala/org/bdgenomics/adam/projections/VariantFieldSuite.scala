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

import com.google.common.collect.ImmutableList
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.VariantField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Variant

class VariantFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet variants") {
    val path = tmpFile("variants.parquet")
    val rdd = sc.parallelize(Seq(Variant.newBuilder()
      .setContigName("6")
      .setStart(29941260L)
      .setEnd(29941261L)
      .setNames(ImmutableList.of("rs3948572"))
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .setFiltersApplied(true)
      .setFiltersPassed(false)
      .setFiltersFailed(ImmutableList.of("filter"))
      .setSomatic(false)
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      contigName,
      start,
      end,
      names,
      referenceAllele,
      alternateAllele,
      filtersApplied,
      filtersPassed,
      filtersFailed,
      somatic
    )

    val variants: RDD[Variant] = sc.loadParquet(path, projection = Some(projection))
    assert(variants.count() === 1)
    assert(variants.first.getContigName === "6")
    assert(variants.first.getStart === 29941260L)
    assert(variants.first.getEnd === 29941261L)
    assert(variants.first.getNames.get(0) === "rs3948572")
    assert(variants.first.getReferenceAllele === "A")
    assert(variants.first.getAlternateAllele === "T")
    assert(variants.first.getFiltersApplied === true)
    assert(variants.first.getFiltersPassed === false)
    assert(variants.first.getFiltersFailed.get(0) === "filter")
    assert(variants.first.getSomatic === false)
  }
}
