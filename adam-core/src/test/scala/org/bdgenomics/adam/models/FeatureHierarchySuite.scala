/*
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
package org.bdgenomics.adam.models

import org.bdgenomics.formats.avro.{ Contig, Feature }
import org.scalatest.FunSuite

class FeatureHierarchySuite extends FunSuite {

  test("Feature values converted into BaseFeatures retain their aligned positions in the fields of ReferenceRegion") {
    val contig = Contig.newBuilder().setContigName("chr1").build()
    val f = Feature.newBuilder()
      .setContig(contig)
      .setStart(1000L)
      .setEnd(2000L)
      .setFeatureId("id")
      .build()

    val bf = new BaseFeature(f)

    assert(bf.referenceName === "chr1")
    assert(bf.start === 1000L)
    assert(bf.end === 2000L)
    assert(bf.featureId === "id")
  }
}
