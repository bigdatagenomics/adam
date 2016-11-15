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
import org.bdgenomics.adam.projections.RecordGroupMetadataField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.RecordGroupMetadata

class RecordGroupMetadataFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet record group metadata") {
    val path = tmpFile("recordGroupMetadata.parquet")
    val rdd = sc.parallelize(Seq(RecordGroupMetadata.newBuilder()
      .setName("name")
      .setSequencingCenter("sequencing_center")
      .setDescription("description")
      .setRunDateEpoch(42L)
      .setFlowOrder("flow_order")
      .setKeySequence("key_sequence")
      .setLibrary("library")
      .setPredictedMedianInsertSize(99)
      .setPlatform("platform")
      .setPlatformUnit("platform_unit")
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      name,
      sample,
      sequencingCenter,
      description,
      runDateEpoch,
      flowOrder,
      keySequence,
      library,
      predictedMedianInsertSize,
      platform,
      platformUnit
    )

    val recordGroupMetadata: RDD[RecordGroupMetadata] = sc.loadParquet(path, projection = Some(projection))
    assert(recordGroupMetadata.count() === 1)
    assert(recordGroupMetadata.first.getName === "name")
    assert(recordGroupMetadata.first.getSequencingCenter === "sequencing_center")
    assert(recordGroupMetadata.first.getDescription === "description")
    assert(recordGroupMetadata.first.getRunDateEpoch === 42L)
    assert(recordGroupMetadata.first.getFlowOrder === "flow_order")
    assert(recordGroupMetadata.first.getKeySequence === "key_sequence")
    assert(recordGroupMetadata.first.getLibrary === "library")
    assert(recordGroupMetadata.first.getPredictedMedianInsertSize === 99)
    assert(recordGroupMetadata.first.getPlatform === "platform")
    assert(recordGroupMetadata.first.getPlatformUnit === "platform_unit")
  }
}
