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
package org.bdgenomics.adam.rdd.features

import org.bdgenomics.adam.util.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.features.ADAMFeaturesContext._
import org.bdgenomics.formats.avro.Feature

class FeatureParsingSuite extends SparkFunSuite {

  sparkTest("Can read a .gtf file") {
    val path = testFile("features/Homo_sapiens.GRCh37.75.trun20.gtf")
    val features: RDD[Feature] = sc.adamGTFFeatureLoad(path)
    assert(features.count === 15)
  }

  sparkTest("Can read a .bed file") {
    // note: this .bed doesn't actually conform to the UCSC BED spec...sigh...
    val path = testFile("features/gencode.v7.annotation.trunc10.bed")
    val features: RDD[Feature] = sc.adamBEDFeatureLoad(path)
    assert(features.count === 10)
  }

  sparkTest("Can read a .narrowPeak file") {
    val path = testFile("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val annot: RDD[Feature] = sc.adamNarrowPeakFeatureLoad(path)
    assert(annot.count === 10)
  }
}
