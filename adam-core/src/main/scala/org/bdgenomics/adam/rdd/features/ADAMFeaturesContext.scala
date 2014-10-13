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

import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Feature

object ADAMFeaturesContext {
  implicit def sparkContextToADAMFeaturesContext(sc: SparkContext): ADAMFeaturesContext = new ADAMFeaturesContext(sc)
}

class ADAMFeaturesContext(sc: SparkContext) extends Serializable with Logging {

  def adamGTFFeatureLoad(filePath: String): RDD[Feature] = {
    sc.textFile(filePath).flatMap(new GTFParser().parse)
  }

  def adamBEDFeatureLoad(filePath: String): RDD[Feature] = {
    sc.textFile(filePath).flatMap(new BEDParser().parse)
  }

  def adamNarrowPeakFeatureLoad(filePath: String): RDD[Feature] = {
    sc.textFile(filePath).flatMap(new NarrowPeakParser().parse)
  }
}
