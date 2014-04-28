/*
 * Copyright (c) 2014. Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.bdgenomics.adam.util.HadoopUtil
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.BaseFeature

object ADAMFeaturesContext {
  implicit def sparkContextToADAMFeaturesContext(sc: SparkContext): ADAMFeaturesContext = new ADAMFeaturesContext(sc)
}

class ADAMFeaturesContext(sc: SparkContext) extends Serializable with Logging {

  def adamFeatureLoad(filePath: String): RDD[_ <: BaseFeature] = {
    val job = HadoopUtil.newJob(sc)
    // regex: anything . (extenstion) EOL
    val extensionPattern = """.*[.]([^.]*)$""".r
    val extensionPattern(extension) = filePath
    extension.toLowerCase match {
      case "bed"        => sc.textFile(filePath).map(new BEDParser().parse _)
      case "narrowPeak" => sc.textFile(filePath).map(new NarrowPeakParser().parse _)
    }
  }
}
