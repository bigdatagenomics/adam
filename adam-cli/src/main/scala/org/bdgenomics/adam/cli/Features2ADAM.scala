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
package org.bdgenomics.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.BaseFeature
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.features.ADAMFeaturesContext._
import org.bdgenomics.formats.avro.Feature
import org.kohsuke.args4j.Argument

object Features2ADAM extends ADAMCommandCompanion {
  val commandName = "features2adam"
  val commandDescription = "Convert a file with sequence features into corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Features2ADAM(Args4j[Features2ADAMArgs](cmdLine))
  }
}

class Features2ADAMArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "FEATURES",
    usage = "The features file to convert (e.g., .bed, .gff)", index = 0)
  var featuresFile: String = _
  @Argument(required = true, metaVar = "ADAM",
    usage = "Location to write ADAM features data", index = 1)
  var outputPath: String = null
}

class Features2ADAM(val args: Features2ADAMArgs)
    extends ADAMSparkCommand[Features2ADAMArgs] {
  val companion = Features2ADAM

  def run(sc: SparkContext, job: Job) {
    // get file extension
    // regex: anything . (extension) EOL
    val extensionPattern = """.*[.]([^.]*)$""".r
    val extensionPattern(extension) = args.featuresFile
    val features: RDD[Feature] = extension.toLowerCase match {
      case "gff"        => sc.adamGTFFeatureLoad(args.featuresFile) // TODO(Timothy) write a GFF-specific loader?
      case "gtf"        => sc.adamGTFFeatureLoad(args.featuresFile)
      case "bed"        => sc.adamBEDFeatureLoad(args.featuresFile)
      case "narrowPeak" => sc.adamNarrowPeakFeatureLoad(args.featuresFile)
    }
    features.adamSave(args.outputPath, blockSize = args.blockSize,
      pageSize = args.pageSize, compressCodec = args.compressionCodec,
      disableDictionaryEncoding = args.disableDictionary)
  }
}
