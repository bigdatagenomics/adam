/*
 * Copyright (c) 2014. Cloudera Inc.
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

package edu.berkeley.cs.amplab.adam.cli

import org.apache.spark.{SparkContext, Logging}
import org.kohsuke.args4j.{Option, Argument}
import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.annotation.ADAMAnnotationContext._
import edu.berkeley.cs.amplab.adam.rdd.annotation.AnnotationMetadata

object Annot2Adam extends AdamCommandCompanion {
  val commandName = "annot2adam"
  val commandDescription = "Convert an annotation file to ADAMAnnotation format"

  def apply(cmdLine: Array[String]) = {
    new Annot2Adam(Args4j[Annot2AdamArgs](cmdLine))
  }
}

class Annot2AdamArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "ANNOT", usage = "The annotation file to convert", index = 0)
  var annotFile: String = _
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM Annotation data", index = 1)
  var outputPath: String = null
  @Option(required = false, name = "-name", usage = "Track name for this annotation")
  var name: String = null
  @Option(required = false, name = "-experiment", usage = "Experiment identifier for this annotation")
  var experiment: String = null
  @Option(required = false, name = "-sample", usage = "Sample identifier for this annotation")
  var sample: String = null
}

class Annot2Adam(val args: Annot2AdamArgs) extends AdamSparkCommand[Annot2AdamArgs] with Logging {
  val companion = Annot2Adam

  def run(sc: SparkContext, job: Job) {
    val metadata = new AnnotationMetadata(name = args.name, experiment = args.experiment, sample = args.sample)
    sc.adamAnnotLoad(args.annotFile, metadata).adamSave(args.outputPath, blockSize = args.blockSize,
      pageSize = args.pageSize, compressCodec = args.compressionCodec,
      disableDictionaryEncoding = args.disableDictionary)
  }
}
