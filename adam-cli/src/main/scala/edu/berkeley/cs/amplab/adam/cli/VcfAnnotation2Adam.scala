/*
* Copyright (c) 2014. Mount Sinai School of Medicine
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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Logging, SparkContext}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import edu.berkeley.cs.amplab.adam.converters.VariantAnnotationConverter
import edu.berkeley.cs.amplab.adam.rich.RichADAMVariant

object VcfAnnotation2Adam extends AdamCommandCompanion {

  val commandName = "anno2adam"
  val commandDescription = "Convert a annotation file (in VCF format) to the corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new VcfAnnotation2Adam(Args4j[VcfAnnotation2AdamArgs](cmdLine))
  }
}

class VcfAnnotation2AdamArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "VCF", usage = "The VCF file with annotations to convert", index = 0)
  var vcfFile: String = _
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM Variant annotations data", index = 1)
  var outputPath: String = null
  @Args4jOption(required=false, name = "-current-db", usage = "Location of existing ADAM Variant annotations data")
  var currentAnnotations: String = null
}

class VcfAnnotation2Adam(val args: VcfAnnotation2AdamArgs) extends AdamSparkCommand[VcfAnnotation2AdamArgs] with Logging {
  val companion = VcfAnnotation2Adam

  def run(sc: SparkContext, job: Job) {
    log.info("Reading VCF file from %s".format(args.vcfFile))
    val annotations : RDD[ADAMDatabaseVariantAnnotation] = sc.adamVCFAnnotationLoad(args.vcfFile)
    log.info("Converted %d records".format(annotations.count))

    if (args.currentAnnotations != null) {
      val existingAnnotations : RDD[ADAMDatabaseVariantAnnotation] = sc.adamLoad(args.currentAnnotations)
      val keyedAnnotations = existingAnnotations.keyBy(anno => new RichADAMVariant(anno.getVariant))
      val joinedAnnotations = keyedAnnotations.join( annotations.keyBy(anno => new RichADAMVariant(anno.getVariant)))
      val mergedAnnotations = joinedAnnotations.map( kv => VariantAnnotationConverter.mergeAnnotations( kv._2._1, kv._2._2 ))
      mergedAnnotations.adamSave(args.outputPath, blockSize = args.blockSize, pageSize = args.pageSize,
        compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
    }
    else {
      annotations.adamSave(args.outputPath, blockSize = args.blockSize, pageSize = args.pageSize,
        compressCodec = args.compressionCodec, disableDictionaryEncoding = args.disableDictionary)
    }

    log.info("Added %d annotation records".format(annotations.count()))

  }

}