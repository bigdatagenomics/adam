/*
 * Copyright (c) 2013. Regents of the University of California
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

import java.lang.{Integer => JInt}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.SparkContext._
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.{VariantContextWritable, VCFOutputFormat, VCFFormat}
import edu.berkeley.cs.amplab.adam.converters.VariantContextConverter

object Adam2Vcf extends AdamCommandCompanion {

  val commandName = "adam2vcf"
  val commandDescription = "Convert an ADAM variant to the VCF ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Adam2Vcf(Args4j[Adam2VcfArgs](cmdLine))
  }
}

class Adam2VcfArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "ADAM", usage = "The ADAM variant files to convert", index = 0)
  var adamFile: String = _
  @Argument(required = true, metaVar = "VCF", usage = "Location to write VCF data", index = 1)
  var outputPath: String = null
}

class Adam2Vcf(val args: Adam2VcfArgs) extends AdamSparkCommand[Adam2VcfArgs] with Logging {
  val companion = Adam2Vcf

  def run(sc: SparkContext, job: Job) {

    // val adamVC: RDD[ADAMVariantContext] = sc.adamVariantContextLoad(args.adamFile)

    // val converter = new VariantContextConverter

    // // convert all variant contexts
    // val variantContexts: RDD[VariantContextWritable] = adamVC.map(r => {
    //   // create new variant context writable
    //   val vcw = new VariantContextWritable
    //   val vc = converter.convert(r)
    //   vcw.set(vc)

    //   vcw
    // })

    // // add index for writing output format
    // val mapIndex = variantContexts.keyBy(r => new LongWritable(r.get.getStart))

    // log.info("Counted " + variantContexts.count + " variants.")

    // // new context
    // val conf = sc.hadoopConfiguration
    // val vcfFormat = VCFFormat.valueOf("VCF")
    // conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY,
    //   vcfFormat.toString)

    // // write out
    // mapIndex.saveAsNewAPIHadoopFile(args.outputPath, classOf[LongWritable], classOf[VariantContextWritable],
    //   classOf[VCFOutputFormat[LongWritable]])
  }
}
