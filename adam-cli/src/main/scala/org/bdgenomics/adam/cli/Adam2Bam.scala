package org.bdgenomics.adam.cli

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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.utils.cli.{ Args4j, Args4jBase, BDGCommandCompanion, BDGSparkCommand, ParquetArgs }
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Adam2Bam extends BDGCommandCompanion {
  val commandName = "adam2bam"
  val commandDescription = "Convert ADAM to SAM/BAM format"

  def apply(cmdLine: Array[String]) = {
    new Adam2Bam(Args4j[Adam2BamArgs](cmdLine))
  }
}

class Adam2BamArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM file to apply the transforms to", index = 0)
  var inputPath: String = null
  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write BAM/SAM file", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-sort_fastq_output", usage = "Sets whether to sort the FASTQ output, if saving as FASTQ. False by default. Ignored if not saving as FASTQ.")
  var sortFastqOutput: Boolean = false
}

class Adam2Bam(protected val args: Adam2BamArgs) extends BDGSparkCommand[Adam2BamArgs] with Logging {
  val companion = Adam2Bam

  def run(sc: SparkContext) {
    if (sc.loadParquetAlignments(args.inputPath).adamSave(args)) {
      //make regular BAM/SAM file (not hadopped)
      log.info("make regular BAM/SAM file (not hadopped)")
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      val ouputParentDir = args.outputPath.substring(0, args.outputPath.lastIndexOf("/") + 1)
      val tmpPath = ouputParentDir + "tmp" + System.currentTimeMillis().toString
      fs.rename(new Path(args.outputPath + "/part-r-00000"), new Path(tmpPath))
      fs.delete(new Path(args.outputPath), true)
      fs.rename(new Path(tmpPath), new Path(args.outputPath))
    } else {
      log.error("Something went wrong...")
    }
  }

}
