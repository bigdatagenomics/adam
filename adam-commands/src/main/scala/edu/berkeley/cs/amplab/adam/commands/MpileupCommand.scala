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

package edu.berkeley.cs.amplab.adam.commands

import edu.berkeley.cs.amplab.adam.util.{Args4j, Args4jBase}
import org.kohsuke.args4j.Argument
import parquet.hadoop.ParquetInputFormat
import parquet.avro.AvroReadSupport
import edu.berkeley.cs.amplab.adam.avro.ADAMPileup
import parquet.hadoop.util.ContextUtil
import org.apache.hadoop.mapreduce.Job
import spark.SparkContext._
import edu.berkeley.cs.amplab.adam.avro.AvroWrapper

class MpileupArgs extends Args4jBase with SparkArgs {
  @Argument(required = true, metaVar = "FILE", usage = "ADAM reference-oriented file")
  var rodFile: String = _
}

class MpileupCommand extends AdamCommand with SparkCommand {
  val commandName: String = "mpileup"
  val commandDescription: String = "Output the samtool mpileup text from ADAM reference-oriented data"

  def commandExec(cmdLine: Array[String]) {
    var args = Args4j[MpileupArgs](cmdLine)
    val sc = createSparkContext(args)
    val job = new Job()

    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[ADAMPileup]])
    val pileups = sc.newAPIHadoopFile(args.rodFile,
      classOf[ParquetInputFormat[ADAMPileup]], classOf[Void], classOf[ADAMPileup],
      ContextUtil.getConfiguration(job))

    val sorted = pileups.map {
      case (_, pileup: ADAMPileup) => {
        ((pileup.getReferenceId, pileup.getPosition), AvroWrapper[ADAMPileup](pileup))
      }
    }.sortByKey(numPartitions = 10000)

    sorted.foreach(println)
  }
}
