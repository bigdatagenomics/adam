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

import java.io.{ BufferedWriter, OutputStreamWriter }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.{ GenotypesSummary, GenotypesSummaryFormatting }
import org.bdgenomics.formats.avro.Genotype
import org.kohsuke.args4j

object SummarizeGenotypes extends ADAMCommandCompanion {

  val commandName = "summarize_genotypes"
  val commandDescription = "Print statistics of genotypes and variants in an ADAM file"

  def apply(cmdLine: Array[String]) = {
    new SummarizeGenotypes(Args4j[SummarizeGenotypesArgs](cmdLine))
  }
}

class SummarizeGenotypesArgs extends Args4jBase with ParquetArgs {
  @args4j.Argument(required = true, metaVar = "ADAM", usage = "The ADAM genotypes file to print stats for", index = 0)
  var adamFile: String = _

  @args4j.Option(required = false, name = "-format", usage = "Format: one of human, csv. Default: human.")
  var format: String = "human"

  @args4j.Option(required = false, name = "-out", usage = "Write output to the given file.")
  var out: String = ""
}

class SummarizeGenotypes(val args: SummarizeGenotypesArgs) extends ADAMSparkCommand[SummarizeGenotypesArgs] with Logging {
  val companion = SummarizeGenotypes

  def run(sc: SparkContext, job: Job) {
    val adamGTs: RDD[Genotype] = sc.adamLoad(args.adamFile)
    val stats = GenotypesSummary(adamGTs)
    val result = args.format match {
      case "human" => GenotypesSummaryFormatting.format_human_readable(stats)
      case "csv"   => GenotypesSummaryFormatting.format_csv(stats)
      case _       => throw new IllegalArgumentException("Invalid -format: %s".format(args.format))
    }
    if (args.out.isEmpty) {
      println(result)
    } else {
      val filesystem = FileSystem.get(new Configuration())
      val path = new Path(args.out)
      val writer = new BufferedWriter(new OutputStreamWriter(filesystem.create(path, true)))
      writer.write(result)
      writer.close()
      println("Wrote: %s".format(args.out))
    }
  }
}
