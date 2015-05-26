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
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => option, Argument }

object PrintGenes extends BDGCommandCompanion {
  val commandName: String = "print_genes"
  val commandDescription: String = "Load a GTF file containing gene annotations and print the corresponding gene models"

  def apply(cmdLine: Array[String]) = {
    new PrintGenes(Args4j[PrintGenesArgs](cmdLine))
  }
}

class PrintGenesArgs extends Args4jBase with ParquetArgs with Serializable {
  @Argument(metaVar = "GTF", required = true, usage = "GTF file with gene model data", index = 0)
  var gtfInput: String = _
}

class PrintGenes(protected val args: PrintGenesArgs)
    extends BDGSparkCommand[PrintGenesArgs] with Serializable {

  val companion = PrintGenes

  def run(sc: SparkContext): Unit = {
    val genes: RDD[Gene] = sc.loadGenes(args.gtfInput)

    genes.map(printGene).collect().foreach(println)
  }

  def printGene(gene: Gene): String = {
    val builder = new StringBuilder()
    builder.append("Gene %s (%s)".format(gene.id, gene.names.mkString(",")))
    gene.transcripts.foreach(t => builder.append(printTranscript(t)))
    builder.toString()
  }

  def printTranscript(transcript: Transcript): String = {
    "\n\tTranscript %s %s:%d-%d:%s (%d exons)".format(
      transcript.id,
      transcript.region.referenceName,
      transcript.region.start, transcript.region.end,
      if (transcript.strand) "+" else "-",
      transcript.exons.size)
  }
}
