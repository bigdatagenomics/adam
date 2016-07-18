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

import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.kohsuke.args4j.spi.BooleanOptionHandler
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegion }
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.projections.AlignmentRecordField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Variant }
import org.bdgenomics.utils.cli._
import scala.io._

/**
 * CalculateDepth (accessible as the command 'depth' through the CLI) takes two arguments,
 * an Read file and a VCF (or equivalent) file, and calculates the number of reads
 * (the 'depth') from the Read file which overlap each of the variants given by the VCF.
 * It then reports, on standard out, the location and name of each variant along with the
 * calculated depth.
 */
object CalculateDepth extends BDGCommandCompanion {
  val commandName: String = "depth"
  val commandDescription: String = "Calculate the depth from a given ADAM file, " +
    "at each variant in a VCF"

  def apply(cmdLine: Array[String]): BDGCommand = {
    new CalculateDepth(Args4j[CalculateDepthArgs](cmdLine))
  }
}

class CalculateDepthArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "ADAM", usage = "The Read file to use to calculate depths", index = 0)
  val adamInputPath: String = null

  @Argument(required = true, metaVar = "VCF", usage = "The VCF containing the sites at which to calculate depths", index = 1)
  val vcfInputPath: String = null
}

class CalculateDepth(protected val args: CalculateDepthArgs) extends BDGSparkCommand[CalculateDepthArgs] {
  val companion: BDGCommandCompanion = CalculateDepth

  def run(sc: SparkContext): Unit = {

    val proj = Projection(contigName, start, cigar, readMapped)

    // load reads and variants
    val readRdd = sc.loadAlignments(args.adamInputPath, projection = Some(proj))
    val variants = sc.loadVariants(args.vcfInputPath)

    // perform join
    val joinedRdd = readRdd.broadcastRegionJoin(variants)

    // map variant to region and swap tuple field order
    val finalRdd = joinedRdd.rdd.map(kv => (ReferenceRegion(kv._2), kv._1))

    // count at sites
    val depths: RDD[(ReferenceRegion, Int)] =
      finalRdd.map { case (region, record) => (region, 1) }.reduceByKey(_ + _).sortByKey()

    /*
     * tab-delimited output, containing the following columns:
     * 0: the location of the variant
     * 1: the depth of overlapping reads at the variant
     */
    println("location\tname\tdepth")
    depths.collect().foreach {
      case (region, count) =>
        println("%20s\t% 5d".format(
          "%s:%d".format(region.referenceName, region.start),
          count
        ))
    }
  }
}
