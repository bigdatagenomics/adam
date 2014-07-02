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

import scala.io._

import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.kohsuke.args4j.spi.BooleanOptionHandler
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegion }
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.projections.ADAMRecordField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.RegionJoin
import org.bdgenomics.adam.rich.ReferenceMappingContext._

/**
 * CalculateDepth (accessible as the command 'depth' through the CLI) takes two arguments,
 * an ADAMRecord file and a VCF (or equivalent) file, and calculates the number of reads
 * (the 'depth') from the ADAMRecord file which overlap each of the variants given by the VCF.
 * It then reports, on standard out, the location and name of each variant along with the
 * calculated depth.
 */
object CalculateDepth extends ADAMCommandCompanion {
  val commandName: String = "depth"
  val commandDescription: String = "Calculate the depth from a given ADAM file, " +
    "at each variant in a VCF"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new CalculateDepth(Args4j[CalculateDepthArgs](cmdLine))
  }
}

class CalculateDepthArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "ADAM", usage = "The ADAMRecord file to use to calculate depths", index = 0)
  val adamInputPath: String = null

  @Argument(required = true, metaVar = "VCF", usage = "The VCF containing the sites at which to calculate depths", index = 1)
  val vcfInputPath: String = null

  @Args4jOption(name = "-cartesian", handler = classOf[BooleanOptionHandler], usage = "Use a cartesian join, then filter")
  val cartesian: Boolean = false
}

class CalculateDepth(protected val args: CalculateDepthArgs) extends ADAMSparkCommand[CalculateDepthArgs] {
  val companion: ADAMCommandCompanion = CalculateDepth

  def run(sc: SparkContext, job: Job): Unit = {

    val proj = Projection(contig, start, cigar, readMapped)

    val adamRDD: RDD[ADAMRecord] = sc.adamLoad(args.adamInputPath, projection = Some(proj))
    val mappedRDD = adamRDD.filter(_.getReadMapped)

    val seqDict = sc.adamDictionaryLoad[ADAMRecord](args.adamInputPath)

    /*
     * The following is the code I _want_ to be able to run.  However, in order to do this,
     * we need to modify the adamVariantContextLoad method to not fail when the VCF is missing
     * the 'contig' header fields.  This will come with Neal's PR that will move us from using
     * refId to referenceName as the primary key for aligned positions.  Until that time,
     * this code remains commented out and I'll be using the loadPositions convenience method, below.
     * --TWD
     *
    val vcf : RDD[ADAMVariantContext] = sc.adamVariantContextLoad(args.vcfInputPath)
    val variantPositions = vcf.map(_.position).map(ReferenceRegion(_))

    val variantNames = vcf
      .map(ctx => (ReferenceRegion(ctx.position), ctx.variants.map(_.getId).mkString(",")))
      .collect().toMap
    */
    val vcf: RDD[(ReferenceRegion, String)] = loadPositions(sc, seqDict, args.vcfInputPath)
    val variantPositions = vcf.map(_._1)
    val variantNames = vcf.collect().toMap

    val joinedRDD: RDD[(ReferenceRegion, ADAMRecord)] =
      if (args.cartesian) RegionJoin.cartesianFilter(variantPositions, mappedRDD)
      else RegionJoin.partitionAndJoin(sc, seqDict, variantPositions, mappedRDD)

    val depths: RDD[(ReferenceRegion, Int)] =
      joinedRDD.map { case (region, record) => (region, 1) }.reduceByKey(_ + _).sortByKey()

    /*
     * tab-delimited output, containing the following columns:
     * 0: the location of the variant
     * 1: the display name of the variant
     * 2: the depth of overlapping reads at the variant
     */
    println("location\tname\tdepth")
    depths.collect().foreach {
      case (region, count) =>
        println("%20s\t%15s\t% 5d".format(
          "%s:%d".format(region.referenceName, region.start),
          variantNames(region),
          count))
    }
  }

  /**
   * See note above -- this is a convenience method, for loading positions from tab-delimited files
   * which have a format (for their first four columns) identical to that of the VCF.
   *
   * In the long run however, this should be replaced, as above, with a call to adamLoadVariantContext.
   *
   * @param sc A SparkContext
   * @param seqDict The sequence dictionary containing the refIds for all the contig/chromosomes listed
   *                in the file named in 'path'.
   * @param path The filesystem location fo the VCF-like file to load
   * @return An RDD of ReferenceRegion,String pairs where each ReferenceRegion is the location of a
   *         variant and each paired String is the display name of that variant.
   *
   * @throws IllegalArgumentException if the file contains a chromosome name that is not in the
   *                                  SequenceDictionary
   */
  private def loadPositions(sc: SparkContext, seqDict: SequenceDictionary, path: String): RDD[(ReferenceRegion, String)] = {
    sc.parallelize(Source.fromFile(path).getLines().filter(!_.startsWith("#")).map {
      line =>
        {
          val array = line.split("\t")
          val chrom = array(0)
          if (!seqDict.containsRefName(chrom)) {
            throw new IllegalArgumentException("chromosome name \"%s\" wasn't in the sequence dictionary (%s)".format(
              chrom, seqDict.records.map(_.name).mkString(",")))
          }
          val start = array(1).toLong
          val name = array(2)
          val end = start + array(3).length
          (ReferenceRegion(chrom, start, end), name)
        }
    }.toSeq)
  }
}
