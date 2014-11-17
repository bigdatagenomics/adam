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
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAllele }
import org.kohsuke.args4j.Argument

object AlleleCount extends ADAMCommandCompanion {
  val commandName = "allelecount"
  val commandDescription = "Calculate Allele frequencies"

  def apply(cmdLine: Array[String]) = {
    new AlleleCount(Args4j[AlleleCountArgs](cmdLine))
  }
}

class AlleleCountArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "ADAM",
    usage = "The ADAM Variant file", index = 0)
  var adamFile: String = _
  @Argument(required = true, metaVar = "Output",
    usage = "Location to write allele frequency data", index = 1)
  var outputPath: String = null
}

object AlleleCountHelper extends Serializable {
  def chooseAllele(x: (String, java.lang.Long, String, String, GenotypeAllele)) =
    x match {
      case (chr, position, refAllele, varAllele, GenotypeAllele.Ref) => Some(chr, position, refAllele)
      case (chr, position, refAllele, varAllele, GenotypeAllele.Alt) => Some(chr, position, varAllele)
      case _ => None
    }

  def countAlleles(adamVariants: RDD[Genotype], args: AlleleCountArgs) {
    val usefulData = adamVariants.map(p => (p.getVariant.getContig.getContigName,
      p.getVariant.getStart,
      p.getVariant.getReferenceAllele,
      p.getVariant.getAlternateAllele,
      p.getAlleles.get(0),
      p.getAlleles.get(1)))
    val reduced_Variants = usefulData.flatMap(p => Seq((p._1, p._2, p._3, p._4, p._5), (p._1, p._2, p._3, p._4, p._6)))
    val alleles = reduced_Variants.flatMap(chooseAllele)
    alleles.groupBy(identity).map { case (a, b) => "%s\t%s\t%s\t%d".format(a._1, a._2, a._3, b.size) }
      .saveAsTextFile(args.outputPath)
  }
}

class AlleleCount(val args: AlleleCountArgs) extends ADAMSparkCommand[AlleleCountArgs] with Logging {
  val companion = AlleleCount

  def run(sc: SparkContext, job: Job) {

    val adamVariants: RDD[Genotype] = sc.adamLoad(args.adamFile)
    AlleleCountHelper.countAlleles(adamVariants, args)

  }
}
