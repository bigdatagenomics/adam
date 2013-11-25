/*
 * Copyright (c) 2013. The Broad Institute of MIT/Harvard
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

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import edu.berkeley.cs.amplab.adam.avro._
import edu.berkeley.cs.amplab.adam.util._
import org.apache.hadoop.mapreduce.Job
import org.kohsuke.args4j.Argument
import java.lang.{Integer => JInt}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._

object Vcf2Adam extends AdamCommandCompanion {

  val commandName = "vcf2adam"
  val commandDescription = "Convert a VCF file to the corresponding ADAM format"

  def apply(cmdLine: Array[String]) = {
    new Vcf2Adam(Args4j[Vcf2AdamArgs](cmdLine))
  }

}

class Vcf2AdamArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "VCF", usage = "The VCF file to convert", index = 0)
  var vcfFile: String = _
  @Argument(required = true, metaVar = "ADAMVariants", usage = "Location to write ADAM Variant data", index = 1)
  var outputPathV: String = null
  @Argument(required = true, metaVar = "ADAMGenotypes", usage = "Location to write ADAM genotype data", index = 2)
  var outputPathG: String = null
}

class Vcf2Adam(val args: Vcf2AdamArgs) extends AdamSparkCommand[Vcf2AdamArgs] with Logging {
  val companion = Vcf2Adam

  def run(sc: SparkContext, job: Job) {

    val source: RDD[String] = sc.textFile(args.vcfFile)
    val header = source.filter(_.startsWith("#")).collect()
    val converter = new VcfConverter(header)

    val converted = source.filter(u => !u.startsWith("#")).map(converter.convert)
    val variants = converted.map(_._1)
    val genotypes = converted.flatMap(_._2)

    variants.adamSave(args.outputPathV, args)
    genotypes.adamSave(args.outputPathG, args)
  }
}

class VcfConverter(val header: Array[String]) extends Serializable {
  def this() = this(Array[String]())

  // the only important thing for decoding is the map from offset to samples; thus

  val samples = header.filter(_.startsWith("#CHR")).head.split("\t").drop(9)

  def convert(record: String): (ADAMVariant, List[ADAMGenotype]) = {
    val fields = record.split("\t")

    val variant = ADAMVariant.newBuilder().setReferenceName(fields(0).asInstanceOf[CharSequence])
      .setStartPosition(fields(1).toLong)
      .setVariantId(fields(2))
      .setReferenceAllele(fields(3))
      .setAlternateAlleles(fields(4))
      .setSiteFilter(fields(6))
      .setType(getType(fields(3), fields(4)))

    // TODO: add back .setInfo(fields(7))

    if (fields.size == 7) {
      return (variant.build(), List[ADAMGenotype]())
    }

    val ft = fields(8)
    val pl_idx = ft.split(":").indexOf("PL")
    val gl_idx = ft.split(":").indexOf("GL")
    val hasPL = pl_idx >= 0
    val hasGL = gl_idx >= 0
    val records = fields.slice(9, fields.size).map(_.split(":")).zip(samples).map(formatSample => {
      val gtFormat = formatSample._1
      val sampleId = formatSample._2
      val adamGenotype: ADAMGenotype.Builder = ADAMGenotype.newBuilder()
      val gtField = gtFormat(0)

      val gt = gtField.split("/|\\|").map(_.toInt.asInstanceOf[JInt]).toList
      adamGenotype.setGenotype(gt.map(_.toString).reduce(_ + "," + _))

      adamGenotype.setSampleId(sampleId)
      if (hasPL && gtFormat.size > pl_idx) {
        // TODO: Add back "&& gtFormat(pl_idx).find(".") < 0"
        val pl = gtFormat(pl_idx).split(",").map(_.toInt.asInstanceOf[JInt]).toList
        adamGenotype.setPhredLikelihoods(pl.map(_.toString).reduce(_ + "," + _))
      } else if (hasGL && gtFormat.size > gl_idx && gtFormat(gl_idx) != ".") {
        val pl = gtFormat(gl_idx).split(",").map(x => (-10 * x.toDouble).toInt.asInstanceOf[JInt]).toList
        adamGenotype.setPhredLikelihoods(pl.map(_.toString).reduce(_ + "," + _))
      }
      if (gtFormat.size > 2) {
        val keys = ft.split(":").slice(2, gtFormat.size)
        val vals = gtFormat.slice(2, gtFormat.size)
        adamGenotype.setFormat(keys.zip(vals).map(c => c._1 + "=" + c._2).reduce(_ + ";" + _))
      }
      adamGenotype.setPosition(fields(1).toLong)
      val geno = adamGenotype.build()
      geno
    }).toList

    val counts = records.flatMap(g => List(g.getAllele1, g.getAllele2)).distinct.length
    variant.setAlleleCount(counts)

    (variant.build(), records)
  }

  def getType(ref: String, alt: String): VariantType = {
    val altAlleles = alt.split(",")
    if (altAlleles.forall(_.startsWith("<"))) {
      VariantType.SV
    }
    val altSize = altAlleles.map(_.size)
    val matches = altSize.forall(_ == ref.size)
    if (matches) {
      if (ref.size == 1) VariantType.SNP else VariantType.MNP
    } else {
      val isInsert = altAlleles.forall(_.startsWith(ref))
      if (isInsert) {
        VariantType.Insertion
      } else if (altAlleles.forall(ref.startsWith)) {
        VariantType.Deletion
      } else {
        VariantType.Complex
      }
    }
  }
}
