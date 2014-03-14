/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use file except in compliance with the License.
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
package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.ADAMGenotype
import org.apache.spark.rdd.RDD
import collection.mutable.HashMap
import collection.mutable
import edu.berkeley.cs.amplab.adam.avro.VariantType

case class SNV(reference: String, alternate: String)

case class PerSampleVariantStatistics(
  var snv_count: Long,
  snv_counts: mutable.Map[SNV, Long]
)

case class XZ()

case class VariantStatistics(
  sample_statistics: mutable.Map[String, PerSampleVariantStatistics],
  aggregate_statistics: PerSampleVariantStatistics
) {
  /*
  def toString() = {
    "A string"
  }
  */
}

object ComputeVariantStatistics {
  def apply(rdd: RDD[ADAMGenotype]) : VariantStatistics = {
    println(rdd.count())
    val c = rdd.map(_.getVariant.getVariantType).collect()
    println(c)

    val snps = rdd.filter(_.getVariant.getVariantType == VariantType.SNP)

    println(snps.count())

    val counts = snps.map(genotype => {
      val variant = genotype.getVariant
      val snv = SNV(variant.getReferenceAllele.toString, variant.getVariantAllele.toString)
      val sample = genotype.getSampleId
      (sample, snv)
    }).countByValue()

    val stats = VariantStatistics(
      new HashMap[String, PerSampleVariantStatistics].withDefault(
        sample => PerSampleVariantStatistics(0, new HashMap[SNV, Long].withDefaultValue(0))),
      PerSampleVariantStatistics(0, new HashMap[SNV, Long].withDefaultValue(0)))

    println(counts.toString)

    counts.foreach(item => {
      val sample: String = item._1._1.toString
      val snv: SNV = item._1._2
      val count: Long = item._2
      val sample_stats = stats.sample_statistics(sample)
      sample_stats.snv_count += count
      sample_stats.snv_counts(snv) += count
      stats.aggregate_statistics.snv_count += count
      stats.aggregate_statistics.snv_counts(snv) += count
    })

    return stats
  }
}