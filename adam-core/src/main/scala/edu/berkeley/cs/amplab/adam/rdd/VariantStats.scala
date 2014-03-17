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

class PerSampleVariantStatistics {
  val snv_counts = mutable.Map[SNV, Long]().withDefaultValue(0)

  def snv_count: Long = snv_counts.values.sum
  def transitions: Long = ComputeVariantStatistics.TransitionSNVs.map(snv_counts).sum
  def transversions: Long = ComputeVariantStatistics.TransversionsSNVs.map(snv_counts).sum

  def format_human(result : mutable.StringBuilder = new mutable.StringBuilder()): mutable.StringBuilder = {
    var from, to = 0
    result ++= "\tSNV Summary (%d total)\n".format(snv_count)
    result ++= "\t\tTransitions / transversions = %4d / %4d = %1.3f\n".format(
      transitions,
      transversions,
      transitions.toDouble / transversions.toDouble)
    for (from <- ComputeVariantStatistics.SimpleNucleotides; to <- ComputeVariantStatistics.SimpleNucleotides; if (from != to)) {
      result ++= "\t\t%s -> %s: %9d\n".format(from, to, snv_counts((SNV(from, to))))
    }
    result
  }
}

class VariantStatistics {
  val sample_statistics = mutable.Map[String, PerSampleVariantStatistics]()
  val aggregate_statistics = new PerSampleVariantStatistics()

  def format_human(result : mutable.StringBuilder = new mutable.StringBuilder()): mutable.StringBuilder = {
    result ++= toString
    for ((sample, data) <- sample_statistics) {
      result ++= "\nSample: %s\n".format(sample)
      data.format_human(result)
    }
    result ++= "\nAggregated over samples\n"
    aggregate_statistics.format_human(result)
    result
  }
}

object ComputeVariantStatistics {
  val SimpleNucleotides = List("A", "C", "T", "G")
  val TransitionSNVs = List(
    SNV("A", "G"),
    SNV("G", "A"),
    SNV("C", "T"),
    SNV("T", "C"))

  val TransversionsSNVs = List(
    SNV("A", "C"),
    SNV("C", "A"),
    SNV("A", "T"),
    SNV("T", "A"),
    SNV("G", "C"),
    SNV("C", "G"),
    SNV("G", "T"),
    SNV("T", "G"))

  def apply(rdd: RDD[ADAMGenotype]) : VariantStatistics = {
    val snps = rdd.filter(_.getVariant.getVariantType == VariantType.SNP)

    val counts = snps.map(genotype => {
      val variant = genotype.getVariant
      val snv = SNV(variant.getReferenceAllele.toString, variant.getVariantAllele.toString)
      val sample = genotype.getSampleId
      (sample, snv)
    }).countByValue()

    val stats = new VariantStatistics
    for (((sample: String, snv: SNV), count: Long) <- counts) {
      if (!stats.sample_statistics.contains(sample)) {
        stats.sample_statistics(sample) = new PerSampleVariantStatistics
      }
      stats.sample_statistics(sample).snv_counts(snv) += count
      stats.aggregate_statistics.snv_counts(snv) += count
    }
    stats
  }
}