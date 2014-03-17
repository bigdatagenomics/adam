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

import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotypeAllele, ADAMGenotype, VariantType}
import org.apache.spark.rdd.RDD
import collection.mutable.HashMap
import collection.mutable

class VariantsSummaryStatistics {
  val snv_counts = mutable.Map[VariantsSummary.SNV, Long]().withDefaultValue(0)

  def snv_count: Long = snv_counts.values.sum
  def transitions: Long = VariantsSummary.TransitionSNVs.map(snv_counts).sum
  def transversions: Long = VariantsSummary.TransversionsSNVs.map(snv_counts).sum
}

class VariantsSummary {
  val sample_statistics = mutable.Map[String, VariantsSummaryStatistics]()
  val aggregate_statistics = new VariantsSummaryStatistics
}

object VariantsSummary {
  case class SNV(reference: String, alternate: String)

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

  def apply(rdd: RDD[ADAMGenotype]) : VariantsSummary = {
    val snvs = rdd.filter(genotype => genotype.getVariant.getVariantType == VariantType.SNP
                                      && genotype.getAlleles.contains(ADAMGenotypeAllele.Alt))

    val sample_snvs = snvs.map(genotype => {
      val variant = genotype.getVariant
      val snv = SNV(variant.getReferenceAllele.toString, variant.getVariantAllele.toString)
      val sample = genotype.getSampleId
      (sample, snv)
    })
    val counts = sample_snvs.countByValue()

    val stats = new VariantsSummary
    for (((sample: String, snv: VariantsSummary.SNV), count: Long) <- counts) {
      if (!stats.sample_statistics.contains(sample)) {
        stats.sample_statistics(sample) = new VariantsSummaryStatistics
      }
      stats.sample_statistics(sample).snv_counts(snv) += count
      stats.aggregate_statistics.snv_counts(snv) += count
    }
    stats
  }
}

object VariantSummaryOutput {
  def format_csv(summary: VariantsSummary): String = {
    def format_statistics(stats: VariantsSummaryStatistics): Seq[String] = {
      val row = mutable.MutableList[String]()
      row += stats.snv_count.toString
      row += stats.transitions.toString
      row += stats.transversions.toString
      row += (stats.transitions.toDouble / stats.transversions.toDouble).toString
      for (from <- VariantsSummary.SimpleNucleotides; to <- VariantsSummary.SimpleNucleotides; if (from != to)) {
        row += stats.snv_counts((VariantsSummary.SNV(from, to))).toString
      }
      row
    }

    val header = List("# Sample", "Num SNV", "Transitions", "Transversions", "Ti / Tv") ++
      (for (from <- VariantsSummary.SimpleNucleotides;
           to <- VariantsSummary.SimpleNucleotides
           if (from != to)) yield "%s>%s".format(from, to))

    val result = new mutable.StringBuilder
    result ++= header.mkString(", ") + "\n"

    for ((sample, stats) <- summary.sample_statistics) {
      val row = mutable.MutableList(sample)
      row ++= format_statistics(stats)
      result ++= row.mkString(", ") + "\n"
    }
    val final_row = List("Aggregated") ++ format_statistics(summary.aggregate_statistics)
    result ++= final_row.mkString(", ") + "\n"
    result.toString
  }

  def format_human_readable(summary: VariantsSummary): String = {
    def format_statistics(stats: VariantsSummaryStatistics, result: mutable.StringBuilder) = {
      result ++= "\tSNVs (%d total)\n".format(stats.snv_count)
      result ++= "\t\tTransitions / transversions = %4d / %4d = %1.3f\n".format(
        stats.transitions,
        stats.transversions,
        stats.transitions.toDouble / stats.transversions.toDouble)
      var from, to = 0
      for (from <- VariantsSummary.SimpleNucleotides; to <- VariantsSummary.SimpleNucleotides; if (from != to)) {
        result ++= "\t\t%s>%s %9d\n".format(from, to, stats.snv_counts((VariantsSummary.SNV(from, to))))
      }
    }

    val result = new mutable.StringBuilder
    for ((sample, stats) <- summary.sample_statistics) {
      result ++= "\nSample: %s\n".format(sample)
      format_statistics(stats, result)
    }
    result ++= "\nAggregated over samples\n"
    format_statistics(summary.aggregate_statistics, result)
    result.toString
  }
}