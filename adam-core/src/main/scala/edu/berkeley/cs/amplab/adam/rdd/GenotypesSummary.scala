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

import edu.berkeley.cs.amplab.adam.avro.{ADAMGenotypeAllele, ADAMGenotype}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import edu.berkeley.cs.amplab.adam.rich.RichADAMVariant._
import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.adam.rdd.GenotypesSummary.StatisticsMap
import scala.collection.immutable.Map

case class GenotypesStatistics(
  genotypesCounts: GenotypesStatistics.GenotypeAlleleCounts,
  singleNucleotideVariantCounts: GenotypesStatistics.VariantCounts,
  multipleNucleotideVariantCount: Long,
  insertionCount: Long,
  deletionCount: Long,
  readCount: Long,
  phasedCount: Long)
{
  lazy val genotypesCount: Long = genotypesCounts.values.sum
  lazy val singleNucleotideVariantCount: Long = singleNucleotideVariantCounts.values.sum
  lazy val transitionCount: Long = GenotypesStatistics.Transitions.map(singleNucleotideVariantCounts).sum
  lazy val transversionCount: Long = GenotypesStatistics.Transversions.map(singleNucleotideVariantCounts).sum
  lazy val noCallCount: Long = genotypesCounts.count(_._1.contains(ADAMGenotypeAllele.NoCall))

  def combine(that: GenotypesStatistics): GenotypesStatistics = {
    def combine_counts[A](map1: Map[A, Long], map2: Map[A, Long]): Map[A, Long] = {
      val keys: Set[A] = map1.keySet.union(map2.keySet)
      val pairs = keys.map(k => (k -> (map1.getOrElse(k, 0.toLong) + map2.getOrElse(k, 0.toLong))))
      pairs.toMap.withDefaultValue(0)
    }
    GenotypesStatistics(
      combine_counts(genotypesCounts, that.genotypesCounts),
      combine_counts(singleNucleotideVariantCounts, that.singleNucleotideVariantCounts),
      multipleNucleotideVariantCount + that.multipleNucleotideVariantCount,
      insertionCount + that.insertionCount,
      deletionCount + that.deletionCount,
      readCount + that.readCount,
      phasedCount + that.phasedCount
    )
  }
}
object GenotypesStatistics {
  case class ReferenceAndAlternate(reference: String, alternate: String)

  type GenotypeAlleleCounts =  Map[List[ADAMGenotypeAllele], Long]
  object GenotypeAlleleCounts {
    def apply(): GenotypeAlleleCounts = Map[List[ADAMGenotypeAllele], Long]().withDefaultValue(0)
  }
  type VariantCounts = Map[ReferenceAndAlternate, Long]
  object VariantCounts {
    def apply(): VariantCounts = Map[ReferenceAndAlternate, Long]().withDefaultValue(0)
  }

  val SimpleNucleotides = List("A", "C", "T", "G")

  val Transitions = List(
    ReferenceAndAlternate("A", "G"),
    ReferenceAndAlternate("G", "A"),
    ReferenceAndAlternate("C", "T"),
    ReferenceAndAlternate("T", "C"))

  val Transversions = List(
    ReferenceAndAlternate("A", "C"),
    ReferenceAndAlternate("C", "A"),
    ReferenceAndAlternate("A", "T"),
    ReferenceAndAlternate("T", "A"),
    ReferenceAndAlternate("G", "C"),
    ReferenceAndAlternate("C", "G"),
    ReferenceAndAlternate("G", "T"),
    ReferenceAndAlternate("T", "G"))

  def apply(): GenotypesStatistics =
    GenotypesStatistics(Map[List[ADAMGenotypeAllele], Long](),Map[ReferenceAndAlternate, Long]() , 0, 0, 0, 0, 0)

  def apply(genotype: ADAMGenotype): GenotypesStatistics = {
    val variant = genotype.getVariant
    val ref_and_alt = ReferenceAndAlternate(variant.getReferenceAllele.toString, variant.getVariantAllele.toString)
    GenotypesStatistics(
      (GenotypeAlleleCounts() + (genotype.getAlleles.asScala.toList -> 1.toLong)).withDefaultValue(0),
      if (variant.isSingleNucleotideVariant && genotype.getAlleles.contains(ADAMGenotypeAllele.Alt))
        (VariantCounts() + (ref_and_alt -> 1.toLong)).withDefaultValue(0.toLong)
      else
        VariantCounts(),
      if (variant.isMultipleNucleotideVariant) 1 else 0,
      if (variant.isInsertion) 1 else 0,
      if (variant.isDeletion) 1 else 0,
      if (genotype.getReadDepth == null) 0 else genotype.getReadDepth.toLong,
      if (genotype.getIsPhased == null || !genotype.getIsPhased) 0 else 1)
  }
}

case class GenotypesSummary(
  perSampleStatistics: StatisticsMap,
  singletonCount: Long,
  uniqueVariantCount: Long)
{
  lazy val aggregateStatistics = perSampleStatistics.values.foldLeft(GenotypesStatistics())(_.combine(_))
}

object GenotypesSummary {
  type StatisticsMap = Map[String, GenotypesStatistics]
  object StatisticsMap {
    def apply(): StatisticsMap = Map[String, GenotypesStatistics]()
  }

  def apply(rdd: RDD[ADAMGenotype]) : GenotypesSummary = {
    def combineStatisticsMap(stats1: StatisticsMap, stats2: StatisticsMap): StatisticsMap = {
      stats1.keySet.union(stats2.keySet).map(sample => {
        (stats1.get(sample), stats2.get(sample)) match {
          case (Some(stats1), Some(stats2)) => (sample, stats1.combine(stats2))
          case (Some(stats1), None) => (sample, stats1)
          case (None, Some(stats2)) => (sample, stats2)
          case (None, None) => throw new AssertionError("Unreachable")
        }
      }).toMap
    }
    val perSampleStatistics: StatisticsMap = rdd
      .map(genotype => StatisticsMap() + (genotype.getSampleId.toString -> GenotypesStatistics(genotype)))
      .fold(StatisticsMap())(combineStatisticsMap(_, _))
    val variantCounts =
      rdd.filter(_.getAlleles.contains(ADAMGenotypeAllele.Alt)).map(genotype => {
        val variant = genotype.getVariant
        (variant.getContig, variant.getPosition, variant.getReferenceAllele, variant.getVariantAllele)
      }).countByValue
    val singletonCount = variantCounts.count(_._2 == 1)
    val uniqueVariantsCount = variantCounts.size
    GenotypesSummary(perSampleStatistics, singletonCount, uniqueVariantsCount)
  }
}

object VariantSummaryOutput {
  def format_csv(summary: GenotypesSummary): String = {
    def format_statistics(stats: GenotypesStatistics): Seq[String] = {
      val row = mutable.MutableList[String]()
      row += stats.singleNucleotideVariantCount.toString
      row += stats.transitionCount.toString
      row += stats.transversionCount.toString
      row += (stats.transitionCount.toDouble / stats.transversionCount.toDouble).toString
      for (from <- GenotypesStatistics.SimpleNucleotides; to <- GenotypesStatistics.SimpleNucleotides; if (from != to)) {
        row += stats.singleNucleotideVariantCounts((GenotypesStatistics.ReferenceAndAlternate(from, to))).toString
      }
      row
    }

    val header = List("# Sample", "Num SNV", "Transitions", "Transversions", "Ti / Tv") ++
      (for (from <- GenotypesStatistics.SimpleNucleotides;
           to <- GenotypesStatistics.SimpleNucleotides
           if (from != to)) yield "%s>%s".format(from, to))

    val result = new mutable.StringBuilder
    result ++= header.mkString(", ") + "\n"

    for ((sample, stats) <- summary.perSampleStatistics) {
      val row = mutable.MutableList(sample)
      row ++= format_statistics(stats)
      result ++= row.mkString(", ") + "\n"
    }
    val final_row = List("Aggregated") ++ format_statistics(summary.aggregateStatistics)
    result ++= final_row.mkString(", ") + "\n"
    result.toString
  }

  def format_human_readable(summary: GenotypesSummary): String = {
    def format_statistics(stats: GenotypesStatistics, result: mutable.StringBuilder) = {
      result ++= "\tGenotypes: %d\n".format(stats.genotypesCount)
      def alleleSorter(allele: ADAMGenotypeAllele): Int = allele match {
        case ADAMGenotypeAllele.Ref => 0
        case ADAMGenotypeAllele.Alt => 1
        case ADAMGenotypeAllele.NoCall => 10
      }
      for ((genotype, count) <- stats.genotypesCounts.toList.sortBy(_._1.map(alleleSorter(_)).sum)) {
        result ++= "\t%20s: %9d = %1.3f%%\n".format(
          genotype.map(_.toString).mkString("-"),
          count,
          count.toDouble * 100.0 / stats.genotypesCount.toDouble
        )
      }
      result ++= "\tInsertions: %d\n".format(stats.insertionCount)
      result ++= "\tDeletions: %d\n".format(stats.deletionCount)
      result ++= "\tMultiple nucleotide variants: %d\n".format(stats.multipleNucleotideVariantCount)
      result ++= "\tSingle nucleotide variants: %d\n".format(stats.singleNucleotideVariantCount)
      result ++= "\t\tTransitions / transversions = %4d / %4d = %1.3f\n".format(
        stats.transitionCount,
        stats.transversionCount,
        stats.transitionCount.toDouble / stats.transversionCount.toDouble)
      var from, to = 0
      for (from <- GenotypesStatistics.SimpleNucleotides; to <- GenotypesStatistics.SimpleNucleotides; if (from != to)) {
        result ++= "\t\t%s>%s %9d\n".format(
          from,
          to,
          stats.singleNucleotideVariantCounts((GenotypesStatistics.ReferenceAndAlternate(from, to))))
      }
      result ++= "\tAverage read depth: %1.1f\n".format(stats.readCount.toDouble / stats.genotypesCount)
      result ++= "\tPhased genotypes: %d / %d = %1.3f%%\n".format(
        stats.phasedCount,
        stats.genotypesCount,
        stats.phasedCount.toDouble * 100 / stats.genotypesCount
      )
    }

    val result = new mutable.StringBuilder
    for ((sample, stats) <- summary.perSampleStatistics) {
      result ++= "\nSample: %s\n".format(sample)
      format_statistics(stats, result)
    }
    result ++= "\nSummary\n"
    result ++= "\tSamples: %d\n".format(summary.perSampleStatistics.size)
    result ++= "\tUnique variants: %d\n".format(summary.uniqueVariantCount)
    result ++= "\tVariants found only in a single sample: %d = %1.3f%%\n".format(
      summary.singletonCount,
      summary.singletonCount.toDouble * 100.0 / summary.uniqueVariantCount)
    format_statistics(summary.aggregateStatistics, result)
    result.toString
  }
}