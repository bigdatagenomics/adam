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
package org.bdgenomics.adam.rdd

import org.bdgenomics.formats.avro.{ ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.adam.rdd.GenotypesSummary.StatisticsMap
import org.bdgenomics.adam.rich.RichADAMVariant._
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable
import org.bdgenomics.adam.rdd.GenotypesSummaryCounts.ReferenceAndAlternate

/**
 * Simple counts of various properties across a set of genotypes.
 *
 * Note: for counts of variants, both homozygous and heterozygous
 * count as 1 (i.e. homozygous alternate is NOT counted as 2).
 * This seems to be the most common convention.
 *
 * @param genotypesCounts Counts of genotypes: map from list of ADAMGenotypeAllele (of size ploidy) -> count
 * @param singleNucleotideVariantCounts Map from ReferenceAndAlternate -> count
 *                                      where ReferenceAndAlternate is a single base variant.
 * @param multipleNucleotideVariantCount Count of multiple nucleotide variants (e.g.: AA -> TG)
 * @param insertionCount Count of insertions
 * @param deletionCount Count of deletions
 * @param readCount Sum of read depths for all genotypes with a called variant
 * @param phasedCount Number of genotypes with phasing information
 *
 */

case class GenotypesSummaryCounts(
    genotypesCounts: GenotypesSummaryCounts.GenotypeAlleleCounts,
    singleNucleotideVariantCounts: GenotypesSummaryCounts.VariantCounts,
    multipleNucleotideVariantCount: Long,
    insertionCount: Long,
    deletionCount: Long,
    readCount: Option[Long],
    phasedCount: Long) {

  lazy val genotypesCount: Long = genotypesCounts.values.sum
  lazy val variantGenotypesCount: Long =
    genotypesCounts.keys.filter(_.contains(ADAMGenotypeAllele.Alt)).map(genotypesCounts(_)).sum
  lazy val singleNucleotideVariantCount: Long = singleNucleotideVariantCounts.values.sum
  lazy val transitionCount: Long = GenotypesSummaryCounts.transitions.map(singleNucleotideVariantCounts).sum
  lazy val transversionCount: Long = GenotypesSummaryCounts.transversions.map(singleNucleotideVariantCounts).sum
  lazy val noCallCount: Long = genotypesCounts.count(_._1.contains(ADAMGenotypeAllele.NoCall))
  lazy val averageReadDepthAtVariants =
    if (variantGenotypesCount == 0) None
    else for (readCount1 <- readCount) yield readCount1.toDouble / variantGenotypesCount.toDouble
  lazy val withDefaultZeroCounts = GenotypesSummaryCounts(
    genotypesCounts.withDefaultValue(0.toLong),
    singleNucleotideVariantCounts.withDefaultValue(0.toLong),
    multipleNucleotideVariantCount,
    insertionCount,
    deletionCount,
    readCount,
    phasedCount)

  def combine(that: GenotypesSummaryCounts): GenotypesSummaryCounts = {
    def combine_counts[A](map1: Map[A, Long], map2: Map[A, Long]): Map[A, Long] = {
      val keys: Set[A] = map1.keySet.union(map2.keySet)
      val pairs = keys.map(k => (k -> (map1.getOrElse(k, 0.toLong) + map2.getOrElse(k, 0.toLong))))
      pairs.toMap
    }
    GenotypesSummaryCounts(
      combine_counts(genotypesCounts, that.genotypesCounts),
      combine_counts(singleNucleotideVariantCounts, that.singleNucleotideVariantCounts),
      multipleNucleotideVariantCount + that.multipleNucleotideVariantCount,
      insertionCount + that.insertionCount,
      deletionCount + that.deletionCount,
      for (readCount1 <- readCount; readcount2 <- that.readCount) yield readCount1 + readcount2,
      phasedCount + that.phasedCount)
  }
}
object GenotypesSummaryCounts {
  case class ReferenceAndAlternate(reference: String, alternate: String)

  type GenotypeAlleleCounts = Map[List[ADAMGenotypeAllele], Long]
  type VariantCounts = Map[ReferenceAndAlternate, Long]

  val simpleNucleotides = List("A", "C", "T", "G")

  val transitions = List(
    ReferenceAndAlternate("A", "G"),
    ReferenceAndAlternate("G", "A"),
    ReferenceAndAlternate("C", "T"),
    ReferenceAndAlternate("T", "C"))

  val transversions = List(
    ReferenceAndAlternate("A", "C"),
    ReferenceAndAlternate("C", "A"),
    ReferenceAndAlternate("A", "T"),
    ReferenceAndAlternate("T", "A"),
    ReferenceAndAlternate("G", "C"),
    ReferenceAndAlternate("C", "G"),
    ReferenceAndAlternate("G", "T"),
    ReferenceAndAlternate("T", "G"))

  /**
   * Factory for an empty GenotypesSummaryCounts.
   */
  def apply(): GenotypesSummaryCounts =
    GenotypesSummaryCounts(
      Map(),
      Map(),
      0, // Multiple nucleotide variants
      0, // Insertion count
      0, // Deletion count
      Some(0), // Read count
      0) // Phased count

  def apply(counts: GenotypesSummaryCounts) {
    assert(false)
  }

  /**
   * Factory for a GenotypesSummaryCounts that counts a single ADAMGenotype.
   */
  def apply(genotype: ADAMGenotype): GenotypesSummaryCounts = {
    val variant = genotype.getVariant
    val ref_and_alt = ReferenceAndAlternate(variant.getReferenceAllele.toString, variant.getVariantAllele.toString)

    // We always count our genotype. The other counts are set to 1 only if we have a variant genotype.
    val isVariant = genotype.getAlleles.contains(ADAMGenotypeAllele.Alt)
    val genotypeAlleleCounts = Map(genotype.getAlleles.asScala.toList -> 1.toLong)
    val variantCounts = (
      if (isVariant && variant.isSingleNucleotideVariant) Map(ref_and_alt -> 1.toLong)
      else Map(): VariantCounts)

    val readDepth = (
      if (genotype.getReadDepth == null) None
      else if (isVariant) Some(genotype.getReadDepth.toLong)
      else Some(0.toLong))

    GenotypesSummaryCounts(
      genotypeAlleleCounts,
      variantCounts,
      if (isVariant && variant.isMultipleNucleotideVariant) 1 else 0,
      if (isVariant && variant.isInsertion) 1 else 0,
      if (isVariant && variant.isDeletion) 1 else 0,
      readDepth,
      if (isVariant && genotype.getIsPhased != null && genotype.getIsPhased) 1 else 0)
  }
}

/**
 * Summary statistics for a set of genotypes.
 * @param perSampleStatistics A map from sample id -> GenotypesSummaryCounts for that sample
 * @param singletonCount Number of variants that are called in exactly one sample.
 * @param distinctVariantCount Number of distinct variants that are called at least once.
 *
 */
case class GenotypesSummary(
    perSampleStatistics: StatisticsMap,
    singletonCount: Long,
    distinctVariantCount: Long) {
  lazy val aggregateStatistics =
    perSampleStatistics.values.foldLeft(GenotypesSummaryCounts())(_.combine(_)).withDefaultZeroCounts
}
object GenotypesSummary {
  type StatisticsMap = Map[String, GenotypesSummaryCounts]

  /**
   * Factory for a GenotypesSummary given an RDD of ADAMGenotype.
   */
  def apply(rdd: RDD[ADAMGenotype]): GenotypesSummary = {
    def combineStatisticsMap(stats1: StatisticsMap, stats2: StatisticsMap): StatisticsMap = {
      stats1.keySet.union(stats2.keySet).map(sample => {
        (stats1.get(sample), stats2.get(sample)) match {
          case (Some(statsA), Some(statsB)) => sample -> statsA.combine(statsB)
          case (Some(stats), None)          => sample -> stats
          case (None, Some(stats))          => sample -> stats
          case (None, None)                 => throw new AssertionError("Unreachable")
        }
      }).toMap
    }
    val perSampleStatistics: StatisticsMap = rdd
      .map(genotype => Map(genotype.getSampleId.toString -> GenotypesSummaryCounts(genotype)))
      .fold(Map(): StatisticsMap)(combineStatisticsMap(_, _))
      .map({ case (sample: String, stats: GenotypesSummaryCounts) => sample -> stats.withDefaultZeroCounts }).toMap
    val variantCounts =
      rdd.filter(_.getAlleles.contains(ADAMGenotypeAllele.Alt)).map(genotype => {
        val variant = genotype.getVariant
        (variant.getContig, variant.getPosition, variant.getReferenceAllele, variant.getVariantAllele)
      }).countByValue
    val singletonCount = variantCounts.count(_._2 == 1)
    val distinctVariantsCount = variantCounts.size
    GenotypesSummary(perSampleStatistics, singletonCount, distinctVariantsCount)
  }
}
/**
 * Functions for converting a GenotypesSummary object to various text formats.
 */
object GenotypesSummaryFormatting {
  def format_csv(summary: GenotypesSummary): String = {

    val genotypeAlleles = sortedGenotypeAlleles(summary.aggregateStatistics)

    def format_statistics(stats: GenotypesSummaryCounts): Seq[String] = {
      val row = mutable.MutableList[String]()
      row += stats.genotypesCount.toString
      row += stats.variantGenotypesCount.toString
      row += stats.insertionCount.toString
      row += stats.deletionCount.toString
      row += stats.singleNucleotideVariantCount.toString
      row += stats.transitionCount.toString
      row += stats.transversionCount.toString
      row += (stats.transitionCount.toDouble / stats.transversionCount.toDouble).toString
      row ++= genotypeAlleles.map(stats.genotypesCounts(_).toString) // Genotype counts
      row ++= allSNVs.map(stats.singleNucleotideVariantCounts(_).toString) // SNV counts
      row
    }

    val basicHeader = List(
      "Sample", "Genotypes", "Variant Genotypes", "Insertions", "Deletions", "SNVs", "Transitions", "Transversions", "Ti / Tv")
    val genotypesHeader = genotypeAlleles.map(genotypeAllelesToString(_))
    val snvsHeader = allSNVs.map(snv => "%s>%s".format(snv.reference, snv.alternate))

    val result = new mutable.StringBuilder
    result ++= "# " + (basicHeader ++ genotypesHeader ++ snvsHeader).mkString(", ") + "\n"

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
    def format_statistics(stats: GenotypesSummaryCounts, result: mutable.StringBuilder) = {
      result ++= "\tVariant Genotypes: %d / %d = %1.3f%%\n".format(
        stats.variantGenotypesCount,
        stats.genotypesCount,
        stats.variantGenotypesCount.toDouble * 100.0 / stats.genotypesCount)

      for (genotype <- sortedGenotypeAlleles(summary.aggregateStatistics)) {
        val count = stats.genotypesCounts(genotype)
        result ++= "\t%20s: %9d = %1.3f%%\n".format(
          genotypeAllelesToString(genotype),
          count,
          count.toDouble * 100.0 / stats.genotypesCount.toDouble)
      }
      result ++= "\tInsertions: %d\n".format(stats.insertionCount)
      result ++= "\tDeletions: %d\n".format(stats.deletionCount)
      result ++= "\tMultiple nucleotide variants: %d\n".format(stats.multipleNucleotideVariantCount)
      result ++= "\tSingle nucleotide variants: %d\n".format(stats.singleNucleotideVariantCount)
      result ++= "\t\tTransitions / transversions: %4d / %4d = %1.3f\n".format(
        stats.transitionCount,
        stats.transversionCount,
        stats.transitionCount.toDouble / stats.transversionCount.toDouble)
      var from, to = 0
      for (snv <- allSNVs) {
        result ++= "\t\t%s>%s %9d\n".format(snv.reference, snv.alternate, stats.singleNucleotideVariantCounts(snv))
      }
      result ++= "\tAverage read depth at called variants: %s\n".format(stats.averageReadDepthAtVariants match {
        case Some(depth) => "%1.1f".format(depth)
        case None        => "[no variant calls, or read depth missing for one or more variant calls]"
      })
      result ++= "\tPhased genotypes: %d / %d = %1.3f%%\n".format(
        stats.phasedCount,
        stats.genotypesCount,
        stats.phasedCount.toDouble * 100 / stats.genotypesCount)
    }

    val result = new mutable.StringBuilder
    for (sample <- summary.perSampleStatistics.keySet.toList.sorted) {
      result ++= "Sample: %s\n".format(sample)
      format_statistics(summary.perSampleStatistics(sample), result)
      result ++= "\n"
    }
    result ++= "\nSummary\n"
    result ++= "\tSamples: %d\n".format(summary.perSampleStatistics.size)
    result ++= "\tDistinct variants: %d\n".format(summary.distinctVariantCount)
    result ++= "\tVariants found only in a single sample: %d = %1.3f%%\n".format(
      summary.singletonCount,
      summary.singletonCount.toDouble * 100.0 / summary.distinctVariantCount)
    format_statistics(summary.aggregateStatistics, result)
    result.toString
  }

  private def sortedGenotypeAlleles(stats: GenotypesSummaryCounts): Seq[List[ADAMGenotypeAllele]] = {
    def genotypeSortOrder(genotype: List[ADAMGenotypeAllele]): Int = genotype.map({
      case ADAMGenotypeAllele.Ref                               => 0
      case ADAMGenotypeAllele.Alt | ADAMGenotypeAllele.OtherAlt => 1 // alt/otheralt sort to same point
      case ADAMGenotypeAllele.NoCall                            => 10 // arbitrary large number so any genotype with a NoCall sorts last.
    }).sum
    stats.genotypesCounts.keySet.toList.sortBy(genotypeSortOrder(_))
  }

  private def genotypeAllelesToString(alleles: List[ADAMGenotypeAllele]): String =
    alleles.map(_.toString).mkString("-")

  lazy val allSNVs: Seq[ReferenceAndAlternate] =
    for (
      from <- GenotypesSummaryCounts.simpleNucleotides;
      to <- GenotypesSummaryCounts.simpleNucleotides;
      if (from != to)
    ) yield GenotypesSummaryCounts.ReferenceAndAlternate(from, to)

}
