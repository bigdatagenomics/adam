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
package org.bdgenomics.adam.util

import java.io.{ StringWriter, PrintWriter }
import htsjdk.variant.vcf.{
  VCFFormatHeaderLine,
  VCFInfoHeaderLine,
  VCFFilterHeaderLine,
  VCFHeaderLine
}
import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.feature.FeatureDataset
import org.bdgenomics.adam.rdd.read.AlignmentRecordDataset
import org.bdgenomics.adam.rdd.variant.{
  GenotypeDataset,
  VariantDataset,
  VariantContextDataset
}
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  Feature,
  Genotype,
  Sample,
  Variant
}
import org.bdgenomics.utils.instrumentation.MetricsListener
import org.bdgenomics.utils.instrumentation._

/**
 * Utility methods for use in adam-shell.
 */
object ADAMShell {

  /** Alignment record headers. */
  val alignmentHeaders = Array(
    new ASCIITableHeader("Reference Name"),
    new ASCIITableHeader("Start"),
    new ASCIITableHeader("End"),
    new ASCIITableHeader("Read Name"),
    new ASCIITableHeader("Sample"),
    new ASCIITableHeader("Read Group")
  )

  /**
   * Print attribute values for alignment records in the specified AlignmentRecordDataset up to the limit.
   *
   * @param alignments AlignmentRecordDataset.
   * @param keys Sequence of attribute keys.
   * @param limit Number of alignment records to print attribute values for. Defaults to 10.
   */
  def printAlignmentAttributes(alignments: AlignmentRecordDataset, keys: Seq[String], limit: Int = 10): Unit = {
    printAlignmentAttributes(alignments.rdd.take(limit), keys)
  }

  private def findMatchingAttribute(key: String, attributes: String): String = {
    AttributeUtils.parseAttributes(attributes).find(_.tag == key).fold("")(_.value.toString)
  }

  /**
   * Print attribute values for the specified alignment records.
   *
   * @param alignments Sequence of alignments.
   * @param keys Sequence of attribute keys.
   */
  def printAlignmentAttributes(alignments: Seq[AlignmentRecord], keys: Seq[String]): Unit = {
    val header = alignmentHeaders ++ keys.map(key => new ASCIITableHeader(key))

    val rows: Array[Array[String]] = alignments.map(a => Array[String](
      a.getReferenceName(),
      a.getStart().toString,
      a.getEnd().toString,
      Option(a.getReadName()).getOrElse(""),
      Option(a.getReadGroupSampleId()).getOrElse(""),
      Option(a.getReadGroupId()).getOrElse("")
    ) ++ keys.map(key => findMatchingAttribute(key, a.getAttributes()))).toArray

    println("\nAlignment Attributes\n" + new ASCIITable(header, rows).toString)
  }

  /** Feature headers. */
  val featureHeaders = Array(
    new ASCIITableHeader("Reference Name"),
    new ASCIITableHeader("Start"),
    new ASCIITableHeader("End"),
    new ASCIITableHeader("Strand"),
    new ASCIITableHeader("Name"),
    new ASCIITableHeader("Identifier"),
    new ASCIITableHeader("Type"),
    new ASCIITableHeader("Score")
  )

  /**
   * Print attribute values for features in the specified FeatureDataset up to the limit.
   *
   * @param features FeatureDataset.
   * @param keys Sequence of attribute keys.
   * @param limit Number of features to print attribute values for. Defaults to 10.
   */
  def printFeatureAttributes(features: FeatureDataset, keys: Seq[String], limit: Int = 10): Unit = {
    printFeatureAttributes(features.rdd.take(limit), keys)
  }

  /**
   * Print attribute values for the specified features.
   *
   * @param alignments Sequence of features.
   * @param keys Sequence of attribute keys.
   */
  def printFeatureAttributes(features: Seq[Feature], keys: Seq[String]): Unit = {
    val header = featureHeaders ++ keys.map(key => new ASCIITableHeader(key))

    val rows: Array[Array[String]] = features.map(f => Array[String](
      f.getReferenceName(),
      f.getStart().toString,
      f.getEnd().toString,
      f.getStrand().toString,
      Option(f.getName()).getOrElse(""),
      Option(f.getFeatureId()).getOrElse(""),
      Option(f.getFeatureType()).getOrElse(""),
      Option(f.getScore()).fold("")(_.toString)
    ) ++ keys.map(key => Option(f.getAttributes().get(key)).getOrElse(""))).toArray

    println("\nFeature Attributes\n" + new ASCIITable(header, rows).toString)
  }

  /**
   * Print VCF FORMAT field attributes for genotypes in the specified GenotypeDataset up to the limit.
   *
   * @param genotypes GenotypeDataset.
   * @param keys Sequence of VCF FORMAT field attribute keys.
   * @param limit Number of genotypes to print VCF FORMAT field attribute values for. Defaults to 10.
   */
  def printFormatFields(genotypes: GenotypeDataset, keys: Seq[String], limit: Int = 10): Unit = {
    printFormatFields(genotypes.rdd.take(limit), keys, genotypes.headerLines)
  }

  /** Genotype headers. */
  val genotypeHeaders = Array(
    new ASCIITableHeader("Reference Name"),
    new ASCIITableHeader("Start"),
    new ASCIITableHeader("End"),
    new ASCIITableHeader("Ref", Alignment.Left),
    new ASCIITableHeader("Alt", Alignment.Left),
    new ASCIITableHeader("Alleles", Alignment.Center),
    new ASCIITableHeader("Sample")
  )

  /**
   * Print VCF FORMAT field attributes for the specified genotypes.
   *
   * @param genotypes Sequence of genotypes.
   * @param keys Sequence of VCF FORMAT field attribute keys.
   * @param headerLines Sequence of VCF header lines.
   */
  def printFormatFields(genotypes: Seq[Genotype], keys: Seq[String], headerLines: Seq[VCFHeaderLine]): Unit = {
    println("Format Header Lines")
    headerLines.filter(line => (line.isInstanceOf[VCFFormatHeaderLine] && keys.contains(line.asInstanceOf[VCFFormatHeaderLine].getID()))).foreach(println)

    val header = genotypeHeaders ++ keys.map(key => new ASCIITableHeader(key))

    val rows: Array[Array[String]] = genotypes.map(g => Array[String](
      g.getReferenceName(),
      g.getStart().toString,
      g.getEnd().toString,
      g.getVariant().getReferenceAllele(),
      g.getVariant().getAlternateAllele(),
      g.getAlleles().toString,
      g.getSampleId()
    ) ++ keys.map(key => Option(g.getVariantCallingAnnotations().getAttributes().get(key)).getOrElse(""))).toArray

    println("\nGenotype Format Fields\n" + new ASCIITable(header, rows).toString)
  }

  /**
   * Print genotype filter values for genotypes in the specified GenotypeDataset up to the limit.
   *
   * @param genotypes GenotypeDataset.
   * @param limit Number of genotypes to print genotype filter values for. Defaults to 10.
   */
  def printGenotypeFilters(genotypes: GenotypeDataset, limit: Int = 10): Unit = {
    printGenotypeFilters(genotypes.rdd.take(limit), genotypes.headerLines)
  }

  /**
   * Print genotype filter values for the specified genotypes.
   *
   * @param genotypes Sequence of genotypes.
   * @param headerLines Sequence of VCF header lines.
   */
  def printGenotypeFilters(genotypes: Seq[Genotype], headerLines: Seq[VCFHeaderLine]): Unit = {
    println("Filter Header Lines")
    headerLines.filter(line => line.isInstanceOf[VCFFilterHeaderLine]).foreach(println)

    val header = genotypeHeaders ++ Array(
      new ASCIITableHeader("Genotype Filters Applied"),
      new ASCIITableHeader("Genotype Filters Passed"),
      new ASCIITableHeader("Genotype Filters Failed")
    )

    val rows: Array[Array[String]] = genotypes.map(g => Array[String](
      g.getReferenceName(),
      g.getStart().toString,
      g.getEnd().toString,
      g.getVariant().getReferenceAllele(),
      g.getVariant().getAlternateAllele(),
      g.getAlleles().toString,
      g.getSampleId(),
      g.getVariantCallingAnnotations().getFiltersApplied().toString,
      g.getVariantCallingAnnotations().getFiltersPassed().toString,
      g.getVariantCallingAnnotations().getFiltersFailed().toString
    )).toArray

    println("\nGenotype Filters\n" + new ASCIITable(header, rows).toString)
  }

  /**
   * Print attribute values for the specified samples.
   *
   * @param samples Sequence of samples.
   * @param keys Sequence of attribute keys.
   */
  def printSampleAttributes(samples: Seq[Sample], keys: Seq[String]): Unit = {
    val header = Array(
      new ASCIITableHeader("Identifier"),
      new ASCIITableHeader("Name")
    ) ++ keys.map(key => new ASCIITableHeader(key)) ++ Array(new ASCIITableHeader("Processing Steps"))

    val rows: Array[Array[String]] = samples.map(s => Array[String](
      s.getId(),
      s.getName()
    ) ++ keys.map(key => Option(s.getAttributes().get(key)).getOrElse("")) ++ Array(Option(s.getProcessingSteps().toString).getOrElse(""))).toArray

    println("\nSample Attributes\n" + new ASCIITable(header, rows).toString)
  }

  /**
   * Print filter values for variants in the specified VariantDataset up to the limit.
   *
   * @param variants VariantDataset.
   * @param limit Number of variants to print filter values for. Defaults to 10.
   */
  def printVariantFilters(variants: VariantDataset, limit: Int = 10): Unit = {
    printVariantFilters(variants.rdd.take(limit), variants.headerLines)
  }

  /** Variant headers. */
  val variantHeaders = Array(
    new ASCIITableHeader("Reference Name"),
    new ASCIITableHeader("Start"),
    new ASCIITableHeader("End"),
    new ASCIITableHeader("Ref", Alignment.Left),
    new ASCIITableHeader("Alt", Alignment.Left)
  )

  /**
   * Print filter values for the specified variants.
   *
   * @param variants Sequence of variants to print filter values for.
   * @param headerLines Sequence of VCF header lines.
   */
  def printVariantFilters(variants: Seq[Variant], headerLines: Seq[VCFHeaderLine]): Unit = {
    println("Filter Header Lines")
    headerLines.filter(line => line.isInstanceOf[VCFFilterHeaderLine]).foreach(println)

    val header = variantHeaders ++ Array(
      new ASCIITableHeader("Filters Applied"),
      new ASCIITableHeader("Filters Passed"),
      new ASCIITableHeader("Filters Failed")
    )

    val rows: Array[Array[String]] = variants.map(v => Array[String](
      v.getReferenceName(),
      v.getStart().toString,
      v.getEnd().toString,
      v.getReferenceAllele(),
      v.getAlternateAllele(),
      v.getFiltersApplied().toString,
      v.getFiltersPassed().toString,
      v.getFiltersFailed().toString
    )).toArray

    println("\nVariant Filters\n" + new ASCIITable(header, rows).toString)
  }

  /**
   * Print VCF INFO field attributes for variants in the specified VariantDataset up to the limit.
   *
   * @param variants VariantDataset.
   * @param keys Sequence of VCF INFO field attribute keys.
   * @param limit Number of variants to print VCF INFO field attribute values for. Defaults to 10.
   */
  def printInfoFields(variants: VariantDataset, keys: Seq[String], limit: Int = 10): Unit = {
    printInfoFields(variants.rdd.take(limit), keys, variants.headerLines)
  }

  /**
   * Print VCF INFO field attributes for the specified variants.
   *
   * @param variants Sequence of variants.
   * @param keys Sequence of VCF INFO field attribute keys.
   * @param headerLines Sequence of VCF header lines.
   */
  def printInfoFields(variants: Seq[Variant], keys: Seq[String], headerLines: Seq[VCFHeaderLine]): Unit = {
    println("Info Header Lines")
    headerLines.filter(line => (line.isInstanceOf[VCFInfoHeaderLine] && keys.contains(line.asInstanceOf[VCFInfoHeaderLine].getID()))).foreach(println)

    val header = variantHeaders ++ keys.map(key => new ASCIITableHeader(key))

    val rows: Array[Array[String]] = variants.map(v => Array[String](
      v.getReferenceName(),
      v.getStart().toString,
      v.getEnd().toString,
      v.getReferenceAllele(),
      v.getAlternateAllele()
    ) ++ keys.map(key => Option(v.getAnnotation().getAttributes().get(key)).getOrElse(""))).toArray

    println("\nVariant Info Fields\n" + new ASCIITable(header, rows).toString)
  }

  /**
   * Create and return a new metrics listener for the specified Spark context.
   *
   * @param sc Spark context.
   * @return Return a new metrics listener for the specified Spark context.
   */
  def createMetricsListener(sc: SparkContext): MetricsListener = {
    val metricsListener = new MetricsListener(new RecordedMetrics())
    sc.addSparkListener(metricsListener)
    Metrics.initialize(sc)
    metricsListener
  }

  /**
   * Print metrics gathered by the specified metrics listener.
   *
   * @param metricsListener Metrics listener.
   */
  def printMetrics(metricsListener: MetricsListener): Unit = printMetrics(None, metricsListener)

  /**
   * Print metrics gathered by the specified metrics listener.
   *
   * @param totalTime Total execution time, in ns.
   * @param metricsListener Metrics listener.
   */
  def printMetrics(totalTime: Long, metricsListener: MetricsListener): Unit = printMetrics(Some(totalTime), metricsListener)

  /**
   * Print metrics gathered by the specified metrics listener.
   *
   * @param optTotalTime Optional total execution time, in ns.
   * @param metricsListener Metrics listener.
   */
  private def printMetrics(optTotalTime: Option[Long], metricsListener: MetricsListener): Unit = {
    val stringWriter = new StringWriter()
    val out = new PrintWriter(stringWriter)
    optTotalTime.foreach(t => out.println("Overall Duration: " + DurationFormatting.formatNanosecondDuration(t)))
    out.println("Metrics:")
    out.println()
    Metrics.print(out, Some(metricsListener.metrics.sparkMetrics.stageTimes))
    out.println()
    metricsListener.metrics.sparkMetrics.print(out)
    out.flush()
    println(stringWriter.getBuffer.toString)
  }
}
