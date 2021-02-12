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

import htsjdk.variant.vcf.{
  VCFFormatHeaderLine,
  VCFInfoHeaderLine,
  VCFFilterHeaderLine,
  VCFHeaderLine
}
import org.bdgenomics.adam.ds.feature.FeatureDataset
import org.bdgenomics.adam.ds.read.{ AlignmentDataset, ReadDataset }
import org.bdgenomics.adam.ds.sequence.{ SequenceDataset, SliceDataset }
import org.bdgenomics.adam.ds.variant.{
  GenotypeDataset,
  VariantDataset
}
import org.bdgenomics.formats.avro.{
  Alignment,
  Feature,
  Genotype,
  Read,
  Sample,
  Sequence,
  Slice,
  Variant
}

/**
 * Utility methods for use in adam-shell.
 */
object ADAMShell {

  /** Alignment headers. */
  val alignmentHeaders = Array(
    new ASCIITableHeader("Reference Name"),
    new ASCIITableHeader("Start"),
    new ASCIITableHeader("End"),
    new ASCIITableHeader("Read Name"),
    new ASCIITableHeader("Sample"),
    new ASCIITableHeader("Read Group")
  )

  /**
   * Print attribute values for alignments in the specified AlignmentDataset up to the limit.
   *
   * @param alignments AlignmentDataset.
   * @param keys Sequence of attribute keys. Defaults to empty.
   * @param limit Number of alignments to print attribute values for. Defaults to 10.
   */
  def printAlignmentAttributes(alignments: AlignmentDataset, keys: Seq[String] = Seq.empty, limit: Int = 10): Unit = {
    printAlignmentAttributes(alignments.rdd.take(limit), keys)
  }

  private def findMatchingAttribute(key: String, attributes: String): String = {
    AttributeUtils.parseAttributes(attributes).find(_.tag == key).fold("")(_.value.toString)
  }

  /**
   * Print attribute values for the specified alignments.
   *
   * @param alignments Sequence of alignments.
   * @param keys Sequence of attribute keys.
   */
  def printAlignmentAttributes(alignments: Seq[Alignment], keys: Seq[String]): Unit = {
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
   * @param keys Sequence of attribute keys. Defaults to empty.
   * @param limit Number of features to print attribute values for. Defaults to 10.
   */
  def printFeatureAttributes(features: FeatureDataset, keys: Seq[String] = Seq.empty, limit: Int = 10): Unit = {
    printFeatureAttributes(features.rdd.take(limit), keys)
  }

  /**
   * Print attribute values for the specified features.
   *
   * @param features Sequence of features.
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
   * @param keys Sequence of VCF FORMAT field attribute keys. Defaults to empty.
   * @param limit Number of genotypes to print VCF FORMAT field attribute values for. Defaults to 10.
   */
  def printFormatFields(genotypes: GenotypeDataset, keys: Seq[String] = Seq.empty, limit: Int = 10): Unit = {
    printFormatFields(genotypes.rdd.take(limit), keys, genotypes.headerLines)
  }

  /** Genotype headers. */
  val genotypeHeaders = Array(
    new ASCIITableHeader("Reference Name"),
    new ASCIITableHeader("Start"),
    new ASCIITableHeader("End"),
    new ASCIITableHeader("Ref", TextAlignment.Left),
    new ASCIITableHeader("Alt", TextAlignment.Left),
    new ASCIITableHeader("Alleles", TextAlignment.Center),
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

  /** Read headers. */
  val readHeaders = Array(
    new ASCIITableHeader("Name"),
    new ASCIITableHeader("Description"),
    new ASCIITableHeader("Alphabet"),
    new ASCIITableHeader("Length")
  )

  /**
   * Print attribute values for reads in the specified ReadDataset up to the limit.
   *
   * @param reads ReadDataset.
   * @param keys Sequence of attribute keys. Defaults to empty.
   * @param limit Number of reads to print attribute values for. Defaults to 10.
   */
  def printReadAttributes(reads: ReadDataset, keys: Seq[String] = Seq.empty, limit: Int = 10): Unit = {
    printReadAttributes(reads.rdd.take(limit), keys)
  }

  /**
   * Print attribute values for the specified reads.
   *
   * @param reads Sequence of reads.
   * @param keys Sequence of attribute keys.
   */
  def printReadAttributes(reads: Seq[Read], keys: Seq[String]): Unit = {
    val header = readHeaders ++ keys.map(key => new ASCIITableHeader(key))

    val rows: Array[Array[String]] = reads.map(r => Array[String](
      Option(r.getName()).getOrElse(""),
      Option(r.getDescription()).getOrElse(""),
      Option(r.getAlphabet()).fold("")(_.toString),
      Option(r.getLength()).fold("")(_.toString)
    ) ++ keys.map(key => Option(r.getAttributes().get(key)).getOrElse(""))).toArray

    println("\nRead Attributes\n" + new ASCIITable(header, rows).toString)
  }

  /**
   * Print attribute values for the specified samples.
   *
   * @param samples Sequence of samples.
   * @param keys Sequence of attribute keys. Defaults to empty.
   */
  def printSampleAttributes(samples: Seq[Sample], keys: Seq[String] = Seq.empty): Unit = {
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

  /** Sequence headers. */
  val sequenceHeaders = Array(
    new ASCIITableHeader("Name"),
    new ASCIITableHeader("Description"),
    new ASCIITableHeader("Alphabet"),
    new ASCIITableHeader("Length")
  )

  /**
   * Print attribute values for sequences in the specified SequenceDataset up to the limit.
   *
   * @param sequences SequenceDataset.
   * @param keys Sequence of attribute keys. Defaults to empty.
   * @param limit Number of sequences to print attribute values for. Defaults to 10.
   */
  def printSequenceAttributes(sequences: SequenceDataset, keys: Seq[String] = Seq.empty, limit: Int = 10): Unit = {
    printSequenceAttributes(sequences.rdd.take(limit), keys)
  }

  /**
   * Print attribute values for the specified sequences.
   *
   * @param sequences Sequence of sequences.
   * @param keys Sequence of attribute keys.
   */
  def printSequenceAttributes(sequences: Seq[Sequence], keys: Seq[String]): Unit = {
    val header = sequenceHeaders ++ keys.map(key => new ASCIITableHeader(key))

    val rows: Array[Array[String]] = sequences.map(s => Array[String](
      Option(s.getName()).getOrElse(""),
      Option(s.getDescription()).getOrElse(""),
      Option(s.getAlphabet()).fold("")(_.toString),
      Option(s.getLength()).fold("")(_.toString)
    ) ++ keys.map(key => Option(s.getAttributes().get(key)).getOrElse(""))).toArray

    println("\nSequence Attributes\n" + new ASCIITable(header, rows).toString)
  }

  /** Slice headers. */
  val sliceHeaders = Array(
    new ASCIITableHeader("Name"),
    new ASCIITableHeader("Description"),
    new ASCIITableHeader("Alphabet"),
    new ASCIITableHeader("Start"),
    new ASCIITableHeader("End"),
    new ASCIITableHeader("Strand"),
    new ASCIITableHeader("Length"),
    new ASCIITableHeader("Total length"),
    new ASCIITableHeader("Index"),
    new ASCIITableHeader("Slices")
  )

  /**
   * Print attribute values for slices in the specified SliceDataset up to the limit.
   *
   * @param slices SliceDataset.
   * @param keys Sequence of attribute keys. Defaults to empty.
   * @param limit Number of slices to print attribute values for. Defaults to 10.
   */
  def printSliceAttributes(slices: SliceDataset, keys: Seq[String] = Seq.empty, limit: Int = 10): Unit = {
    printSliceAttributes(slices.rdd.take(limit), keys)
  }

  /**
   * Print attribute values for the specified slices.
   *
   * @param slices Sequence of slices.
   * @param keys Sequence of attribute keys.
   */
  def printSliceAttributes(slices: Seq[Slice], keys: Seq[String]): Unit = {
    val header = sliceHeaders ++ keys.map(key => new ASCIITableHeader(key))

    val rows: Array[Array[String]] = slices.map(s => Array[String](
      Option(s.getName()).getOrElse(""),
      Option(s.getDescription()).getOrElse(""),
      Option(s.getAlphabet()).fold("")(_.toString),
      Option(s.getStart()).fold("")(_.toString),
      Option(s.getEnd()).fold("")(_.toString),
      Option(s.getStrand()).fold("")(_.toString),
      Option(s.getLength()).fold("")(_.toString),
      Option(s.getTotalLength()).fold("")(_.toString),
      Option(s.getIndex()).fold("")(_.toString),
      Option(s.getSlices()).fold("")(_.toString)
    ) ++ keys.map(key => Option(s.getAttributes().get(key)).getOrElse(""))).toArray

    println("\nSlice Attributes\n" + new ASCIITable(header, rows).toString)
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
    new ASCIITableHeader("Ref", TextAlignment.Left),
    new ASCIITableHeader("Alt", TextAlignment.Left)
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
   * @param keys Sequence of VCF INFO field attribute keys. Defaults to empty.
   * @param limit Number of variants to print VCF INFO field attribute values for. Defaults to 10.
   */
  def printInfoFields(variants: VariantDataset, keys: Seq[String] = Seq.empty, limit: Int = 10): Unit = {
    printInfoFields(variants.rdd.take(limit), keys, variants.headerLines)
  }

  /**
   * Print VCF INFO field attributes for the specified variants.
   *
   * @param variants Sequence of variants.
   * @param keys Sequence of VCF INFO field attribute keys. Defaults to empty.
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
}
