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
package org.bdgenomics.adam.api.java

import htsjdk.samtools.ValidationStringency
import org.apache.spark.api.java.JavaSparkContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.ds.ADAMContext
import org.bdgenomics.adam.ds.feature.{ CoverageDataset, FeatureDataset }
import org.bdgenomics.adam.ds.fragment.FragmentDataset
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.ds.sequence.{ SequenceDataset, SliceDataset }
import org.bdgenomics.adam.ds.variant.{
  GenotypeDataset,
  VariantDataset
}
import org.bdgenomics.adam.util.ReferenceFile
import scala.collection.JavaConversions._

object JavaADAMContext {
  // convert to and from java/scala implementations
  implicit def fromADAMContext(ac: ADAMContext): JavaADAMContext = new JavaADAMContext(ac)
  implicit def toADAMContext(jac: JavaADAMContext): ADAMContext = jac.ac
}

/**
 * The JavaADAMContext provides java-friendly functions on top of ADAMContext.
 *
 * @param ac The ADAMContext to wrap.
 */
class JavaADAMContext(val ac: ADAMContext) extends Serializable {

  /**
   * @return Returns the Java Spark Context associated with this Java ADAM Context.
   */
  def getSparkContext: JavaSparkContext = new JavaSparkContext(ac.sc)

  /**
   * (Java-specific) Load alignments into an AlignmentDataset.
   *
   * Loads path names ending in:
   * * .bam/.cram/.sam as BAM/CRAM/SAM format,
   * * .fa/.fasta as FASTA format,
   * * .fq/.fastq as FASTQ format, and
   * * .ifq as interleaved FASTQ format.
   *
   * If none of these match, fall back to Parquet + Avro.
   *
   * For FASTA, FASTQ, and interleaved FASTQ formats, compressed files are supported
   * through compression codecs configured in Hadoop, which by default include .gz and .bz2,
   * but can include more.
   *
   * @see ADAMContext#loadAlignments
   *
   * @param pathName The path name to load alignments from.
   *   Globs/directories are supported, although file extension must be present
   *   for BAM/CRAM/SAM, FASTA, and FASTQ formats.
   * @return Returns an AlignmentDataset which wraps the genomic dataset of alignments,
   *   sequence dictionary representing contigs the alignments may be aligned to,
   *   and the read group dictionary for the alignments if one is available.
   */
  def loadAlignments(pathName: java.lang.String): AlignmentDataset = {
    ac.loadAlignments(pathName)
  }

  /**
   * (Java-specific) Load alignments into an AlignmentDataset.
   *
   * Loads path names ending in:
   * * .bam/.cram/.sam as BAM/CRAM/SAM format,
   * * .fa/.fasta as FASTA format,
   * * .fq/.fastq as FASTQ format, and
   * * .ifq as interleaved FASTQ format.
   *
   * If none of these match, fall back to Parquet + Avro.
   *
   * For FASTA, FASTQ, and interleaved FASTQ formats, compressed files are supported
   * through compression codecs configured in Hadoop, which by default include .gz and .bz2,
   * but can include more.
   *
   * @see ADAMContext#loadAlignments
   *
   * @param pathName The path name to load alignments from.
   *   Globs/directories are supported, although file extension must be present
   *   for BAM/CRAM/SAM, FASTA, and FASTQ formats.
   * @param stringency The validation stringency to use when validating
   *   BAM/CRAM/SAM or FASTQ formats.
   * @return Returns an AlignmentDataset which wraps the genomic dataset of alignments,
   *   sequence dictionary representing contigs the alignments may be aligned to,
   *   and the read group dictionary for the alignments if one is available.
   */
  def loadAlignments(pathName: java.lang.String,
                     stringency: ValidationStringency): AlignmentDataset = {
    ac.loadAlignments(pathName,
      stringency = stringency)
  }

  /**
   * (Java-specific) Functions like loadBam, but uses BAM index files to look at fewer blocks,
   * and only returns records within the specified ReferenceRegions. BAM index file required.
   *
   * @param pathName The path name to load indexed BAM formatted alignments from.
   *   Globs/directories are supported.
   * @param viewRegions Iterable of ReferenceRegion we are filtering on.
   * @param stringency The validation stringency to use when validating the
   *   BAM/CRAM/SAM format header. Defaults to ValidationStringency.STRICT.
   * @return Returns an AlignmentDataset which wraps the genomic dataset of alignments,
   *   sequence dictionary representing contigs the alignments may be aligned to,
   *   and the read group dictionary for the alignments if one is available.
   */
  def loadIndexedBam(
    pathName: String,
    viewRegions: java.util.List[ReferenceRegion],
    stringency: ValidationStringency): AlignmentDataset = {

    ac.loadIndexedBam(pathName, viewRegions.toIterable, stringency = stringency)
  }

  /**
   * (Java-specific) Load fragments into a FragmentDataset.
   *
   * Loads path names ending in:
   * * .bam/.cram/.sam as BAM/CRAM/SAM format and
   * * .ifq as interleaved FASTQ format.
   *
   * If none of these match, fall back to Parquet + Avro.
   *
   * For interleaved FASTQ format, compressed files are supported through compression codecs
   * configured in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadFragments
   *
   * @param pathName The path name to load fragments from.
   *   Globs/directories are supported, although file extension must be present
   *   for BAM/CRAM/SAM and FASTQ formats.
   * @return Returns a FragmentDataset.
   */
  def loadFragments(pathName: java.lang.String): FragmentDataset = {
    ac.loadFragments(pathName)
  }

  /**
   * (Java-specific) Load fragments into a FragmentDataset.
   *
   * Loads path names ending in:
   * * .bam/.cram/.sam as BAM/CRAM/SAM format and
   * * .ifq as interleaved FASTQ format.
   *
   * If none of these match, fall back to Parquet + Avro.
   *
   * For interleaved FASTQ format, compressed files are supported through compression codecs
   * configured in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadFragments
   *
   * @param pathName The path name to load fragments from.
   *   Globs/directories are supported, although file extension must be present
   *   for BAM/CRAM/SAM and FASTQ formats.
   * @param stringency The validation stringency to use when validating BAM/CRAM/SAM or FASTQ formats.
   * @return Returns a FragmentDataset.
   */
  def loadFragments(pathName: java.lang.String,
                    stringency: ValidationStringency): FragmentDataset = {
    ac.loadFragments(pathName, stringency = stringency)
  }

  /**
   * (Java-specific) Load features into a FeatureDataset.
   *
   * Loads path names ending in:
   * * .bed as BED6/12 format,
   * * .gff3 as GFF3 format,
   * * .gtf/.gff as GTF/GFF2 format,
   * * .narrow[pP]eak as NarrowPeak format, and
   * * .interval_list as IntervalList format.
   *
   * If none of these match, fall back to Parquet + Avro.
   *
   * For BED6/12, GFF3, GTF/GFF2, NarrowPeak, and IntervalList formats, compressed files
   * are supported through compression codecs configured in Hadoop, which by default include
   * .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadFeatures
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported, although file extension must be present
   *   for BED6/12, GFF3, GTF/GFF2, NarrowPeak, or IntervalList formats.
   * @return Returns a FeatureDataset.
   */
  def loadFeatures(pathName: java.lang.String): FeatureDataset = {
    ac.loadFeatures(pathName)
  }

  /**
   * (Java-specific) Load features into a FeatureDataset.
   *
   * Loads path names ending in:
   * * .bed as BED6/12 format,
   * * .gff3 as GFF3 format,
   * * .gtf/.gff as GTF/GFF2 format,
   * * .narrow[pP]eak as NarrowPeak format, and
   * * .interval_list as IntervalList format.
   *
   * If none of these match, fall back to Parquet + Avro.
   *
   * For BED6/12, GFF3, GTF/GFF2, NarrowPeak, and IntervalList formats, compressed files
   * are supported through compression codecs configured in Hadoop, which by default include
   * .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadFeatures
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported, although file extension must be present
   *   for BED6/12, GFF3, GTF/GFF2, NarrowPeak, or IntervalList formats.
   * @param stringency The validation stringency to use when validating BED6/12, GFF3,
   *   GTF/GFF2, NarrowPeak, or IntervalList formats.
   * @return Returns a FeatureDataset.
   */
  def loadFeatures(pathName: java.lang.String,
                   stringency: ValidationStringency): FeatureDataset = {
    ac.loadFeatures(pathName, stringency = stringency)
  }

  /**
   * (Java-specific) Load features into a FeatureDataset and convert to a CoverageDataset.
   * Coverage is stored in the score field of Feature.
   *
   * Loads path names ending in:
   * * .bed as BED6/12 format,
   * * .gff3 as GFF3 format,
   * * .gtf/.gff as GTF/GFF2 format,
   * * .narrow[pP]eak as NarrowPeak format, and
   * * .interval_list as IntervalList format.
   *
   * If none of these match, fall back to Parquet + Avro.
   *
   * For BED6/12, GFF3, GTF/GFF2, NarrowPeak, and IntervalList formats, compressed files
   * are supported through compression codecs configured in Hadoop, which by default include
   * .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadCoverage
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported, although file extension must be present
   *   for BED6/12, GFF3, GTF/GFF2, NarrowPeak, or IntervalList formats.
   * @return Returns a FeatureDataset converted to a CoverageDataset.
   */
  def loadCoverage(pathName: java.lang.String): CoverageDataset = {
    ac.loadCoverage(pathName)
  }

  /**
   * (Java-specific) Load features into a FeatureDataset and convert to a CoverageDataset.
   * Coverage is stored in the score field of Feature.
   *
   * Loads path names ending in:
   * * .bed as BED6/12 format,
   * * .gff3 as GFF3 format,
   * * .gtf/.gff as GTF/GFF2 format,
   * * .narrow[pP]eak as NarrowPeak format, and
   * * .interval_list as IntervalList format.
   *
   * If none of these match, fall back to Parquet + Avro.
   *
   * For BED6/12, GFF3, GTF/GFF2, NarrowPeak, and IntervalList formats, compressed files
   * are supported through compression codecs configured in Hadoop, which by default include
   * .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadCoverage
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported, although file extension must be present
   *   for BED6/12, GFF3, GTF/GFF2, NarrowPeak, or IntervalList formats.
   * @param stringency The validation stringency to use when validating BED6/12, GFF3,
   *   GTF/GFF2, NarrowPeak, or IntervalList formats.
   * @return Returns a FeatureDataset converted to a CoverageDataset.
   */
  def loadCoverage(pathName: java.lang.String,
                   stringency: ValidationStringency): CoverageDataset = {
    ac.loadCoverage(pathName,
      stringency = stringency)
  }

  /**
   * (Java-specific) Load genotypes into a GenotypeDataset.
   *
   * If the path name has a .vcf/.vcf.gz/.vcf.bgzf/.vcf.bgz extension, load as VCF format.
   * Else, fall back to Parquet + Avro.
   *
   * @see ADAMContext#loadGenotypes
   *
   * @param pathName The path name to load genotypes from.
   *   Globs/directories are supported, although file extension must be present
   *   for VCF format.
   * @return Returns a GenotypeDataset.
   */
  def loadGenotypes(pathName: java.lang.String): GenotypeDataset = {
    ac.loadGenotypes(pathName)
  }

  /**
   * (Java-specific) Load genotypes into a GenotypeDataset.
   *
   * If the path name has a .vcf/.vcf.gz/.vcf.bgzf/.vcf.bgz extension, load as VCF format.
   * Else, fall back to Parquet + Avro.
   *
   * @see ADAMContext#loadGenotypes
   *
   * @param pathName The path name to load genotypes from.
   *   Globs/directories are supported, although file extension must be present
   *   for VCF format.
   * @param stringency The validation stringency to use when validating VCF format.
   * @return Returns a GenotypeDataset.
   */
  def loadGenotypes(pathName: java.lang.String,
                    stringency: ValidationStringency): GenotypeDataset = {
    ac.loadGenotypes(pathName,
      stringency = stringency)
  }

  /**
   * (Java-specific) Load variants into a VariantDataset.
   *
   * If the path name has a .vcf/.vcf.gz/.vcf.bgzf/.vcf.bgz extension, load as VCF format.
   * Else, fall back to Parquet + Avro.
   *
   * @see ADAMContext#loadVariants
   *
   * @param pathName The path name to load variants from.
   *   Globs/directories are supported, although file extension must be present for VCF format.
   * @return Returns a VariantDataset.
   */
  def loadVariants(pathName: java.lang.String): VariantDataset = {
    ac.loadVariants(pathName)
  }

  /**
   * (Java-specific) Load variants into a VariantDataset.
   *
   * If the path name has a .vcf/.vcf.gz/.vcf.bgzf/.vcf.bgz extension, load as VCF format.
   * Else, fall back to Parquet + Avro.
   *
   * @see ADAMContext#loadVariants
   *
   * @param pathName The path name to load variants from.
   *   Globs/directories are supported, although file extension must be present for VCF format.
   * @param stringency The validation stringency to use when validating VCF format.
   * @return Returns a VariantDataset.
   */
  def loadVariants(pathName: java.lang.String,
                   stringency: ValidationStringency): VariantDataset = {
    ac.loadVariants(pathName, stringency = stringency)
  }

  /**
   * (Java-specific) Load reference sequences into a broadcastable ReferenceFile.
   *
   * If the path name has a .2bit extension, loads a 2bit file. Else, uses loadSlices
   * to load the reference as an RDD, which is then collected to the driver.
   *
   * @see ADAMContext#loadSlices
   *
   * @param pathName The path name to load reference sequences from.
   *   Globs/directories for 2bit format are not supported.
   * @param maximumLength Maximum fragment length. Defaults to 10000L. Values greater
   *   than 1e9 should be avoided.
   * @return Returns a broadcastable ReferenceFile.
   */
  def loadReferenceFile(pathName: java.lang.String,
                        maximumLength: java.lang.Long): ReferenceFile = {
    ac.loadReferenceFile(pathName, maximumLength)
  }

  /**
   * (Java-specific) Load reference sequences into a broadcastable ReferenceFile.
   *
   * If the path name has a .2bit extension, loads a 2bit file. Else, uses loadSlices
   * to load the reference as an RDD, which is then collected to the driver. Uses a
   * maximum fragment length of 10kbp.
   *
   * @see ADAMContext#loadSlices
   *
   * @param pathName The path name to load reference sequences from.
   *   Globs/directories for 2bit format are not supported.
   * @return Returns a broadcastable ReferenceFile.
   */
  def loadReferenceFile(pathName: java.lang.String): ReferenceFile = {
    loadReferenceFile(pathName, 10000L)
  }

  /**
   * (Java-specific) Load DNA sequences into a SequenceDataset.
   *
   * If the path name has a .fa/.fasta extension, load as FASTA format.
   * Else, fall back to Parquet + Avro.
   *
   * For FASTA format, compressed files are supported through compression codecs configured
   * in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadFastaDna
   * @see ADAMContext#loadParquetSequences
   *
   * @param pathName The path name to load sequences from.
   *   Globs/directories are supported, although file extension must be present
   *   for FASTA format.
   * @return Returns a SequenceDataset containing DNA sequences.
   */
  def loadDnaSequences(pathName: java.lang.String): SequenceDataset = {
    ac.loadDnaSequences(pathName)
  }

  /**
   * (Java-specific) Load protein sequences into a SequenceDataset.
   *
   * If the path name has a .fa/.fasta extension, load as FASTA format.
   * Else, fall back to Parquet + Avro.
   *
   * For FASTA format, compressed files are supported through compression codecs configured
   * in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadFastaProtein
   * @see ADAMContext#loadParquetSequences
   *
   * @param pathName The path name to load sequences from.
   *   Globs/directories are supported, although file extension must be present
   *   for FASTA format.
   * @return Returns a SequenceDataset containing protein sequences.
   */
  def loadProteinSequences(pathName: java.lang.String): SequenceDataset = {
    ac.loadProteinSequences(pathName)
  }

  /**
   * (Java-specific) Load RNA sequences into a SequenceDataset.
   *
   * If the path name has a .fa/.fasta extension, load as FASTA format.
   * Else, fall back to Parquet + Avro.
   *
   * For FASTA format, compressed files are supported through compression codecs configured
   * in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @see ADAMContext#loadFastaRna
   * @see ADAMContext#loadParquetSequences
   *
   * @param pathName The path name to load sequences from.
   *   Globs/directories are supported, although file extension must be present
   *   for FASTA format.
   * @return Returns a SequenceDataset containing RNA sequences.
   */
  def loadRnaSequences(pathName: java.lang.String): SequenceDataset = {
    ac.loadRnaSequences(pathName)
  }

  /**
   * (Java/Python-specific) Load slices into a SliceDataset.
   *
   * If the path name has a .fa/.fasta extension, load as DNA in FASTA format.
   * Else, fall back to Parquet + Avro.
   *
   * For FASTA format, compressed files are supported through compression codecs configured
   * in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @param pathName The path name to load DNA slices from.
   *   Globs/directories are supported, although file extension must be present
   *   for FASTA format.
   * @param maximumLength Maximum slice length, reduced to Integer data type to support
   *   dispatch from Python.
   * @return Returns a SliceDataset.
   */
  def loadSlices(
    pathName: java.lang.String,
    maximumLength: java.lang.Integer): SliceDataset = {

    ac.loadSlices(pathName, maximumLength.toLong)
  }

  /**
   * (R-specific) Load slices into a SliceDataset.
   *
   * If the path name has a .fa/.fasta extension, load as DNA in FASTA format.
   * Else, fall back to Parquet + Avro.
   *
   * For FASTA format, compressed files are supported through compression codecs configured
   * in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @param pathName The path name to load DNA slices from.
   *   Globs/directories are supported, although file extension must be present
   *   for FASTA format.
   * @param maximumLength Maximum fragment length, in Double data type to support
   *   dispatch from SparkR.
   * @return Returns a SliceDataset.
   */
  def loadSlices(
    pathName: java.lang.String,
    maximumLength: java.lang.Double): SliceDataset = {

    ac.loadSlices(pathName, maximumLength.toLong)
  }
}
