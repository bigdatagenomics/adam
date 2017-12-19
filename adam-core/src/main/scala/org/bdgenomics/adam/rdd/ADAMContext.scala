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

import java.io.{ File, FileNotFoundException, InputStream }
import htsjdk.samtools.{ SAMFileHeader, SAMProgramRecord, ValidationStringency }
import htsjdk.samtools.util.Locatable
import htsjdk.variant.vcf.{
  VCFHeader,
  VCFCompoundHeaderLine,
  VCFFormatHeaderLine,
  VCFHeaderLine,
  VCFInfoHeaderLine
}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{ GenericDatumReader, GenericRecord, IndexedRecord }
import org.apache.avro.specific.{ SpecificDatumReader, SpecificRecord, SpecificRecordBase }
import org.apache.hadoop.fs.{ FileSystem, Path, PathFilter }
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.parquet.avro.{ AvroParquetInputFormat, AvroReadSupport }
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.converters._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.io._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.projections.{
  FeatureField,
  Projection
}
import org.bdgenomics.adam.rdd.contig.{
  DatasetBoundNucleotideContigFragmentRDD,
  NucleotideContigFragmentRDD,
  ParquetUnboundNucleotideContigFragmentRDD,
  RDDBoundNucleotideContigFragmentRDD
}
import org.bdgenomics.adam.rdd.feature._
import org.bdgenomics.adam.rdd.fragment.{
  DatasetBoundFragmentRDD,
  FragmentRDD,
  ParquetUnboundFragmentRDD,
  RDDBoundFragmentRDD
}
import org.bdgenomics.adam.rdd.read.{
  AlignmentRecordRDD,
  DatasetBoundAlignmentRecordRDD,
  RepairPartitions,
  ParquetUnboundAlignmentRecordRDD,
  RDDBoundAlignmentRecordRDD
}
import org.bdgenomics.adam.rdd.variant._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.sql.{
  AlignmentRecord => AlignmentRecordProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  NucleotideContigFragment => NucleotideContigFragmentProduct,
  Variant => VariantProduct
}
import org.bdgenomics.adam.util.FileExtensions._
import org.bdgenomics.adam.util.{
  GenomeFileReader,
  ReferenceContigMap,
  ReferenceFile,
  SequenceDictionaryReader,
  TwoBitFile
}
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  Contig,
  Feature,
  Fragment,
  Genotype,
  NucleotideContigFragment,
  ProcessingStep,
  RecordGroup => RecordGroupMetadata,
  Sample,
  Variant
}
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.bdgenomics.utils.misc.{ HadoopUtil, Logging }
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.seqdoop.hadoop_bam._
import org.seqdoop.hadoop_bam.util._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.parsing.json.JSON

/**
 * Case class that wraps a reference region for use with the Indexed VCF/BAM loaders.
 *
 * @param rr Reference region to wrap.
 */
private case class LocatableReferenceRegion(rr: ReferenceRegion) extends Locatable {

  /**
   * @return the start position in a 1-based closed coordinate system.
   */
  def getStart(): Int = rr.start.toInt + 1

  /**
   * @return the end position in a 1-based closed coordinate system.
   */
  def getEnd(): Int = rr.end.toInt

  /**
   * @return the reference contig this interval is on.
   */
  def getContig(): String = rr.referenceName
}

/**
 * This singleton provides an implicit conversion from a SparkContext to the
 * ADAMContext, as well as implicit functions for the Pipe API.
 */
object ADAMContext {

  // conversion functions for pipes
  implicit def sameTypeConversionFn[T, U <: GenomicRDD[T, U]](gRdd: U,
                                                              rdd: RDD[T]): U = {
    // hijack the transform function to discard the old RDD
    gRdd.transform(oldRdd => rdd)
  }

  implicit def contigsToCoverageConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    rdd: RDD[Coverage]): CoverageRDD = {
    new RDDBoundCoverageRDD(rdd, gRdd.sequences, None)
  }

  implicit def contigsToCoverageDatasetConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    ds: Dataset[Coverage]): CoverageRDD = {
    new DatasetBoundCoverageRDD(ds, gRdd.sequences)
  }

  implicit def contigsToFeaturesConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    rdd: RDD[Feature]): FeatureRDD = {
    new RDDBoundFeatureRDD(rdd, gRdd.sequences, None)
  }

  implicit def contigsToFeaturesDatasetConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    ds: Dataset[FeatureProduct]): FeatureRDD = {
    new DatasetBoundFeatureRDD(ds, gRdd.sequences)
  }

  implicit def contigsToFragmentsConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    rdd: RDD[Fragment]): FragmentRDD = {
    new RDDBoundFragmentRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def contigsToFragmentsDatasetConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    ds: Dataset[FragmentProduct]): FragmentRDD = {
    new DatasetBoundFragmentRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def contigsToAlignmentRecordsConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    new RDDBoundAlignmentRecordRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def contigsToAlignmentRecordsDatasetConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    new DatasetBoundAlignmentRecordRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def contigsToGenotypesConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    rdd: RDD[Genotype]): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def contigsToGenotypesDatasetConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    ds: Dataset[GenotypeProduct]): GenotypeRDD = {
    new DatasetBoundGenotypeRDD(ds,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def contigsToVariantsConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    rdd: RDD[Variant]): VariantRDD = {
    new RDDBoundVariantRDD(rdd,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def contigsToVariantsDatasetConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    ds: Dataset[VariantProduct]): VariantRDD = {
    new DatasetBoundVariantRDD(ds,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def contigsToVariantContextConversionFn(
    gRdd: NucleotideContigFragmentRDD,
    rdd: RDD[VariantContext]): VariantContextRDD = {
    VariantContextRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def coverageToContigsConversionFn(
    gRdd: CoverageRDD,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    new RDDBoundNucleotideContigFragmentRDD(rdd, gRdd.sequences, None)
  }

  implicit def coverageToContigsDatasetConversionFn(
    gRdd: CoverageRDD,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    new DatasetBoundNucleotideContigFragmentRDD(ds, gRdd.sequences)
  }

  implicit def coverageToFeaturesConversionFn(
    gRdd: CoverageRDD,
    rdd: RDD[Feature]): FeatureRDD = {
    new RDDBoundFeatureRDD(rdd, gRdd.sequences, None)
  }

  implicit def coverageToFeaturesDatasetConversionFn(
    gRdd: CoverageRDD,
    ds: Dataset[FeatureProduct]): FeatureRDD = {
    new DatasetBoundFeatureRDD(ds, gRdd.sequences)
  }

  implicit def coverageToFragmentsConversionFn(
    gRdd: CoverageRDD,
    rdd: RDD[Fragment]): FragmentRDD = {
    new RDDBoundFragmentRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def coverageToFragmentsDatasetConversionFn(
    gRdd: CoverageRDD,
    ds: Dataset[FragmentProduct]): FragmentRDD = {
    new DatasetBoundFragmentRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def coverageToAlignmentRecordsConversionFn(
    gRdd: CoverageRDD,
    rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    new RDDBoundAlignmentRecordRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def coverageToAlignmentRecordsDatasetConversionFn(
    gRdd: CoverageRDD,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    new DatasetBoundAlignmentRecordRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def coverageToGenotypesConversionFn(
    gRdd: CoverageRDD,
    rdd: RDD[Genotype]): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def coverageToGenotypesDatasetConversionFn(
    gRdd: CoverageRDD,
    ds: Dataset[GenotypeProduct]): GenotypeRDD = {
    new DatasetBoundGenotypeRDD(ds,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def coverageToVariantsConversionFn(
    gRdd: CoverageRDD,
    rdd: RDD[Variant]): VariantRDD = {
    new RDDBoundVariantRDD(rdd,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def coverageToVariantsDatasetConversionFn(
    gRdd: CoverageRDD,
    ds: Dataset[VariantProduct]): VariantRDD = {
    new DatasetBoundVariantRDD(ds,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def coverageToVariantContextConversionFn(
    gRdd: CoverageRDD,
    rdd: RDD[VariantContext]): VariantContextRDD = {
    VariantContextRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def featuresToContigsConversionFn(
    gRdd: FeatureRDD,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    new RDDBoundNucleotideContigFragmentRDD(rdd, gRdd.sequences, None)
  }

  implicit def featuresToContigsDatasetConversionFn(
    gRdd: FeatureRDD,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    new DatasetBoundNucleotideContigFragmentRDD(ds, gRdd.sequences)
  }

  implicit def featuresToCoverageConversionFn(
    gRdd: FeatureRDD,
    rdd: RDD[Coverage]): CoverageRDD = {
    new RDDBoundCoverageRDD(rdd, gRdd.sequences, None)
  }

  implicit def featuresToCoverageDatasetConversionFn(
    gRdd: FeatureRDD,
    ds: Dataset[Coverage]): CoverageRDD = {
    new DatasetBoundCoverageRDD(ds, gRdd.sequences)
  }

  implicit def featuresToFragmentsConversionFn(
    gRdd: FeatureRDD,
    rdd: RDD[Fragment]): FragmentRDD = {
    new RDDBoundFragmentRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def featuresToFragmentsDatasetConversionFn(
    gRdd: FeatureRDD,
    ds: Dataset[FragmentProduct]): FragmentRDD = {
    new DatasetBoundFragmentRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def featuresToAlignmentRecordsConversionFn(
    gRdd: FeatureRDD,
    rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    new RDDBoundAlignmentRecordRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def featuresToAlignmentRecordsDatasetConversionFn(
    gRdd: FeatureRDD,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    new DatasetBoundAlignmentRecordRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def featuresToGenotypesConversionFn(
    gRdd: FeatureRDD,
    rdd: RDD[Genotype]): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def featuresToGenotypesDatasetConversionFn(
    gRdd: FeatureRDD,
    ds: Dataset[GenotypeProduct]): GenotypeRDD = {
    new DatasetBoundGenotypeRDD(ds,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def featuresToVariantsConversionFn(
    gRdd: FeatureRDD,
    rdd: RDD[Variant]): VariantRDD = {
    new RDDBoundVariantRDD(rdd,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def featuresToVariantsDatasetConversionFn(
    gRdd: FeatureRDD,
    ds: Dataset[VariantProduct]): VariantRDD = {
    new DatasetBoundVariantRDD(ds,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def featuresToVariantContextConversionFn(
    gRdd: FeatureRDD,
    rdd: RDD[VariantContext]): VariantContextRDD = {
    VariantContextRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def fragmentsToContigsConversionFn(
    gRdd: FragmentRDD,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    new RDDBoundNucleotideContigFragmentRDD(rdd, gRdd.sequences, None)
  }

  implicit def fragmentsToContigsDatasetConversionFn(
    gRdd: FragmentRDD,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    new DatasetBoundNucleotideContigFragmentRDD(ds, gRdd.sequences)
  }

  implicit def fragmentsToCoverageConversionFn(
    gRdd: FragmentRDD,
    rdd: RDD[Coverage]): CoverageRDD = {
    new RDDBoundCoverageRDD(rdd, gRdd.sequences, None)
  }

  implicit def fragmentsToCoverageDatasetConversionFn(
    gRdd: FragmentRDD,
    ds: Dataset[Coverage]): CoverageRDD = {
    new DatasetBoundCoverageRDD(ds, gRdd.sequences)
  }

  implicit def fragmentsToFeaturesConversionFn(
    gRdd: FragmentRDD,
    rdd: RDD[Feature]): FeatureRDD = {
    new RDDBoundFeatureRDD(rdd, gRdd.sequences, None)
  }

  implicit def fragmentsToFeaturesDatasetConversionFn(
    gRdd: FragmentRDD,
    ds: Dataset[FeatureProduct]): FeatureRDD = {
    new DatasetBoundFeatureRDD(ds, gRdd.sequences)
  }

  implicit def fragmentsToAlignmentRecordsConversionFn(
    gRdd: FragmentRDD,
    rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    new RDDBoundAlignmentRecordRDD(rdd,
      gRdd.sequences,
      gRdd.recordGroups,
      gRdd.processingSteps,
      None)
  }

  implicit def fragmentsToAlignmentRecordsDatasetConversionFn(
    gRdd: FragmentRDD,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    new DatasetBoundAlignmentRecordRDD(ds,
      gRdd.sequences,
      gRdd.recordGroups,
      gRdd.processingSteps)
  }

  implicit def fragmentsToGenotypesConversionFn(
    gRdd: FragmentRDD,
    rdd: RDD[Genotype]): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd,
      gRdd.sequences,
      gRdd.recordGroups.toSamples,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def fragmentsToGenotypesDatasetConversionFn(
    gRdd: FragmentRDD,
    ds: Dataset[GenotypeProduct]): GenotypeRDD = {
    new DatasetBoundGenotypeRDD(ds,
      gRdd.sequences,
      gRdd.recordGroups.toSamples,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def fragmentsToVariantsConversionFn(
    gRdd: FragmentRDD,
    rdd: RDD[Variant]): VariantRDD = {
    new RDDBoundVariantRDD(rdd,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def fragmentsToVariantsDatasetConversionFn(
    gRdd: FragmentRDD,
    ds: Dataset[VariantProduct]): VariantRDD = {
    new DatasetBoundVariantRDD(ds,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def fragmentsToVariantContextConversionFn(
    gRdd: FragmentRDD,
    rdd: RDD[VariantContext]): VariantContextRDD = {
    VariantContextRDD(rdd,
      gRdd.sequences,
      gRdd.recordGroups.toSamples,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def genericToContigsConversionFn[Y <: GenericGenomicRDD[_]](
    gRdd: Y,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    new RDDBoundNucleotideContigFragmentRDD(rdd, gRdd.sequences, None)
  }

  implicit def genericToCoverageConversionFn[Y <: GenericGenomicRDD[_]](
    gRdd: Y,
    rdd: RDD[Coverage]): CoverageRDD = {
    new RDDBoundCoverageRDD(rdd, gRdd.sequences, None)
  }

  implicit def genericToFeatureConversionFn[Y <: GenericGenomicRDD[_]](
    gRdd: Y,
    rdd: RDD[Feature]): FeatureRDD = {
    new RDDBoundFeatureRDD(rdd, gRdd.sequences, None)
  }

  implicit def genericToFragmentsConversionFn[Y <: GenericGenomicRDD[_]](
    gRdd: Y,
    rdd: RDD[Fragment]): FragmentRDD = {
    new RDDBoundFragmentRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def genericToAlignmentRecordsConversionFn[Y <: GenericGenomicRDD[_]](
    gRdd: Y,
    rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    new RDDBoundAlignmentRecordRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def genericToGenotypesConversionFn[Y <: GenericGenomicRDD[_]](
    gRdd: Y,
    rdd: RDD[Genotype]): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def genericToVariantsConversionFn[Y <: GenericGenomicRDD[_]](
    gRdd: Y,
    rdd: RDD[Variant]): VariantRDD = {
    new RDDBoundVariantRDD(rdd,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def genericToVariantContextsConversionFn[Y <: GenericGenomicRDD[_]](
    gRdd: Y,
    rdd: RDD[VariantContext]): VariantContextRDD = {
    new VariantContextRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def alignmentRecordsToContigsConversionFn(
    gRdd: AlignmentRecordRDD,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    new RDDBoundNucleotideContigFragmentRDD(rdd, gRdd.sequences, None)
  }

  implicit def alignmentRecordsToContigsDatasetConversionFn(
    gRdd: AlignmentRecordRDD,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    new DatasetBoundNucleotideContigFragmentRDD(ds, gRdd.sequences)
  }

  implicit def alignmentRecordsToCoverageConversionFn(
    gRdd: AlignmentRecordRDD,
    rdd: RDD[Coverage]): CoverageRDD = {
    new RDDBoundCoverageRDD(rdd, gRdd.sequences, None)
  }

  implicit def alignmentRecordsToCoverageDatasetConversionFn(
    gRdd: AlignmentRecordRDD,
    ds: Dataset[Coverage]): CoverageRDD = {
    new DatasetBoundCoverageRDD(ds, gRdd.sequences)
  }

  implicit def alignmentRecordsToFeaturesConversionFn(
    gRdd: AlignmentRecordRDD,
    rdd: RDD[Feature]): FeatureRDD = {
    new RDDBoundFeatureRDD(rdd, gRdd.sequences, None)
  }

  implicit def alignmentRecordsToFeaturesDatasetConversionFn(
    gRdd: AlignmentRecordRDD,
    ds: Dataset[FeatureProduct]): FeatureRDD = {
    new DatasetBoundFeatureRDD(ds, gRdd.sequences)
  }

  implicit def alignmentRecordsToFragmentsConversionFn(
    gRdd: AlignmentRecordRDD,
    rdd: RDD[Fragment]): FragmentRDD = {
    new RDDBoundFragmentRDD(rdd,
      gRdd.sequences,
      gRdd.recordGroups,
      gRdd.processingSteps,
      None)
  }

  implicit def alignmentRecordsToFragmentsDatasetConversionFn(
    gRdd: AlignmentRecordRDD,
    ds: Dataset[FragmentProduct]): FragmentRDD = {
    new DatasetBoundFragmentRDD(ds,
      gRdd.sequences,
      gRdd.recordGroups,
      gRdd.processingSteps)
  }

  implicit def alignmentRecordsToGenotypesConversionFn(
    gRdd: AlignmentRecordRDD,
    rdd: RDD[Genotype]): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd,
      gRdd.sequences,
      gRdd.recordGroups.toSamples,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def alignmentRecordsToGenotypesDatasetConversionFn(
    gRdd: AlignmentRecordRDD,
    ds: Dataset[GenotypeProduct]): GenotypeRDD = {
    new DatasetBoundGenotypeRDD(ds,
      gRdd.sequences,
      gRdd.recordGroups.toSamples,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def alignmentRecordsToVariantsConversionFn(
    gRdd: AlignmentRecordRDD,
    rdd: RDD[Variant]): VariantRDD = {
    new RDDBoundVariantRDD(rdd,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def alignmentRecordsToVariantsDatasetConversionFn(
    gRdd: AlignmentRecordRDD,
    ds: Dataset[VariantProduct]): VariantRDD = {
    new DatasetBoundVariantRDD(ds,
      gRdd.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def alignmentRecordsToVariantContextConversionFn(
    gRdd: AlignmentRecordRDD,
    rdd: RDD[VariantContext]): VariantContextRDD = {
    VariantContextRDD(rdd,
      gRdd.sequences,
      gRdd.recordGroups.toSamples,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def genotypesToContigsConversionFn(
    gRdd: GenotypeRDD,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    new RDDBoundNucleotideContigFragmentRDD(rdd, gRdd.sequences, None)
  }

  implicit def genotypesToContigsDatasetConversionFn(
    gRdd: GenotypeRDD,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    new DatasetBoundNucleotideContigFragmentRDD(ds, gRdd.sequences)
  }

  implicit def genotypesToCoverageConversionFn(
    gRdd: GenotypeRDD,
    rdd: RDD[Coverage]): CoverageRDD = {
    new RDDBoundCoverageRDD(rdd, gRdd.sequences, None)
  }

  implicit def genotypesToCoverageDatasetConversionFn(
    gRdd: GenotypeRDD,
    ds: Dataset[Coverage]): CoverageRDD = {
    new DatasetBoundCoverageRDD(ds, gRdd.sequences)
  }

  implicit def genotypesToFeaturesConversionFn(
    gRdd: GenotypeRDD,
    rdd: RDD[Feature]): FeatureRDD = {
    new RDDBoundFeatureRDD(rdd, gRdd.sequences, None)
  }

  implicit def genotypesToFeaturesDatasetConversionFn(
    gRdd: GenotypeRDD,
    ds: Dataset[FeatureProduct]): FeatureRDD = {
    new DatasetBoundFeatureRDD(ds, gRdd.sequences)
  }

  implicit def genotypesToFragmentsConversionFn(
    gRdd: GenotypeRDD,
    rdd: RDD[Fragment]): FragmentRDD = {
    new RDDBoundFragmentRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def genotypesToFragmentsDatasetConversionFn(
    gRdd: GenotypeRDD,
    ds: Dataset[FragmentProduct]): FragmentRDD = {
    new DatasetBoundFragmentRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def genotypesToAlignmentRecordsConversionFn(
    gRdd: GenotypeRDD,
    rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    new RDDBoundAlignmentRecordRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def genotypesToAlignmentRecordsDatasetConversionFn(
    gRdd: GenotypeRDD,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    new DatasetBoundAlignmentRecordRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def genotypesToVariantsConversionFn(
    gRdd: GenotypeRDD,
    rdd: RDD[Variant]): VariantRDD = {
    new RDDBoundVariantRDD(rdd,
      gRdd.sequences,
      gRdd.headerLines,
      None)
  }

  implicit def genotypesToVariantsDatasetConversionFn(
    gRdd: GenotypeRDD,
    ds: Dataset[VariantProduct]): VariantRDD = {
    new DatasetBoundVariantRDD(ds,
      gRdd.sequences,
      gRdd.headerLines)
  }

  implicit def genotypesToVariantContextConversionFn(
    gRdd: GenotypeRDD,
    rdd: RDD[VariantContext]): VariantContextRDD = {
    VariantContextRDD(rdd,
      gRdd.sequences,
      gRdd.samples,
      gRdd.headerLines)
  }

  implicit def variantsToContigsConversionFn(
    gRdd: VariantRDD,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    new RDDBoundNucleotideContigFragmentRDD(rdd, gRdd.sequences, None)
  }

  implicit def variantsToContigsDatasetConversionFn(
    gRdd: VariantRDD,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentRDD = {
    new DatasetBoundNucleotideContigFragmentRDD(ds, gRdd.sequences)
  }

  implicit def variantsToCoverageConversionFn(
    gRdd: VariantRDD,
    rdd: RDD[Coverage]): CoverageRDD = {
    new RDDBoundCoverageRDD(rdd, gRdd.sequences, None)
  }

  implicit def variantsToCoverageDatasetConversionFn(
    gRdd: VariantRDD,
    ds: Dataset[Coverage]): CoverageRDD = {
    new DatasetBoundCoverageRDD(ds, gRdd.sequences)
  }

  implicit def variantsToFeaturesConversionFn(
    gRdd: VariantRDD,
    rdd: RDD[Feature]): FeatureRDD = {
    new RDDBoundFeatureRDD(rdd, gRdd.sequences, None)
  }

  implicit def variantsToFeaturesDatasetConversionFn(
    gRdd: VariantRDD,
    ds: Dataset[FeatureProduct]): FeatureRDD = {
    new DatasetBoundFeatureRDD(ds, gRdd.sequences)
  }

  implicit def variantsToFragmentsConversionFn(
    gRdd: VariantRDD,
    rdd: RDD[Fragment]): FragmentRDD = {
    new RDDBoundFragmentRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def variantsToFragmentsDatasetConversionFn(
    gRdd: VariantRDD,
    ds: Dataset[FragmentProduct]): FragmentRDD = {
    new DatasetBoundFragmentRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def variantsToAlignmentRecordsConversionFn(
    gRdd: VariantRDD,
    rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    new RDDBoundAlignmentRecordRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def variantsToAlignmentRecordsDatasetConversionFn(
    gRdd: VariantRDD,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordRDD = {
    new DatasetBoundAlignmentRecordRDD(ds,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  implicit def variantsToGenotypesConversionFn(
    gRdd: VariantRDD,
    rdd: RDD[Genotype]): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      gRdd.headerLines,
      None)
  }

  implicit def variantsToGenotypesDatasetConversionFn(
    gRdd: VariantRDD,
    ds: Dataset[GenotypeProduct]): GenotypeRDD = {
    new DatasetBoundGenotypeRDD(ds,
      gRdd.sequences,
      Seq.empty,
      gRdd.headerLines)
  }

  implicit def variantsToVariantContextConversionFn(
    gRdd: VariantRDD,
    rdd: RDD[VariantContext]): VariantContextRDD = {
    VariantContextRDD(rdd,
      gRdd.sequences,
      Seq.empty,
      gRdd.headerLines)
  }

  implicit def variantContextsToContigsConversionFn(
    gRdd: VariantContextRDD,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentRDD = {
    new RDDBoundNucleotideContigFragmentRDD(rdd, gRdd.sequences, None)
  }

  implicit def variantContextsToCoverageConversionFn(
    gRdd: VariantContextRDD,
    rdd: RDD[Coverage]): CoverageRDD = {
    new RDDBoundCoverageRDD(rdd, gRdd.sequences, None)
  }

  implicit def variantContextsToFeaturesConversionFn(
    gRdd: VariantContextRDD,
    rdd: RDD[Feature]): FeatureRDD = {
    new RDDBoundFeatureRDD(rdd, gRdd.sequences, None)
  }

  implicit def variantContextsToFragmentsConversionFn(
    gRdd: VariantContextRDD,
    rdd: RDD[Fragment]): FragmentRDD = {
    new RDDBoundFragmentRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def variantContextsToAlignmentRecordsConversionFn(
    gRdd: VariantContextRDD,
    rdd: RDD[AlignmentRecord]): AlignmentRecordRDD = {
    new RDDBoundAlignmentRecordRDD(rdd,
      gRdd.sequences,
      RecordGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def variantContextsToGenotypesConversionFn(
    gRdd: VariantContextRDD,
    rdd: RDD[Genotype]): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd,
      gRdd.sequences,
      gRdd.samples,
      gRdd.headerLines,
      None)
  }

  implicit def variantContextsToVariantsConversionFn(
    gRdd: VariantContextRDD,
    rdd: RDD[Variant]): VariantRDD = {
    new RDDBoundVariantRDD(rdd,
      gRdd.sequences,
      gRdd.headerLines,
      None)
  }

  // Add ADAM Spark context methods
  implicit def sparkContextToADAMContext(sc: SparkContext): ADAMContext = new ADAMContext(sc)

  // Add generic RDD methods for all types of ADAM RDDs
  implicit def rddToADAMRDD[T](rdd: RDD[T])(implicit ev1: T => IndexedRecord, ev2: Manifest[T]): ConcreteADAMRDDFunctions[T] = new ConcreteADAMRDDFunctions(rdd)

  // Add implicits for the rich adam objects
  implicit def recordToRichRecord(record: AlignmentRecord): RichAlignmentRecord = new RichAlignmentRecord(record)

  /**
   * Builds a program description from a htsjdk program record.
   *
   * @param record Program record to convert.
   * @return Returns an Avro formatted program record.
   */
  private[adam] def convertSAMProgramRecord(
    record: SAMProgramRecord): ProcessingStep = {
    val builder = ProcessingStep.newBuilder
      .setId(record.getId)
    Option(record.getPreviousProgramGroupId).foreach(builder.setPreviousId(_))
    Option(record.getProgramVersion).foreach(builder.setVersion(_))
    Option(record.getProgramName).foreach(builder.setProgramName(_))
    Option(record.getCommandLine).foreach(builder.setCommandLine(_))
    builder.build
  }
}

/**
 * A filter to run on globs/directories that finds all files with a given name.
 *
 * @param name The name to search for.
 */
private class FileFilter(private val name: String) extends PathFilter {

  /**
   * @param path Path to evaluate.
   * @return Returns true if the pathName of the path matches the name passed
   *   to the constructor.
   */
  def accept(path: Path): Boolean = {
    path.getName == name
  }
}

/**
 * A filter to run on globs/directories that finds all files that do not start
 * with a given string.
 *
 * @param prefix The prefix to search for. Files that contain this prefix are
 *   discarded.
 */
private class NoPrefixFileFilter(private val prefix: String) extends PathFilter {

  /**
   * @param path Path to evaluate.
   * @return Returns true if the pathName of the path does not match the prefix passed
   *   to the constructor.
   */
  def accept(path: Path): Boolean = {
    !path.getName.startsWith(prefix)
  }
}

/**
 * The ADAMContext provides functions on top of a SparkContext for loading genomic data.
 *
 * @param sc The SparkContext to wrap.
 */
class ADAMContext(@transient val sc: SparkContext) extends Serializable with Logging {

  /**
   * @param samHeader The header to extract a sequence dictionary from.
   * @return Returns the dictionary converted to an ADAM model.
   */
  private[rdd] def loadBamDictionary(samHeader: SAMFileHeader): SequenceDictionary = {
    SequenceDictionary(samHeader)
  }

  /**
   * @param samHeader The header to extract a read group dictionary from.
   * @return Returns the dictionary converted to an ADAM model.
   */
  private[rdd] def loadBamReadGroups(samHeader: SAMFileHeader): RecordGroupDictionary = {
    RecordGroupDictionary.fromSAMHeader(samHeader)
  }

  /**
   * @param samHeader The header to extract processing lineage from.
   * @return Returns the dictionary converted to an Avro model.
   */
  private[rdd] def loadBamPrograms(
    samHeader: SAMFileHeader): Seq[ProcessingStep] = {
    val pgs = samHeader.getProgramRecords().toSeq
    pgs.map(ADAMContext.convertSAMProgramRecord)
  }

  /**
   * @param pathName The path name to load VCF format metadata from.
   *   Globs/directories are supported.
   * @return Returns a tuple of metadata from the VCF header, including the
   *   sequence dictionary and a list of the samples contained in the VCF.
   */
  private[rdd] def loadVcfMetadata(pathName: String): (SequenceDictionary, Seq[Sample], Seq[VCFHeaderLine]) = {
    // get the paths to all vcfs
    val files = getFsAndFilesWithFilter(pathName, new NoPrefixFileFilter("_"))

    // load yonder the metadata
    files.map(p => loadSingleVcfMetadata(p.toString)).reduce((p1, p2) => {
      (p1._1 ++ p2._1, p1._2 ++ p2._2, p1._3 ++ p2._3)
    })
  }

  /**
   * @param pathName The path name to load VCF format metadata from.
   *   Globs/directories are not supported.
   * @return Returns a tuple of metadata from the VCF header, including the
   *   sequence dictionary and a list of the samples contained in the VCF.
   *
   * @see loadVcfMetadata
   */
  private def loadSingleVcfMetadata(pathName: String): (SequenceDictionary, Seq[Sample], Seq[VCFHeaderLine]) = {
    def headerToMetadata(vcfHeader: VCFHeader): (SequenceDictionary, Seq[Sample], Seq[VCFHeaderLine]) = {
      val sd = SequenceDictionary.fromVCFHeader(vcfHeader)
      val samples = asScalaBuffer(vcfHeader.getGenotypeSamples)
        .map(s => {
          Sample.newBuilder()
            .setSampleId(s)
            .build()
        }).toSeq
      (sd, samples, VariantContextConverter.headerLines(vcfHeader))
    }

    headerToMetadata(readVcfHeader(pathName))
  }

  private def readVcfHeader(pathName: String): VCFHeader = {
    VCFHeaderReader.readHeaderFrom(WrapSeekable.openPath(sc.hadoopConfiguration,
      new Path(pathName)))
  }

  private def loadHeaderLines(pathName: String): Seq[VCFHeaderLine] = {
    getFsAndFilesWithFilter(pathName, new FileFilter("_header"))
      .map(p => VariantContextConverter.headerLines(readVcfHeader(p.toString)))
      .flatten
      .distinct
  }

  /**
   * @param pathName The path name to load Avro processing steps from.
   *   Globs/directories are supported.
   * @return Returns a seq of processing steps.
   */
  private[rdd] def loadAvroPrograms(pathName: String): Seq[ProcessingStep] = {
    getFsAndFilesWithFilter(pathName, new FileFilter("_processing.avro"))
      .map(p => {
        loadAvro[ProcessingStep](p.toString, ProcessingStep.SCHEMA$)
      }).reduce(_ ++ _)
  }

  /**
   * @param pathName The path name to load Avro sequence dictionaries from.
   *   Globs/directories are supported.
   * @return Returns a SequenceDictionary.
   */
  private[rdd] def loadAvroSequenceDictionary(pathName: String): SequenceDictionary = {
    getFsAndFilesWithFilter(pathName, new FileFilter("_seqdict.avro"))
      .map(p => loadSingleAvroSequenceDictionary(p.toString))
      .reduce(_ ++ _)
  }

  /**
   * @see loadAvroSequenceDictionary
   *
   * @param pathName The path name to load a single Avro sequence dictionary from.
   *   Globs/directories are not supported.
   * @return Returns a SequenceDictionary.
   */
  private def loadSingleAvroSequenceDictionary(pathName: String): SequenceDictionary = {
    val avroSd = loadAvro[Contig](pathName, Contig.SCHEMA$)
    SequenceDictionary.fromAvro(avroSd)
  }

  /**
   * @param pathName The path name to load Avro samples from.
   *   Globs/directories are supported.
   * @return Returns a Seq of Samples.
   */
  private[rdd] def loadAvroSamples(pathName: String): Seq[Sample] = {
    getFsAndFilesWithFilter(pathName, new FileFilter("_samples.avro"))
      .map(p => loadAvro[Sample](p.toString, Sample.SCHEMA$))
      .reduce(_ ++ _)
  }

  /**
   * @param pathName The path name to load Avro record group dictionaries from.
   *   Globs/directories are supported.
   * @return Returns a RecordGroupDictionary.
   */
  private[rdd] def loadAvroRecordGroupDictionary(pathName: String): RecordGroupDictionary = {
    getFsAndFilesWithFilter(pathName, new FileFilter("_rgdict.avro"))
      .map(p => loadSingleAvroRecordGroupDictionary(p.toString))
      .reduce(_ ++ _)
  }

  /**
   * @see loadAvroRecordGroupDictionary
   *
   * @param pathName The path name to load a single Avro record group dictionary from.
   *   Globs/directories are not supported.
   * @return Returns a RecordGroupDictionary.
   */
  private def loadSingleAvroRecordGroupDictionary(pathName: String): RecordGroupDictionary = {
    val avroRgd = loadAvro[RecordGroupMetadata](pathName,
      RecordGroupMetadata.SCHEMA$)

    // convert avro to record group dictionary
    new RecordGroupDictionary(avroRgd.map(RecordGroup.fromAvro))
  }

  /**
   * Load a path name in Parquet + Avro format into an RDD.
   *
   * @param pathName The path name to load Parquet + Avro formatted data from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @tparam T The type of records to return.
   * @return An RDD with records of the specified type.
   */
  def loadParquet[T](
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None)(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): RDD[T] = {

    //make sure a type was specified
    //not using require as to make the message clearer
    if (manifest[T] == manifest[scala.Nothing])
      throw new IllegalArgumentException("Type inference failed; when loading please specify a specific type. " +
        "e.g.:\nval reads: RDD[AlignmentRecord] = ...\nbut not\nval reads = ...\nwithout a return type")

    log.info("Reading the ADAM file at %s to create RDD".format(pathName))
    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])
    AvroParquetInputFormat.setAvroReadSchema(job,
      manifest[T].runtimeClass.newInstance().asInstanceOf[T].getSchema)

    optPredicate.foreach { (pred) =>
      log.info("Using the specified push-down predicate")
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, pred)
    }

    if (optProjection.isDefined) {
      log.info("Using the specified projection schema")
      AvroParquetInputFormat.setRequestedProjection(job, optProjection.get)
    }

    val records = sc.newAPIHadoopFile(
      pathName,
      classOf[ADAMParquetInputFormat[T]],
      classOf[Void],
      manifest[T].runtimeClass.asInstanceOf[Class[T]],
      ContextUtil.getConfiguration(job)
    )

    val instrumented = if (Metrics.isRecording) records.instrument() else records
    val mapped = instrumented.map(p => p._2)

    if (optPredicate.isDefined) {
      // Strip the nulls that the predicate returns
      mapped.filter(p => p != null.asInstanceOf[T])
    } else {
      mapped
    }
  }

  /**
   * Elaborates out a directory/glob/plain path.
   *
   * @see getFsAndFiles
   *
   * @param path Path to elaborate.
   * @param fs The underlying file system that this path is on.
   * @return Returns an array of Paths to load.
   * @throws FileNotFoundException if the path does not match any files.
   */
  protected def getFiles(path: Path, fs: FileSystem): Array[Path] = {

    // elaborate out the path; this returns FileStatuses
    val paths = if (fs.isDirectory(path)) fs.listStatus(path) else fs.globStatus(path)

    // the path must match at least one file
    if (paths == null || paths.isEmpty) {
      throw new FileNotFoundException(
        s"Couldn't find any files matching ${path.toUri}. If you are trying to" +
          " glob a directory of Parquet files, you need to glob inside the" +
          " directory as well (e.g., \"glob.me.*.adam/*\", instead of" +
          " \"glob.me.*.adam\"."
      )
    }

    // map the paths returned to their paths
    paths.map(_.getPath)
  }

  /**
   * Elaborates out a directory/glob/plain path.
   *
   * @see getFiles
   *
   * @param path Path to elaborate.
   * @return Returns an array of Paths to load.
   * @throws FileNotFoundException if the path does not match any files.
   */
  protected def getFsAndFiles(path: Path): Array[Path] = {

    // get the underlying fs for the file
    val fs = Option(path.getFileSystem(sc.hadoopConfiguration)).getOrElse(
      throw new FileNotFoundException(
        s"Couldn't find filesystem for ${path.toUri} with Hadoop configuration ${sc.hadoopConfiguration}"
      ))

    getFiles(path, fs)
  }

  /**
   * Elaborates out a directory/glob/plain path name.
   *
   * @see getFiles
   *
   * @param pathName Path name to elaborate.
   * @param filter Filter to discard paths.
   * @return Returns an array of Paths to load.
   * @throws FileNotFoundException if the path does not match any files.
   */
  protected def getFsAndFilesWithFilter(pathName: String, filter: PathFilter): Array[Path] = {

    val path = new Path(pathName)

    // get the underlying fs for the file
    val fs = Option(path.getFileSystem(sc.hadoopConfiguration)).getOrElse(
      throw new FileNotFoundException(
        s"Couldn't find filesystem for ${path.toUri} with Hadoop configuration ${sc.hadoopConfiguration}"
      ))

    // elaborate out the path; this returns FileStatuses
    val paths = if (fs.isDirectory(path)) {
      val paths = fs.listStatus(path)

      if (paths == null || paths.isEmpty) {
        s"Couldn't find any files matching ${path.toUri}"
      }
      fs.listStatus(path, filter)
    } else {
      val paths = fs.globStatus(path)
      if (paths == null || paths.isEmpty) {
        throw new FileNotFoundException(
          s"Couldn't find any files matching ${path.toUri}"
        )
      }
      fs.globStatus(path, filter)
    }

    // the path must match PathFilter
    if (paths == null || paths.isEmpty) {
      throw new FileNotFoundException(
        s"Couldn't find any files matching ${path.toUri} for the requested PathFilter"
      )
    }

    // map the paths returned to their paths
    paths.map(_.getPath)
  }

  /**
   * Checks to see if a set of BAM/CRAM/SAM files are queryname grouped.
   *
   * If we are loading fragments and the BAM/CRAM/SAM files are sorted by the
   * read names, or the file is unsorted but is query grouped, this implies
   * that all of the reads in a pair are consecutive in
   * the file. If this is the case, we can configure Hadoop-BAM to keep all of
   * the reads from a fragment in a single split. This allows us to eliminate
   * an expensive groupBy when loading a BAM file as fragments.
   *
   * @param pathName The path name to load BAM/CRAM/SAM formatted alignment records from.
   *   Globs/directories are supported.
   * @param stringency The validation stringency to use when validating the
   *   BAM/CRAM/SAM format header. Defaults to ValidationStringency.STRICT.
   * @return Returns true if all files described by the path name are queryname
   *   sorted.
   */
  private[rdd] def filesAreQueryGrouped(
    pathName: String,
    stringency: ValidationStringency = ValidationStringency.STRICT): Boolean = {

    val path = new Path(pathName)
    val bamFiles = getFsAndFiles(path)
    val filteredFiles = bamFiles.filter(p => {
      val pPath = p.getName()
      isBamExt(pPath) || pPath.startsWith("part-")
    })

    filteredFiles
      .forall(fp => {
        try {
          // the sort order is saved in the file header
          sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, stringency.toString)
          val samHeader = SAMHeaderReader.readSAMHeaderFrom(fp, sc.hadoopConfiguration)

          (samHeader.getSortOrder == SAMFileHeader.SortOrder.queryname ||
            samHeader.getGroupOrder == SAMFileHeader.GroupOrder.query)
        } catch {
          case e: Throwable => {
            log.error(
              s"Loading header failed for $fp:n${e.getMessage}\n\t${e.getStackTrace.take(25).map(_.toString).mkString("\n\t")}"
            )
            false
          }
        }
      })
  }

  /**
   * Trim the default compression extension from the specified path name, if it is
   * recognized as compressed by the compression codecs in the Hadoop configuration.
   *
   * @param pathName The path name to trim.
   * @return The path name with the default compression extension trimmed.
   */
  private[rdd] def trimExtensionIfCompressed(pathName: String): String = {
    val codecFactory = new CompressionCodecFactory(sc.hadoopConfiguration)
    val path = new Path(pathName)
    val codec = codecFactory.getCodec(path)
    if (codec == null) {
      pathName
    } else {
      log.info(s"Found compression codec $codec for $pathName in Hadoop configuration.")
      val extension = codec.getDefaultExtension()
      CompressionCodecFactory.removeSuffix(pathName, extension)
    }
  }

  /**
   * Load alignment records from BAM/CRAM/SAM into an AlignmentRecordRDD.
   *
   * This reads the sequence and record group dictionaries from the BAM/CRAM/SAM file
   * header. SAMRecords are read from the file and converted to the
   * AlignmentRecord schema.
   *
   * @param pathName The path name to load BAM/CRAM/SAM formatted alignment records from.
   *   Globs/directories are supported.
   * @param stringency The validation stringency to use when validating the
   *   BAM/CRAM/SAM format header. Defaults to ValidationStringency.STRICT.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of alignment records,
   *   sequence dictionary representing contigs the alignment records may be aligned to,
   *   and the record group dictionary for the alignment records if one is available.
   */
  def loadBam(
    pathName: String,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = LoadBam.time {

    val path = new Path(pathName)
    val bamFiles = getFsAndFiles(path)
    val filteredFiles = bamFiles.filter(p => {
      val pPath = p.getName()
      isBamExt(pPath) || pPath.startsWith("part-")
    })

    require(filteredFiles.nonEmpty,
      "Did not find any BAM files at %s.".format(path))

    val (seqDict, readGroups, programs) =
      filteredFiles
        .flatMap(fp => {
          try {
            // We need to separately read the header, so that we can inject the sequence dictionary
            // data into each individual Read (see the argument to samRecordConverter.convert,
            // below).
            sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, stringency.toString)
            val samHeader = SAMHeaderReader.readSAMHeaderFrom(fp, sc.hadoopConfiguration)
            log.info("Loaded header from " + fp)
            val sd = loadBamDictionary(samHeader)
            val rg = loadBamReadGroups(samHeader)
            val pgs = loadBamPrograms(samHeader)
            Some((sd, rg, pgs))
          } catch {
            case e: Throwable => {
              if (stringency == ValidationStringency.STRICT) {
                throw e
              } else if (stringency == ValidationStringency.LENIENT) {
                log.error(
                  s"Loading failed for $fp:\n${e.getMessage}\n\t${e.getStackTrace.take(25).map(_.toString).mkString("\n\t")}"
                )
              }
              None
            }
          }
        }).reduce((kv1, kv2) => {
          (kv1._1 ++ kv2._1, kv1._2 ++ kv2._2, kv1._3 ++ kv2._3)
        })

    val job = HadoopUtil.newJob(sc)

    // this logic is counterintuitive but important.
    // hadoop-bam does not filter out .bai files, etc. as such, if we have a
    // directory of bam files where all the bams also have bais or md5s etc
    // in the same directory, hadoop-bam will barf. if the directory just
    // contains bams, hadoop-bam is a-ok! i believe that it is better (perf) to
    // just load from a single newAPIHadoopFile call instead of a union across
    // files, so we do that whenever possible
    val records = if (filteredFiles.length != bamFiles.length) {
      sc.union(filteredFiles.map(p => {
        sc.newAPIHadoopFile(p.toString, classOf[AnySAMInputFormat], classOf[LongWritable],
          classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
      }))
    } else {
      sc.newAPIHadoopFile(pathName, classOf[AnySAMInputFormat], classOf[LongWritable],
        classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
    }
    if (Metrics.isRecording) records.instrument() else records
    val samRecordConverter = new SAMRecordConverter

    AlignmentRecordRDD(records.map(p => samRecordConverter.convert(p._2.get)),
      seqDict,
      readGroups,
      programs)
  }

  /**
   * Functions like loadBam, but uses BAM index files to look at fewer blocks,
   * and only returns records within a specified ReferenceRegion. BAM index file required.
   *
   * @param pathName The path name to load indexed BAM formatted alignment records from.
   *   Globs/directories are supported.
   * @param viewRegion The ReferenceRegion we are filtering on.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of alignment records,
   *   sequence dictionary representing contigs the alignment records may be aligned to,
   *   and the record group dictionary for the alignment records if one is available.
   */
  // todo: add stringency with default if possible
  def loadIndexedBam(
    pathName: String,
    viewRegion: ReferenceRegion): AlignmentRecordRDD = {
    loadIndexedBam(pathName, Iterable(viewRegion))
  }

  /**
   * Functions like loadBam, but uses BAM index files to look at fewer blocks,
   * and only returns records within the specified ReferenceRegions. BAM index file required.
   *
   * @param pathName The path name to load indexed BAM formatted alignment records from.
   *   Globs/directories are supported.
   * @param viewRegions Iterable of ReferenceRegion we are filtering on.
   * @param stringency The validation stringency to use when validating the
   *   BAM/CRAM/SAM format header. Defaults to ValidationStringency.STRICT.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of alignment records,
   *   sequence dictionary representing contigs the alignment records may be aligned to,
   *   and the record group dictionary for the alignment records if one is available.
   */
  def loadIndexedBam(
    pathName: String,
    viewRegions: Iterable[ReferenceRegion],
    stringency: ValidationStringency = ValidationStringency.STRICT)(implicit s: DummyImplicit): AlignmentRecordRDD = LoadIndexedBam.time {

    val path = new Path(pathName)
    // todo: can this method handle SAM and CRAM, or just BAM?
    val bamFiles = getFsAndFiles(path).filter(p => p.toString.endsWith(".bam"))

    require(bamFiles.nonEmpty,
      "Did not find any BAM files at %s.".format(path))
    val (seqDict, readGroups, programs) = bamFiles
      .flatMap(fp => {
        try {
          // We need to separately read the header, so that we can inject the sequence dictionary
          // data into each individual Read (see the argument to samRecordConverter.convert,
          // below).
          sc.hadoopConfiguration.set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, stringency.toString)
          val samHeader = SAMHeaderReader.readSAMHeaderFrom(fp, sc.hadoopConfiguration)

          log.info("Loaded header from " + fp)
          val sd = loadBamDictionary(samHeader)
          val rg = loadBamReadGroups(samHeader)
          val pgs = loadBamPrograms(samHeader)

          Some((sd, rg, pgs))
        } catch {
          case e: Throwable => {
            if (stringency == ValidationStringency.STRICT) {
              throw e
            } else if (stringency == ValidationStringency.LENIENT) {
              log.error(
                s"Loading failed for $fp:\n${e.getMessage}\n\t${e.getStackTrace.take(25).map(_.toString).mkString("\n\t")}"
              )
            }
            None
          }
        }
      }).reduce((kv1, kv2) => {
        (kv1._1 ++ kv2._1, kv1._2 ++ kv2._2, kv1._3 ++ kv2._3)
      })

    val job = HadoopUtil.newJob(sc)
    val conf = ContextUtil.getConfiguration(job)
    BAMInputFormat.setIntervals(conf, viewRegions.toList.map(r => LocatableReferenceRegion(r)))

    val records = sc.newAPIHadoopFile(pathName,
      classOf[BAMInputFormat],
      classOf[LongWritable],
      classOf[SAMRecordWritable],
      conf)

    if (Metrics.isRecording) records.instrument() else records
    val samRecordConverter = new SAMRecordConverter
    AlignmentRecordRDD(records.map(p => samRecordConverter.convert(p._2.get)),
      seqDict,
      readGroups,
      programs)
  }

  /**
   * Load Avro data from a Hadoop File System.
   *
   * This method uses the SparkContext wrapped by this class to identify our
   * underlying file system. We then use the underlying FileSystem imp'l to
   * open the Avro file, and we read the Avro files into a Seq.
   *
   * Frustratingly enough, although all records generated by the Avro IDL
   * compiler have a static SCHEMA$ field, this field does not belong to
   * the SpecificRecordBase abstract class, or the SpecificRecord interface.
   * As such, we must force the user to pass in the schema.
   *
   * @tparam T The type of the specific record we are loading.
   * @param pathName The path name to load Avro records from.
   *   Globs/directories are supported.
   * @param schema Schema of records we are loading.
   * @return Returns a Seq containing the Avro records.
   */
  private def loadAvro[T <: SpecificRecordBase](
    pathName: String,
    schema: Schema)(implicit tTag: ClassTag[T]): Seq[T] = {

    // get our current file system
    val path = new Path(pathName)
    val fs = path.getFileSystem(sc.hadoopConfiguration)

    // get an input stream
    val is = fs.open(path)
      .asInstanceOf[InputStream]

    // set up avro for reading
    val dr = new SpecificDatumReader[T](schema)
    val fr = new DataFileStream[T](is, dr)

    // get iterator and create an empty list
    val iter = fr.iterator
    var list = List.empty[T]

    // !!!!!
    // important implementation note:
    // !!!!!
    //
    // in theory, we should be able to call iter.toSeq to get a Seq of the
    // specific records we are reading. this would allow us to avoid needing
    // to manually pop things into a list.
    //
    // however! this causes odd problems that seem to be related to some sort of
    // lazy execution inside of scala. specifically, if you go
    // iter.toSeq.map(fn) in scala, this seems to be compiled into a lazy data
    // structure where the map call is only executed when the Seq itself is
    // actually accessed (e.g., via seq.apply(i), seq.head, etc.). typically,
    // this would be OK, but if the Seq[T] goes into a spark closure, the closure
    // cleaner will fail with a NotSerializableException, since SpecificRecord's
    // are not java serializable. specifically, we see this happen when using
    // this function to load RecordGroupMetadata when creating a
    // RecordGroupDictionary.
    //
    // good news is, you can work around this by explicitly walking the iterator
    // and building a collection, which is what we do here. this would not be
    // efficient if we were loading a large amount of avro data (since we're
    // loading all the data into memory), but currently, we are just using this
    // code for building sequence/record group dictionaries, which are fairly
    // small (seq dict is O(30) entries, rgd is O(20n) entries, where n is the
    // number of samples).
    while (iter.hasNext) {
      list = iter.next :: list
    }

    // close file
    fr.close()
    is.close()

    // reverse list and return as seq
    list.reverse
      .toSeq
  }

  /**
   * Gets the sort and partition map metadata from the header of the file given
   * as input.
   *
   * @param filename the filename for the metadata
   * @return a partition map if the data was written sorted, or an empty Seq if unsorted
   */
  private[rdd] def extractPartitionMap(
    filename: String): Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = {

    val path = new Path(filename + "/_partitionMap.avro")
    val fs = path.getFileSystem(sc.hadoopConfiguration)

    try {
      // get an input stream
      val is = fs.open(path)

      // set up avro for reading
      val dr = new GenericDatumReader[GenericRecord]
      val fr = new DataFileStream[GenericRecord](is, dr)

      // parsing the json from the metadata header
      // this unfortunately seems to be the only way to do this
      // avro does not seem to support getting metadata fields out once
      // you have the input from the string
      val metaDataMap = JSON.parseFull(fr.getMetaString("avro.schema"))
        // the cast here is required because parsefull does not cast for
        // us. parsefull returns an object of type Any and leaves it to 
        // the user to cast.
        .get.asInstanceOf[Map[String, String]]

      val optPartitionMap = metaDataMap.get("partitionMap")
      // we didn't write a partition map, which means this was not sorted at write
      // or at least we didn't have information that it was sorted
      val partitionMap = optPartitionMap.getOrElse("")

      // this is used to parse out the json. we use default because we don't need
      // anything special
      implicit val formats = DefaultFormats
      val partitionMapBuilder = new ArrayBuffer[Option[(ReferenceRegion, ReferenceRegion)]]

      // using json4s to parse the json values
      // we have to cast it because the JSON parser does not actually give
      // us the raw types. instead, it uses a wrapper which requires that we
      // cast to the correct types. we also have to use Any because there
      // are both Strings and BigInts stored there (by json4s), so we cast
      // them later
      val parsedJson = (parse(partitionMap) \ "partitionMap").values
        .asInstanceOf[List[Map[String, Any]]]
      for (f <- parsedJson) {
        if (f.get("ReferenceRegion1").get.toString == "None") {
          partitionMapBuilder += None
        } else {
          // ReferenceRegion1 in storage is the lower bound for the partition
          val lowerBoundJson = f.get("ReferenceRegion1")
            .get
            .asInstanceOf[Map[String, Any]]

          val lowerBound = ReferenceRegion(
            lowerBoundJson.get("referenceName").get.toString,
            lowerBoundJson.get("start").get.asInstanceOf[BigInt].toLong,
            lowerBoundJson.get("end").get.asInstanceOf[BigInt].toLong)
          // ReferenceRegion2 in storage is the upper bound for the partition
          val upperBoundJson = f.get("ReferenceRegion2")
            .get
            .asInstanceOf[Map[String, Any]]

          val upperBound = ReferenceRegion(
            upperBoundJson.get("referenceName").get.toString,
            upperBoundJson.get("start").get.asInstanceOf[BigInt].toLong,
            upperBoundJson.get("end").get.asInstanceOf[BigInt].toLong)

          partitionMapBuilder += Some((lowerBound, upperBound))
        }
      }

      Some(partitionMapBuilder.toArray)
    } catch {
      case e: FileNotFoundException => None
      case e: Throwable             => throw e
    }
  }

  /**
   * Load a path name in Parquet + Avro format into an AlignmentRecordRDD.
   *
   * @note The sequence dictionary is read from an Avro file stored at
   *   pathName/_seqdict.avro and the record group dictionary is read from an
   *   Avro file stored at pathName/_rgdict.avro. These files are pure Avro,
   *   not Parquet + Avro.
   *
   * @param pathName The path name to load alignment records from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of alignment records,
   *   sequence dictionary representing contigs the alignment records may be aligned to,
   *   and the record group dictionary for the alignment records if one is available.
   */
  def loadParquetAlignments(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): AlignmentRecordRDD = {

    // convert avro to sequence dictionary
    val sd = loadAvroSequenceDictionary(pathName)

    // convert avro to sequence dictionary
    val rgd = loadAvroRecordGroupDictionary(pathName)

    // load processing step descriptions
    val pgs = loadAvroPrograms(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundAlignmentRecordRDD(sc, pathName, sd, rgd, pgs)
      }
      case (_, _) => {
        // load from disk
        val rdd = loadParquet[AlignmentRecord](pathName, optPredicate, optProjection)

        RDDBoundAlignmentRecordRDD(rdd, sd, rgd, pgs,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load unaligned alignment records from interleaved FASTQ into an AlignmentRecordRDD.
   *
   * In interleaved FASTQ, the two reads from a paired sequencing protocol are
   * interleaved in a single file. This is a zipped representation of the
   * typical paired FASTQ.
   *
   * @param pathName The path name to load unaligned alignment records from.
   *   Globs/directories are supported.
   * @return Returns an unaligned AlignmentRecordRDD.
   */
  def loadInterleavedFastq(
    pathName: String): AlignmentRecordRDD = LoadInterleavedFastq.time {

    val job = HadoopUtil.newJob(sc)
    val conf = ContextUtil.getConfiguration(job)
    conf.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
    val records = sc.newAPIHadoopFile(
      pathName,
      classOf[InterleavedFastqInputFormat],
      classOf[Void],
      classOf[Text],
      conf
    )
    if (Metrics.isRecording) records.instrument() else records

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    AlignmentRecordRDD.unaligned(records.flatMap(fastqRecordConverter.convertPair))
  }

  /**
   * Load unaligned alignment records from (possibly paired) FASTQ into an AlignmentRecordRDD.
   *
   * @see loadPairedFastq
   * @see loadUnpairedFastq
   *
   * @param pathName1 The path name to load the first set of unaligned alignment records from.
   *   Globs/directories are supported.
   * @param optPathName2 The path name to load the second set of unaligned alignment records from,
   *   if provided. Globs/directories are supported.
   * @param optRecordGroup The optional record group name to associate to the unaligned alignment
   *   records. Defaults to None.
   * @param stringency The validation stringency to use when validating (possibly paired) FASTQ format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns an unaligned AlignmentRecordRDD.
   */
  def loadFastq(
    pathName1: String,
    optPathName2: Option[String],
    optRecordGroup: Option[String] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = LoadFastq.time {

    optPathName2.fold({
      loadUnpairedFastq(pathName1,
        optRecordGroup = optRecordGroup,
        stringency = stringency)
    })(filePath2 => {
      loadPairedFastq(pathName1,
        filePath2,
        optRecordGroup = optRecordGroup,
        stringency = stringency)
    })
  }

  /**
   * Load unaligned alignment records from paired FASTQ into an AlignmentRecordRDD.
   *
   * @param pathName1 The path name to load the first set of unaligned alignment records from.
   *   Globs/directories are supported.
   * @param pathName2 The path name to load the second set of unaligned alignment records from.
   *   Globs/directories are supported.
   * @param optRecordGroup The optional record group name to associate to the unaligned alignment
   *   records. Defaults to None.
   * @param stringency The validation stringency to use when validating paired FASTQ format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns an unaligned AlignmentRecordRDD.
   */
  def loadPairedFastq(
    pathName1: String,
    pathName2: String,
    optRecordGroup: Option[String] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = LoadPairedFastq.time {

    val reads1 = loadUnpairedFastq(
      pathName1,
      setFirstOfPair = true,
      optRecordGroup = optRecordGroup,
      stringency = stringency
    )
    val reads2 = loadUnpairedFastq(
      pathName2,
      setSecondOfPair = true,
      optRecordGroup = optRecordGroup,
      stringency = stringency
    )

    stringency match {
      case ValidationStringency.STRICT | ValidationStringency.LENIENT =>
        val count1 = reads1.rdd.cache.count
        val count2 = reads2.rdd.cache.count

        if (count1 != count2) {
          val msg = s"Fastq 1 ($pathName1) has $count1 reads, fastq 2 ($pathName2) has $count2 reads"
          if (stringency == ValidationStringency.STRICT)
            throw new IllegalArgumentException(msg)
          else {
            // ValidationStringency.LENIENT
            logError(msg)
          }
        }
      case ValidationStringency.SILENT =>
    }

    AlignmentRecordRDD.unaligned(reads1.rdd ++ reads2.rdd)
  }

  /**
   * Load unaligned alignment records from unpaired FASTQ into an AlignmentRecordRDD.
   *
   * @param pathName The path name to load unaligned alignment records from.
   *   Globs/directories are supported.
   * @param setFirstOfPair If true, sets the unaligned alignment record as first from the fragment.
   *   Defaults to false.
   * @param setSecondOfPair If true, sets the unaligned alignment record as second from the fragment.
   *   Defaults to false.
   * @param optRecordGroup The optional record group name to associate to the unaligned alignment
   *   records. Defaults to None.
   * @param stringency The validation stringency to use when validating unpaired FASTQ format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns an unaligned AlignmentRecordRDD.
   */
  def loadUnpairedFastq(
    pathName: String,
    setFirstOfPair: Boolean = false,
    setSecondOfPair: Boolean = false,
    optRecordGroup: Option[String] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = LoadUnpairedFastq.time {

    val job = HadoopUtil.newJob(sc)
    val conf = ContextUtil.getConfiguration(job)
    conf.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)

    val records = sc.newAPIHadoopFile(
      pathName,
      classOf[SingleFastqInputFormat],
      classOf[Void],
      classOf[Text],
      conf
    )
    if (Metrics.isRecording) records.instrument() else records

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    AlignmentRecordRDD.unaligned(records.map(
      fastqRecordConverter.convertRead(
        _,
        optRecordGroup.map(recordGroup =>
          if (recordGroup.isEmpty)
            pathName.substring(pathName.lastIndexOf("/") + 1)
          else
            recordGroup),
        setFirstOfPair,
        setSecondOfPair,
        stringency
      )
    ))
  }

  /**
   * @param pathName The path name to load VCF variant context records from.
   *   Globs/directories are supported.
   * @param optViewRegions Optional intervals to push down into file using index.
   * @return Returns a raw RDD of (LongWritable, VariantContextWritable)s.
   */
  private def readVcfRecords(
    pathName: String,
    optViewRegions: Option[Iterable[ReferenceRegion]]): RDD[(LongWritable, VariantContextWritable)] = {

    // load vcf data
    val job = HadoopUtil.newJob(sc)
    job.getConfiguration().setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName(),
      classOf[BGZFEnhancedGzipCodec].getCanonicalName())

    val conf = ContextUtil.getConfiguration(job)
    optViewRegions.foreach(vr => {
      val intervals = vr.toList.map(r => LocatableReferenceRegion(r))
      VCFInputFormat.setIntervals(conf, intervals)
    })

    sc.newAPIHadoopFile(
      pathName,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      conf
    )
  }

  /**
   * Load variant context records from VCF into a VariantContextRDD.
   *
   * @param pathName The path name to load VCF variant context records from.
   *   Globs/directories are supported.
   * @param stringency The validation stringency to use when validating VCF format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a VariantContextRDD.
   */
  def loadVcf(
    pathName: String,
    stringency: ValidationStringency = ValidationStringency.STRICT): VariantContextRDD = LoadVcf.time {

    // load records from VCF
    val records = readVcfRecords(pathName, None)

    // attach instrumentation
    if (Metrics.isRecording) records.instrument() else records

    // load vcf metadata
    val (sd, samples, headers) = loadVcfMetadata(pathName)

    val vcc = VariantContextConverter(headers, stringency, sc.hadoopConfiguration)
    VariantContextRDD(records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples,
      VariantContextConverter.cleanAndMixInSupportedLines(headers, stringency, log))
  }

  /**
   * Load variant context records from VCF into a VariantContextRDD.
   *
   * Only converts the core Genotype/Variant fields, and the fields set in the
   * requested projection. Core variant fields include:
   *
   * * Names (ID)
   * * Filters (FILTER)
   *
   * Core genotype fields include:
   *
   * * Allelic depth (AD)
   * * Read depth (DP)
   * * Min read depth (MIN_DP)
   * * Genotype quality (GQ)
   * * Genotype likelihoods (GL/PL)
   * * Strand bias components (SB)
   * * Phase info (PS,PQ)
   *
   * @param pathName The path name to load VCF variant context records from.
   *   Globs/directories are supported.
   * @param infoFields The info fields to include, in addition to the ID and
   *   FILTER attributes.
   * @param formatFields The format fields to include, in addition to the core
   *   fields listed above.
   * @param stringency The validation stringency to use when validating VCF format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a VariantContextRDD.
   */
  def loadVcfWithProjection(
    pathName: String,
    infoFields: Set[String],
    formatFields: Set[String],
    stringency: ValidationStringency = ValidationStringency.STRICT): VariantContextRDD = LoadVcf.time {

    // load records from VCF
    val records = readVcfRecords(pathName, None)

    // attach instrumentation
    if (Metrics.isRecording) records.instrument() else records

    // load vcf metadata
    val (sd, samples, headers) = loadVcfMetadata(pathName)

    val vcc = VariantContextConverter(headers.flatMap(hl => hl match {
      case il: VCFInfoHeaderLine => {
        if (infoFields(il.getID)) {
          Some(il)
        } else {
          None
        }
      }
      case fl: VCFFormatHeaderLine => {
        if (formatFields(fl.getID)) {
          Some(fl)
        } else {
          None
        }
      }
      case _ => None
    }), stringency, sc.hadoopConfiguration)
    VariantContextRDD(records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples,
      VariantContextConverter.cleanAndMixInSupportedLines(headers, stringency, log))
  }

  /**
   * Load variant context records from VCF indexed by tabix (tbi) into a VariantContextRDD.
   *
   * @param pathName The path name to load VCF variant context records from.
   *   Globs/directories are supported.
   * @param viewRegion ReferenceRegion we are filtering on.
   * @return Returns a VariantContextRDD.
   */
  // todo: add stringency with default if possible
  def loadIndexedVcf(
    pathName: String,
    viewRegion: ReferenceRegion): VariantContextRDD = {
    loadIndexedVcf(pathName, Iterable(viewRegion))
  }

  /**
   * Load variant context records from VCF indexed by tabix (tbi) into a VariantContextRDD.
   *
   * @param pathName The path name to load VCF variant context records from.
   *   Globs/directories are supported.
   * @param viewRegions Iterator of ReferenceRegions we are filtering on.
   * @param stringency The validation stringency to use when validating VCF format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a VariantContextRDD.
   */
  def loadIndexedVcf(
    pathName: String,
    viewRegions: Iterable[ReferenceRegion],
    stringency: ValidationStringency = ValidationStringency.STRICT)(implicit s: DummyImplicit): VariantContextRDD = LoadIndexedVcf.time {

    // load records from VCF
    val records = readVcfRecords(pathName, Some(viewRegions))

    // attach instrumentation
    if (Metrics.isRecording) records.instrument() else records

    // load vcf metadata
    val (sd, samples, headers) = loadVcfMetadata(pathName)

    val vcc = VariantContextConverter(headers, stringency, sc.hadoopConfiguration)
    VariantContextRDD(records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples,
      VariantContextConverter.cleanAndMixInSupportedLines(headers, stringency, log))
  }

  /**
   * Load a path name in Parquet + Avro format into a GenotypeRDD.
   *
   * @param pathName The path name to load genotypes from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a GenotypeRDD.
   */
  def loadParquetGenotypes(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): GenotypeRDD = {

    // load header lines
    val headers = loadHeaderLines(pathName)

    // load sequence info
    val sd = loadAvroSequenceDictionary(pathName)

    // load avro record group dictionary and convert to samples
    val samples = loadAvroSamples(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundGenotypeRDD(sc, pathName, sd, samples, headers)
      }
      case (_, _) => {
        // load from disk
        val rdd = loadParquet[Genotype](pathName, optPredicate, optProjection)

        new RDDBoundGenotypeRDD(rdd, sd, samples, headers,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load a path name in Parquet + Avro format into a VariantRDD.
   *
   * @param pathName The path name to load variants from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a VariantRDD.
   */
  def loadParquetVariants(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): VariantRDD = {

    val sd = loadAvroSequenceDictionary(pathName)

    // load header lines
    val headers = loadHeaderLines(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        new ParquetUnboundVariantRDD(sc, pathName, sd, headers)
      }
      case _ => {
        val rdd = loadParquet[Variant](pathName, optPredicate, optProjection)
        new RDDBoundVariantRDD(rdd, sd, headers,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load nucleotide contig fragments from FASTA into a NucleotideContigFragmentRDD.
   *
   * @param pathName The path name to load nucleotide contig fragments from.
   *   Globs/directories are supported.
   * @param maximumLength Maximum fragment length. Defaults to 10000L. Values greater
   *   than 1e9 should be avoided.
   * @return Returns a NucleotideContigFragmentRDD.
   */
  def loadFasta(
    pathName: String,
    maximumLength: Long = 10000L): NucleotideContigFragmentRDD = LoadFasta.time {

    val fastaData: RDD[(LongWritable, Text)] = sc.newAPIHadoopFile(
      pathName,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]
    )
    if (Metrics.isRecording) fastaData.instrument() else fastaData

    val remapData = fastaData.map(kv => (kv._1.get, kv._2.toString))

    // convert rdd and cache
    val fragmentRdd = FastaConverter(remapData, maximumLength)
      .cache()

    NucleotideContigFragmentRDD(fragmentRdd)
  }

  /**
   * Load paired unaligned alignment records grouped by sequencing fragment
   * from interleaved FASTQ into an FragmentRDD.
   *
   * In interleaved FASTQ, the two reads from a paired sequencing protocol are
   * interleaved in a single file. This is a zipped representation of the
   * typical paired FASTQ.
   *
   * Fragments represent all of the reads from a single sequenced fragment as
   * a single object, which is a useful representation for some tasks.
   *
   * @param pathName The path name to load unaligned alignment records from.
   *   Globs/directories are supported.
   * @return Returns a FragmentRDD containing the paired reads grouped by
   *   sequencing fragment.
   */
  def loadInterleavedFastqAsFragments(
    pathName: String): FragmentRDD = LoadInterleavedFastqFragments.time {

    val job = HadoopUtil.newJob(sc)
    val conf = ContextUtil.getConfiguration(job)
    conf.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
    val records = sc.newAPIHadoopFile(
      pathName,
      classOf[InterleavedFastqInputFormat],
      classOf[Void],
      classOf[Text],
      conf
    )
    if (Metrics.isRecording) records.instrument() else records

    // convert records
    val fastqRecordConverter = new FastqRecordConverter
    FragmentRDD.fromRdd(records.map(fastqRecordConverter.convertFragment))
  }

  /**
   * Load features into a FeatureRDD and convert to a CoverageRDD.
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
   * @see loadBed
   * @see loadGtf
   * @see loadGff3
   * @see loadNarrowPeak
   * @see loadIntervalList
   * @see loadParquetFeatures
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported, although file extension must be present
   *   for BED6/12, GFF3, GTF/GFF2, NarrowPeak, or IntervalList formats.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to use. For
   *   textual formats, if this is None, fall back to the Spark default
   *   parallelism. Defaults to None.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param stringency The validation stringency to use when validating BED6/12, GFF3,
   *   GTF/GFF2, NarrowPeak, or IntervalList formats. Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureRDD converted to a CoverageRDD.
   */
  def loadCoverage(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): CoverageRDD = LoadCoverage.time {

    loadFeatures(pathName,
      optSequenceDictionary = optSequenceDictionary,
      optMinPartitions = optMinPartitions,
      optPredicate = optPredicate,
      optProjection = optProjection,
      stringency = stringency).toCoverage
  }

  /**
   * Load a path name in Parquet + Avro format into a FeatureRDD and convert to a CoverageRDD.
   * Coverage is stored in the score field of Feature.
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param forceRdd Forces loading the RDD.
   * @return Returns a FeatureRDD converted to a CoverageRDD.
   */
  def loadParquetCoverage(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    forceRdd: Boolean = false): CoverageRDD = {

    if (optPredicate.isEmpty && !forceRdd) {
      // convert avro to sequence dictionary
      val sd = loadAvroSequenceDictionary(pathName)

      new ParquetUnboundCoverageRDD(sc, pathName, sd)
    } else {
      val coverageFields = Projection(FeatureField.contigName,
        FeatureField.start,
        FeatureField.end,
        FeatureField.score)
      loadParquetFeatures(pathName,
        optPredicate = optPredicate,
        optProjection = Some(coverageFields))
        .toCoverage
    }
  }

  /**
   * Load a path name in GFF3 format into a FeatureRDD.
   *
   * @param pathName The path name to load features in GFF3 format from.
   *   Globs/directories are supported.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating GFF3 format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureRDD.
   */
  def loadGff3(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureRDD = LoadGff3.time {

    val records = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new GFF3Parser().parse(_, stringency))
    if (Metrics.isRecording) records.instrument() else records

    optSequenceDictionary
      .fold(FeatureRDD(records))(FeatureRDD(records, _))
  }

  /**
   * Load a path name in GTF/GFF2 format into a FeatureRDD.
   *
   * @param pathName The path name to load features in GTF/GFF2 format from.
   *   Globs/directories are supported.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating GTF/GFF2 format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureRDD.
   */
  def loadGtf(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureRDD = LoadGtf.time {

    val records = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new GTFParser().parse(_, stringency))
    if (Metrics.isRecording) records.instrument() else records

    optSequenceDictionary
      .fold(FeatureRDD(records))(FeatureRDD(records, _))
  }

  /**
   * Load a path name in BED6/12 format into a FeatureRDD.
   *
   * @param pathName The path name to load features in BED6/12 format from.
   *   Globs/directories are supported.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating BED6/12 format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureRDD.
   */
  def loadBed(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureRDD = LoadBed.time {

    val records = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new BEDParser().parse(_, stringency))
    if (Metrics.isRecording) records.instrument() else records

    optSequenceDictionary
      .fold(FeatureRDD(records))(FeatureRDD(records, _))
  }

  /**
   * Load a path name in NarrowPeak format into a FeatureRDD.
   *
   * @param pathName The path name to load features in NarrowPeak format from.
   *   Globs/directories are supported.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating NarrowPeak format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureRDD.
   */
  def loadNarrowPeak(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureRDD = LoadNarrowPeak.time {

    val records = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new NarrowPeakParser().parse(_, stringency))
    if (Metrics.isRecording) records.instrument() else records

    optSequenceDictionary
      .fold(FeatureRDD(records))(FeatureRDD(records, _))
  }

  /**
   * Load a path name in IntervalList format into a FeatureRDD.
   *
   * @param pathName The path name to load features in IntervalList format from.
   *   Globs/directories are supported.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating IntervalList format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureRDD.
   */
  def loadIntervalList(
    pathName: String,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureRDD = LoadIntervalList.time {

    val parsedLines = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .map(new IntervalListParser().parseWithHeader(_, stringency))
    val (seqDict, records) = (SequenceDictionary(parsedLines.flatMap(_._1).collect(): _*),
      parsedLines.flatMap(_._2))

    if (Metrics.isRecording) records.instrument() else records
    FeatureRDD(records, seqDict)
  }

  /**
   * Load a path name in Parquet + Avro format into a FeatureRDD.
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a FeatureRDD.
   */
  def loadParquetFeatures(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): FeatureRDD = {

    val sd = loadAvroSequenceDictionary(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundFeatureRDD(sc, pathName, sd)
      }
      case (_, _) => {
        // load from disk
        val rdd = loadParquet[Feature](pathName, optPredicate, optProjection)

        new RDDBoundFeatureRDD(rdd, sd, optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load a path name in Parquet + Avro format into a NucleotideContigFragmentRDD.
   *
   * @param pathName The path name to load nucleotide contig fragments from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a NucleotideContigFragmentRDD.
   */
  def loadParquetContigFragments(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): NucleotideContigFragmentRDD = {

    val sd = loadAvroSequenceDictionary(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundNucleotideContigFragmentRDD(
          sc, pathName, sd)
      }
      case (_, _) => {
        val rdd = loadParquet[NucleotideContigFragment](pathName, optPredicate, optProjection)
        new RDDBoundNucleotideContigFragmentRDD(rdd,
          sd,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load a path name in Parquet + Avro format into a FragmentRDD.
   *
   * @param pathName The path name to load fragments from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a FragmentRDD.
   */
  def loadParquetFragments(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): FragmentRDD = {

    // convert avro to sequence dictionary
    val sd = loadAvroSequenceDictionary(pathName)

    // convert avro to sequence dictionary
    val rgd = loadAvroRecordGroupDictionary(pathName)

    // load processing step descriptions
    val pgs = loadAvroPrograms(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundFragmentRDD(sc, pathName, sd, rgd, pgs)
      }
      case (_, _) => {
        // load from disk
        val rdd = loadParquet[Fragment](pathName, optPredicate, optProjection)

        new RDDBoundFragmentRDD(rdd,
          sd,
          rgd,
          pgs,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load features into a FeatureRDD.
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
   * @see loadBed
   * @see loadGtf
   * @see loadGff3
   * @see loadNarrowPeak
   * @see loadIntervalList
   * @see loadParquetFeatures
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported, although file extension must be present
   *   for BED6/12, GFF3, GTF/GFF2, NarrowPeak, or IntervalList formats.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to use. For
   *   textual formats, if this is None, fall back to the Spark default
   *   parallelism. Defaults to None.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param stringency The validation stringency to use when validating BED6/12, GFF3,
   *   GTF/GFF2, NarrowPeak, or IntervalList formats. Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureRDD.
   */
  def loadFeatures(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureRDD = LoadFeatures.time {

    val trimmedPathName = trimExtensionIfCompressed(pathName)
    if (isBedExt(trimmedPathName)) {
      log.info(s"Loading $pathName as BED and converting to Features.")
      loadBed(pathName,
        optSequenceDictionary = optSequenceDictionary,
        optMinPartitions = optMinPartitions,
        stringency = stringency)
    } else if (isGff3Ext(trimmedPathName)) {
      log.info(s"Loading $pathName as GFF3 and converting to Features.")
      loadGff3(pathName,
        optSequenceDictionary = optSequenceDictionary,
        optMinPartitions = optMinPartitions,
        stringency = stringency)
    } else if (isGtfExt(trimmedPathName)) {
      log.info(s"Loading $pathName as GTF/GFF2 and converting to Features.")
      loadGtf(pathName,
        optSequenceDictionary = optSequenceDictionary,
        optMinPartitions = optMinPartitions,
        stringency = stringency)
    } else if (isNarrowPeakExt(trimmedPathName)) {
      log.info(s"Loading $pathName as NarrowPeak and converting to Features.")
      loadNarrowPeak(pathName,
        optSequenceDictionary = optSequenceDictionary,
        optMinPartitions = optMinPartitions,
        stringency = stringency)
    } else if (isIntervalListExt(trimmedPathName)) {
      log.info(s"Loading $pathName as IntervalList and converting to Features.")
      loadIntervalList(pathName,
        optMinPartitions = optMinPartitions,
        stringency = stringency)
    } else {
      log.info(s"Loading $pathName as Parquet containing Features.")
      loadParquetFeatures(pathName,
        optPredicate = optPredicate,
        optProjection = optProjection)
    }
  }

  /**
   * Load reference sequences into a broadcastable ReferenceFile.
   *
   * If the path name has a .2bit extension, loads a 2bit file. Else, uses loadContigFragments
   * to load the reference as an RDD, which is then collected to the driver.
   *
   * @see loadContigFragments
   *
   * @param pathName The path name to load reference sequences from.
   *   Globs/directories for 2bit format are not supported.
   * @param maximumLength Maximum fragment length. Defaults to 10000L. Values greater
   *   than 1e9 should be avoided.
   * @return Returns a broadcastable ReferenceFile.
   */
  def loadReferenceFile(
    pathName: String,
    maximumLength: Long): ReferenceFile = LoadReferenceFile.time {

    if (is2BitExt(pathName)) {
      new TwoBitFile(new LocalFileByteAccess(new File(pathName)))
    } else {
      ReferenceContigMap(loadContigFragments(pathName, maximumLength = maximumLength).rdd)
    }
  }

  /**
   * Load a sequence dictionary.
   *
   * Loads path names ending in:
   * * .dict as HTSJDK sequence dictionary format,
   * * .genome as Bedtools genome file format,
   * * .txt as UCSC Genome Browser chromInfo files.
   *
   * Compressed files are supported through compression codecs configured
   * in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @param pathName The path name to load a sequence dictionary from.
   * @return Returns a sequence dictionary.
   * @throws IllegalArgumentException if pathName file extension not one of .dict,
   *   .genome, or .txt
   */
  def loadSequenceDictionary(pathName: String): SequenceDictionary = LoadSequenceDictionary.time {
    val trimmedPathName = trimExtensionIfCompressed(pathName)
    if (isDictExt(trimmedPathName)) {
      log.info(s"Loading $pathName as HTSJDK sequence dictionary.")
      SequenceDictionaryReader(pathName, sc)
    } else if (isGenomeExt(trimmedPathName)) {
      log.info(s"Loading $pathName as Bedtools genome file sequence dictionary.")
      GenomeFileReader(pathName, sc)
    } else if (isTextExt(trimmedPathName)) {
      log.info(s"Loading $pathName as UCSC Genome Browser chromInfo file sequence dictionary.")
      GenomeFileReader(pathName, sc)
    } else {
      throw new IllegalArgumentException("Path name file extension must be one of .dict, .genome, or .txt")
    }
  }

  /**
   * Load nucleotide contig fragments into a NucleotideContigFragmentRDD.
   *
   * If the path name has a .fa/.fasta extension, load as FASTA format.
   * Else, fall back to Parquet + Avro.
   *
   * For FASTA format, compressed files are supported through compression codecs configured
   * in Hadoop, which by default include .gz and .bz2, but can include more.
   *
   * @see loadFasta
   * @see loadParquetContigFragments
   *
   * @param pathName The path name to load nucleotide contig fragments from.
   *   Globs/directories are supported, although file extension must be present
   *   for FASTA format.
   * @param maximumLength Maximum fragment length. Defaults to 10000L. Values greater
   *   than 1e9 should be avoided.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a NucleotideContigFragmentRDD.
   */
  def loadContigFragments(
    pathName: String,
    maximumLength: Long = 10000L,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): NucleotideContigFragmentRDD = LoadContigFragments.time {

    val trimmedPathName = trimExtensionIfCompressed(pathName)
    if (isFastaExt(trimmedPathName)) {
      log.info(s"Loading $pathName as FASTA and converting to NucleotideContigFragment.")
      loadFasta(
        pathName,
        maximumLength
      )
    } else {
      log.info(s"Loading $pathName as Parquet containing NucleotideContigFragments.")
      loadParquetContigFragments(pathName, optPredicate = optPredicate, optProjection = optProjection)
    }
  }

  /**
   * Load genotypes into a GenotypeRDD.
   *
   * If the path name has a .vcf/.vcf.gz/.vcf.bgz extension, load as VCF format.
   * Else, fall back to Parquet + Avro.
   *
   * @see loadVcf
   * @see loadParquetGenotypes
   *
   * @param pathName The path name to load genotypes from.
   *   Globs/directories are supported, although file extension must be present
   *   for VCF format.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param stringency The validation stringency to use when validating VCF format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a GenotypeRDD.
   */
  def loadGenotypes(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): GenotypeRDD = LoadGenotypes.time {

    if (isVcfExt(pathName)) {
      log.info(s"Loading $pathName as VCF and converting to Genotypes.")
      loadVcf(pathName, stringency).toGenotypes
    } else {
      log.info(s"Loading $pathName as Parquet containing Genotypes. Sequence dictionary for translation is ignored.")
      loadParquetGenotypes(pathName, optPredicate = optPredicate, optProjection = optProjection)
    }
  }

  /**
   * Load variants into a VariantRDD.
   *
   * If the path name has a .vcf/.vcf.gz/.vcf.bgz extension, load as VCF format.
   * Else, fall back to Parquet + Avro.
   *
   * @see loadVcf
   * @see loadParquetVariants
   *
   * @param pathName The path name to load variants from.
   *   Globs/directories are supported, although file extension must be present for VCF format.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param stringency The validation stringency to use when validating VCF format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a VariantRDD.
   */
  def loadVariants(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): VariantRDD = LoadVariants.time {

    if (isVcfExt(pathName)) {
      log.info(s"Loading $pathName as VCF and converting to Variants.")
      loadVcf(pathName, stringency).toVariants
    } else {
      log.info(s"Loading $pathName as Parquet containing Variants. Sequence dictionary for translation is ignored.")
      loadParquetVariants(pathName, optPredicate = optPredicate, optProjection = optProjection)
    }
  }

  /**
   * Load alignment records into an AlignmentRecordRDD.
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
   * @see loadBam
   * @see loadFastq
   * @see loadFasta
   * @see loadInterleavedFastq
   * @see loadParquetAlignments
   *
   * @param pathName The path name to load alignment records from.
   *   Globs/directories are supported, although file extension must be present
   *   for BAM/CRAM/SAM, FASTA, and FASTQ formats.
   * @param optPathName2 The optional path name to load the second set of alignment
   *   records from, if loading paired FASTQ format. Globs/directories are supported,
   *   although file extension must be present. Defaults to None.
   * @param optRecordGroup The optional record group name to associate to the alignment
   *   records. Defaults to None.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param stringency The validation stringency to use when validating BAM/CRAM/SAM or FASTQ formats.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns an AlignmentRecordRDD which wraps the RDD of alignment records,
   *   sequence dictionary representing contigs the alignment records may be aligned to,
   *   and the record group dictionary for the alignment records if one is available.
   */
  def loadAlignments(
    pathName: String,
    optPathName2: Option[String] = None,
    optRecordGroup: Option[String] = None,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordRDD = LoadAlignments.time {

    // need this to pick up possible .bgz extension
    sc.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
    val trimmedPathName = trimExtensionIfCompressed(pathName)
    if (isBamExt(trimmedPathName)) {
      log.info(s"Loading $pathName as BAM/CRAM/SAM and converting to AlignmentRecords.")
      loadBam(pathName, stringency)
    } else if (isInterleavedFastqExt(trimmedPathName)) {
      log.info(s"Loading $pathName as interleaved FASTQ and converting to AlignmentRecords.")
      loadInterleavedFastq(pathName)
    } else if (isFastqExt(trimmedPathName)) {
      log.info(s"Loading $pathName as unpaired FASTQ and converting to AlignmentRecords.")
      loadFastq(pathName, optPathName2, optRecordGroup, stringency)
    } else if (isFastaExt(trimmedPathName)) {
      log.info(s"Loading $pathName as FASTA and converting to AlignmentRecords.")
      AlignmentRecordRDD.unaligned(loadFasta(pathName, maximumLength = 10000L).toReads)
    } else {
      log.info(s"Loading $pathName as Parquet of AlignmentRecords.")
      loadParquetAlignments(pathName, optPredicate = optPredicate, optProjection = optProjection)
    }
  }

  /**
   * Load fragments into a FragmentRDD.
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
   * @see loadBam
   * @see loadAlignments
   * @see loadInterleavedFastqAsFragments
   * @see loadParquetFragments
   *
   * @param pathName The path name to load fragments from.
   *   Globs/directories are supported, although file extension must be present
   *   for BAM/CRAM/SAM and FASTQ formats.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param stringency The validation stringency to use when validating BAM/CRAM/SAM or FASTQ formats.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FragmentRDD.
   */
  def loadFragments(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FragmentRDD = LoadFragments.time {

    // need this to pick up possible .bgz extension
    sc.hadoopConfiguration.setStrings("io.compression.codecs",
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName)
    val trimmedPathName = trimExtensionIfCompressed(pathName)
    if (isBamExt(trimmedPathName)) {
      // check to see if the input files are all queryname sorted
      if (filesAreQueryGrouped(pathName)) {
        log.info(s"Loading $pathName as queryname sorted BAM/CRAM/SAM and converting to Fragments.")
        loadBam(pathName, stringency).transform(RepairPartitions(_))
          .querynameSortedToFragments
      } else {
        log.info(s"Loading $pathName as BAM/CRAM/SAM and converting to Fragments.")
        loadBam(pathName, stringency).toFragments
      }
    } else if (isInterleavedFastqExt(trimmedPathName)) {
      log.info(s"Loading $pathName as interleaved FASTQ and converting to Fragments.")
      loadInterleavedFastqAsFragments(pathName)
    } else {
      log.info(s"Loading $pathName as Parquet containing Fragments.")
      loadParquetFragments(pathName, optPredicate = optPredicate, optProjection = optProjection)
    }
  }
}
