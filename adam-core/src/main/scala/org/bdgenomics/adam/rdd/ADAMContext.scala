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
import org.apache.spark.sql.{ Dataset, SparkSession, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.converters._
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.io._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.projections.{
  FeatureField,
  Projection
}
import org.bdgenomics.adam.rdd.contig.{
  DatasetBoundNucleotideContigFragmentDataset,
  NucleotideContigFragmentDataset,
  ParquetUnboundNucleotideContigFragmentDataset,
  RDDBoundNucleotideContigFragmentDataset
}
import org.bdgenomics.adam.rdd.feature._
import org.bdgenomics.adam.rdd.fragment.{
  DatasetBoundFragmentDataset,
  FragmentDataset,
  ParquetUnboundFragmentDataset,
  RDDBoundFragmentDataset
}
import org.bdgenomics.adam.rdd.read.{
  AlignmentRecordDataset,
  DatasetBoundAlignmentRecordDataset,
  RepairPartitions,
  ParquetUnboundAlignmentRecordDataset,
  RDDBoundAlignmentRecordDataset
}
import org.bdgenomics.adam.rdd.variant._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.sql.{
  AlignmentRecord => AlignmentRecordProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  NucleotideContigFragment => NucleotideContigFragmentProduct,
  Variant => VariantProduct,
  VariantContext => VariantContextProduct
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
  Feature,
  Fragment,
  Genotype,
  NucleotideContigFragment,
  ProcessingStep,
  ReadGroup => ReadGroupMetadata,
  Reference,
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
  implicit def contigsToContigsConversionFn(gDataset: NucleotideContigFragmentDataset,
                                            rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    // hijack the transform function to discard the old RDD
    gDataset.transform(oldRdd => rdd)
  }

  implicit def contigsToCoverageConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def contigsToCoverageDatasetConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    ds: Dataset[Coverage]): CoverageDataset = {
    new DatasetBoundCoverageDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def contigsToFeaturesConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    rdd: RDD[Feature]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def contigsToFeaturesDatasetConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    ds: Dataset[FeatureProduct]): FeatureDataset = {
    new DatasetBoundFeatureDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def contigsToFragmentsConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    rdd: RDD[Fragment]): FragmentDataset = {
    new RDDBoundFragmentDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def contigsToFragmentsDatasetConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    ds: Dataset[FragmentProduct]): FragmentDataset = {
    new DatasetBoundFragmentDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def contigsToAlignmentRecordsConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    new RDDBoundAlignmentRecordDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def contigsToAlignmentRecordsDatasetConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    new DatasetBoundAlignmentRecordDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def contigsToGenotypesConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    rdd: RDD[Genotype]): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def contigsToGenotypesDatasetConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    ds: Dataset[GenotypeProduct]): GenotypeDataset = {
    new DatasetBoundGenotypeDataset(ds,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def contigsToVariantsConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    rdd: RDD[Variant]): VariantDataset = {
    new RDDBoundVariantDataset(rdd,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def contigsToVariantsDatasetConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    ds: Dataset[VariantProduct]): VariantDataset = {
    new DatasetBoundVariantDataset(ds,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def contigsToVariantContextConversionFn(
    gDataset: NucleotideContigFragmentDataset,
    rdd: RDD[VariantContext]): VariantContextDataset = {
    VariantContextDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def coverageToContigsConversionFn(
    gDataset: CoverageDataset,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(rdd, gDataset.sequences, None)
  }

  implicit def coverageToContigsDatasetConversionFn(
    gDataset: CoverageDataset,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    new DatasetBoundNucleotideContigFragmentDataset(ds, gDataset.sequences)
  }

  implicit def coverageToCoverageConversionFn(gDataset: CoverageDataset,
                                              rdd: RDD[Coverage]): CoverageDataset = {
    // hijack the transform function to discard the old RDD
    gDataset.transform(oldRdd => rdd)
  }

  implicit def coverageToFeaturesConversionFn(
    gDataset: CoverageDataset,
    rdd: RDD[Feature]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, gDataset.sequences, gDataset.samples, None)
  }

  implicit def coverageToFeaturesDatasetConversionFn(
    gDataset: CoverageDataset,
    ds: Dataset[FeatureProduct]): FeatureDataset = {
    new DatasetBoundFeatureDataset(ds, gDataset.sequences, gDataset.samples)
  }

  implicit def coverageToFragmentsConversionFn(
    gDataset: CoverageDataset,
    rdd: RDD[Fragment]): FragmentDataset = {
    new RDDBoundFragmentDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def coverageToFragmentsDatasetConversionFn(
    gDataset: CoverageDataset,
    ds: Dataset[FragmentProduct]): FragmentDataset = {
    new DatasetBoundFragmentDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def coverageToAlignmentRecordsConversionFn(
    gDataset: CoverageDataset,
    rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    new RDDBoundAlignmentRecordDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def coverageToAlignmentRecordsDatasetConversionFn(
    gDataset: CoverageDataset,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    new DatasetBoundAlignmentRecordDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def coverageToGenotypesConversionFn(
    gDataset: CoverageDataset,
    rdd: RDD[Genotype]): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def coverageToGenotypesDatasetConversionFn(
    gDataset: CoverageDataset,
    ds: Dataset[GenotypeProduct]): GenotypeDataset = {
    new DatasetBoundGenotypeDataset(ds,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def coverageToVariantsConversionFn(
    gDataset: CoverageDataset,
    rdd: RDD[Variant]): VariantDataset = {
    new RDDBoundVariantDataset(rdd,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def coverageToVariantsDatasetConversionFn(
    gDataset: CoverageDataset,
    ds: Dataset[VariantProduct]): VariantDataset = {
    new DatasetBoundVariantDataset(ds,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def coverageToVariantContextConversionFn(
    gDataset: CoverageDataset,
    rdd: RDD[VariantContext]): VariantContextDataset = {
    VariantContextDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def featuresToContigsConversionFn(
    gDataset: FeatureDataset,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(rdd, gDataset.sequences, None)
  }

  implicit def featuresToContigsDatasetConversionFn(
    gDataset: FeatureDataset,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    new DatasetBoundNucleotideContigFragmentDataset(ds, gDataset.sequences)
  }

  implicit def featuresToCoverageConversionFn(
    gDataset: FeatureDataset,
    rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, gDataset.sequences, gDataset.samples, None)
  }

  implicit def featuresToCoverageDatasetConversionFn(
    gDataset: FeatureDataset,
    ds: Dataset[Coverage]): CoverageDataset = {
    new DatasetBoundCoverageDataset(ds, gDataset.sequences, gDataset.samples)
  }

  implicit def featuresToFeaturesConversionFn(gDataset: FeatureDataset,
                                              rdd: RDD[Feature]): FeatureDataset = {
    // hijack the transform function to discard the old RDD
    gDataset.transform(oldRdd => rdd)
  }

  implicit def featuresToFragmentsConversionFn(
    gDataset: FeatureDataset,
    rdd: RDD[Fragment]): FragmentDataset = {
    new RDDBoundFragmentDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def featuresToFragmentsDatasetConversionFn(
    gDataset: FeatureDataset,
    ds: Dataset[FragmentProduct]): FragmentDataset = {
    new DatasetBoundFragmentDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def featuresToAlignmentRecordsConversionFn(
    gDataset: FeatureDataset,
    rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    new RDDBoundAlignmentRecordDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def featuresToAlignmentRecordsDatasetConversionFn(
    gDataset: FeatureDataset,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    new DatasetBoundAlignmentRecordDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def featuresToGenotypesConversionFn(
    gDataset: FeatureDataset,
    rdd: RDD[Genotype]): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def featuresToGenotypesDatasetConversionFn(
    gDataset: FeatureDataset,
    ds: Dataset[GenotypeProduct]): GenotypeDataset = {
    new DatasetBoundGenotypeDataset(ds,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def featuresToVariantsConversionFn(
    gDataset: FeatureDataset,
    rdd: RDD[Variant]): VariantDataset = {
    new RDDBoundVariantDataset(rdd,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def featuresToVariantsDatasetConversionFn(
    gDataset: FeatureDataset,
    ds: Dataset[VariantProduct]): VariantDataset = {
    new DatasetBoundVariantDataset(ds,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def featuresToVariantContextConversionFn(
    gDataset: FeatureDataset,
    rdd: RDD[VariantContext]): VariantContextDataset = {
    VariantContextDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def fragmentsToContigsConversionFn(
    gDataset: FragmentDataset,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(rdd, gDataset.sequences, None)
  }

  implicit def fragmentsToContigsDatasetConversionFn(
    gDataset: FragmentDataset,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    new DatasetBoundNucleotideContigFragmentDataset(ds, gDataset.sequences)
  }

  implicit def fragmentsToCoverageConversionFn(
    gDataset: FragmentDataset,
    rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def fragmentsToCoverageDatasetConversionFn(
    gDataset: FragmentDataset,
    ds: Dataset[Coverage]): CoverageDataset = {
    new DatasetBoundCoverageDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def fragmentsToFeaturesConversionFn(
    gDataset: FragmentDataset,
    rdd: RDD[Feature]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def fragmentsToFeaturesDatasetConversionFn(
    gDataset: FragmentDataset,
    ds: Dataset[FeatureProduct]): FeatureDataset = {
    new DatasetBoundFeatureDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def fragmentsToFragmentsConversionFn(gDataset: FragmentDataset,
                                                rdd: RDD[Fragment]): FragmentDataset = {
    // hijack the transform function to discard the old RDD
    gDataset.transform(oldRdd => rdd)
  }

  implicit def fragmentsToAlignmentRecordsConversionFn(
    gDataset: FragmentDataset,
    rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    new RDDBoundAlignmentRecordDataset(rdd,
      gDataset.sequences,
      gDataset.readGroups,
      gDataset.processingSteps,
      None)
  }

  implicit def fragmentsToAlignmentRecordsDatasetConversionFn(
    gDataset: FragmentDataset,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    new DatasetBoundAlignmentRecordDataset(ds,
      gDataset.sequences,
      gDataset.readGroups,
      gDataset.processingSteps)
  }

  implicit def fragmentsToGenotypesConversionFn(
    gDataset: FragmentDataset,
    rdd: RDD[Genotype]): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd,
      gDataset.sequences,
      gDataset.readGroups.toSamples,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def fragmentsToGenotypesDatasetConversionFn(
    gDataset: FragmentDataset,
    ds: Dataset[GenotypeProduct]): GenotypeDataset = {
    new DatasetBoundGenotypeDataset(ds,
      gDataset.sequences,
      gDataset.readGroups.toSamples,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def fragmentsToVariantsConversionFn(
    gDataset: FragmentDataset,
    rdd: RDD[Variant]): VariantDataset = {
    new RDDBoundVariantDataset(rdd,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def fragmentsToVariantsDatasetConversionFn(
    gDataset: FragmentDataset,
    ds: Dataset[VariantProduct]): VariantDataset = {
    new DatasetBoundVariantDataset(ds,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def fragmentsToVariantContextConversionFn(
    gDataset: FragmentDataset,
    rdd: RDD[VariantContext]): VariantContextDataset = {
    VariantContextDataset(rdd,
      gDataset.sequences,
      gDataset.readGroups.toSamples,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def genericToContigsConversionFn[Y <: GenericGenomicDataset[_, _]](
    gDataset: Y,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(rdd, gDataset.sequences, None)
  }

  implicit def genericToCoverageConversionFn[Y <: GenericGenomicDataset[_, _]](
    gDataset: Y,
    rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def genericToFeatureConversionFn[Y <: GenericGenomicDataset[_, _]](
    gDataset: Y,
    rdd: RDD[Feature]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def genericToFragmentsConversionFn[Y <: GenericGenomicDataset[_, _]](
    gDataset: Y,
    rdd: RDD[Fragment]): FragmentDataset = {
    new RDDBoundFragmentDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def genericToAlignmentRecordsConversionFn[Y <: GenericGenomicDataset[_, _]](
    gDataset: Y,
    rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    new RDDBoundAlignmentRecordDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def genericToGenotypesConversionFn[Y <: GenericGenomicDataset[_, _]](
    gDataset: Y,
    rdd: RDD[Genotype]): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def genericToVariantsConversionFn[Y <: GenericGenomicDataset[_, _]](
    gDataset: Y,
    rdd: RDD[Variant]): VariantDataset = {
    new RDDBoundVariantDataset(rdd,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def genericToVariantContextsConversionFn[Y <: GenericGenomicDataset[_, _]](
    gDataset: Y,
    rdd: RDD[VariantContext]): VariantContextDataset = {
    new RDDBoundVariantContextDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def alignmentRecordsToContigsConversionFn(
    gDataset: AlignmentRecordDataset,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(rdd, gDataset.sequences, None)
  }

  implicit def alignmentRecordsToContigsDatasetConversionFn(
    gDataset: AlignmentRecordDataset,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    new DatasetBoundNucleotideContigFragmentDataset(ds, gDataset.sequences)
  }

  implicit def alignmentRecordsToCoverageConversionFn(
    gDataset: AlignmentRecordDataset,
    rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def alignmentRecordsToCoverageDatasetConversionFn(
    gDataset: AlignmentRecordDataset,
    ds: Dataset[Coverage]): CoverageDataset = {
    new DatasetBoundCoverageDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def alignmentRecordsToFeaturesConversionFn(
    gDataset: AlignmentRecordDataset,
    rdd: RDD[Feature]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def alignmentRecordsToFeaturesDatasetConversionFn(
    gDataset: AlignmentRecordDataset,
    ds: Dataset[FeatureProduct]): FeatureDataset = {
    new DatasetBoundFeatureDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def alignmentRecordsToFragmentsConversionFn(
    gDataset: AlignmentRecordDataset,
    rdd: RDD[Fragment]): FragmentDataset = {
    new RDDBoundFragmentDataset(rdd,
      gDataset.sequences,
      gDataset.readGroups,
      gDataset.processingSteps,
      None)
  }

  implicit def alignmentRecordsToFragmentsDatasetConversionFn(
    gDataset: AlignmentRecordDataset,
    ds: Dataset[FragmentProduct]): FragmentDataset = {
    new DatasetBoundFragmentDataset(ds,
      gDataset.sequences,
      gDataset.readGroups,
      gDataset.processingSteps)
  }

  implicit def alignmentRecordsToAlignmentRecordsConversionFn(gDataset: AlignmentRecordDataset,
                                                              rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    // hijack the transform function to discard the old RDD
    gDataset.transform(oldRdd => rdd)
  }

  implicit def alignmentRecordsToGenotypesConversionFn(
    gDataset: AlignmentRecordDataset,
    rdd: RDD[Genotype]): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd,
      gDataset.sequences,
      gDataset.readGroups.toSamples,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def alignmentRecordsToGenotypesDatasetConversionFn(
    gDataset: AlignmentRecordDataset,
    ds: Dataset[GenotypeProduct]): GenotypeDataset = {
    new DatasetBoundGenotypeDataset(ds,
      gDataset.sequences,
      gDataset.readGroups.toSamples,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def alignmentRecordsToVariantsConversionFn(
    gDataset: AlignmentRecordDataset,
    rdd: RDD[Variant]): VariantDataset = {
    new RDDBoundVariantDataset(rdd,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines,
      None)
  }

  implicit def alignmentRecordsToVariantsDatasetConversionFn(
    gDataset: AlignmentRecordDataset,
    ds: Dataset[VariantProduct]): VariantDataset = {
    new DatasetBoundVariantDataset(ds,
      gDataset.sequences,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def alignmentRecordsToVariantContextConversionFn(
    gDataset: AlignmentRecordDataset,
    rdd: RDD[VariantContext]): VariantContextDataset = {
    VariantContextDataset(rdd,
      gDataset.sequences,
      gDataset.readGroups.toSamples,
      DefaultHeaderLines.allHeaderLines)
  }

  implicit def genotypesToContigsConversionFn(
    gDataset: GenotypeDataset,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(rdd, gDataset.sequences, None)
  }

  implicit def genotypesToContigsDatasetConversionFn(
    gDataset: GenotypeDataset,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    new DatasetBoundNucleotideContigFragmentDataset(ds, gDataset.sequences)
  }

  implicit def genotypesToCoverageConversionFn(
    gDataset: GenotypeDataset,
    rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def genotypesToCoverageDatasetConversionFn(
    gDataset: GenotypeDataset,
    ds: Dataset[Coverage]): CoverageDataset = {
    new DatasetBoundCoverageDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def genotypesToFeaturesConversionFn(
    gDataset: GenotypeDataset,
    rdd: RDD[Feature]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def genotypesToFeaturesDatasetConversionFn(
    gDataset: GenotypeDataset,
    ds: Dataset[FeatureProduct]): FeatureDataset = {
    new DatasetBoundFeatureDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def genotypesToFragmentsConversionFn(
    gDataset: GenotypeDataset,
    rdd: RDD[Fragment]): FragmentDataset = {
    new RDDBoundFragmentDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def genotypesToFragmentsDatasetConversionFn(
    gDataset: GenotypeDataset,
    ds: Dataset[FragmentProduct]): FragmentDataset = {
    new DatasetBoundFragmentDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def genotypesToAlignmentRecordsConversionFn(
    gDataset: GenotypeDataset,
    rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    new RDDBoundAlignmentRecordDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def genotypesToAlignmentRecordsDatasetConversionFn(
    gDataset: GenotypeDataset,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    new DatasetBoundAlignmentRecordDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def genotypesToGenotypesConversionFn(gDataset: GenotypeDataset,
                                                rdd: RDD[Genotype]): GenotypeDataset = {
    // hijack the transform function to discard the old RDD
    gDataset.transform(oldRdd => rdd)
  }

  implicit def genotypesToVariantsConversionFn(
    gDataset: GenotypeDataset,
    rdd: RDD[Variant]): VariantDataset = {
    new RDDBoundVariantDataset(rdd,
      gDataset.sequences,
      gDataset.headerLines,
      None)
  }

  implicit def genotypesToVariantsDatasetConversionFn(
    gDataset: GenotypeDataset,
    ds: Dataset[VariantProduct]): VariantDataset = {
    new DatasetBoundVariantDataset(ds,
      gDataset.sequences,
      gDataset.headerLines)
  }

  implicit def genotypesToVariantContextConversionFn(
    gDataset: GenotypeDataset,
    rdd: RDD[VariantContext]): VariantContextDataset = {
    VariantContextDataset(rdd,
      gDataset.sequences,
      gDataset.samples,
      gDataset.headerLines)
  }

  implicit def variantsToContigsConversionFn(
    gDataset: VariantDataset,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(rdd, gDataset.sequences, None)
  }

  implicit def variantsToContigsDatasetConversionFn(
    gDataset: VariantDataset,
    ds: Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    new DatasetBoundNucleotideContigFragmentDataset(ds, gDataset.sequences)
  }

  implicit def variantsToCoverageConversionFn(
    gDataset: VariantDataset,
    rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def variantsToCoverageDatasetConversionFn(
    gDataset: VariantDataset,
    ds: Dataset[Coverage]): CoverageDataset = {
    new DatasetBoundCoverageDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def variantsToFeaturesConversionFn(
    gDataset: VariantDataset,
    rdd: RDD[Feature]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def variantsToFeaturesDatasetConversionFn(
    gDataset: VariantDataset,
    ds: Dataset[FeatureProduct]): FeatureDataset = {
    new DatasetBoundFeatureDataset(ds, gDataset.sequences, Seq.empty[Sample])
  }

  implicit def variantsToFragmentsConversionFn(
    gDataset: VariantDataset,
    rdd: RDD[Fragment]): FragmentDataset = {
    new RDDBoundFragmentDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def variantsToFragmentsDatasetConversionFn(
    gDataset: VariantDataset,
    ds: Dataset[FragmentProduct]): FragmentDataset = {
    new DatasetBoundFragmentDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def variantsToAlignmentRecordsConversionFn(
    gDataset: VariantDataset,
    rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    new RDDBoundAlignmentRecordDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def variantsToAlignmentRecordsDatasetConversionFn(
    gDataset: VariantDataset,
    ds: Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    new DatasetBoundAlignmentRecordDataset(ds,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  implicit def variantsToGenotypesConversionFn(
    gDataset: VariantDataset,
    rdd: RDD[Genotype]): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      gDataset.headerLines,
      None)
  }

  implicit def variantsToGenotypesDatasetConversionFn(
    gDataset: VariantDataset,
    ds: Dataset[GenotypeProduct]): GenotypeDataset = {
    new DatasetBoundGenotypeDataset(ds,
      gDataset.sequences,
      Seq.empty,
      gDataset.headerLines)
  }

  implicit def variantsToVariantsConversionFn(gDataset: VariantDataset,
                                              rdd: RDD[Variant]): VariantDataset = {
    // hijack the transform function to discard the old RDD
    gDataset.transform(oldRdd => rdd)
  }

  implicit def variantsToVariantContextConversionFn(
    gDataset: VariantDataset,
    rdd: RDD[VariantContext]): VariantContextDataset = {
    VariantContextDataset(rdd,
      gDataset.sequences,
      Seq.empty,
      gDataset.headerLines)
  }

  implicit def variantContextsToContigsConversionFn(
    gDataset: VariantContextDataset,
    rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(rdd, gDataset.sequences, None)
  }

  implicit def variantContextsToCoverageConversionFn(
    gDataset: VariantContextDataset,
    rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def variantContextsToFeaturesConversionFn(
    gDataset: VariantContextDataset,
    rdd: RDD[Feature]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, gDataset.sequences, Seq.empty[Sample], None)
  }

  implicit def variantContextsToFragmentsConversionFn(
    gDataset: VariantContextDataset,
    rdd: RDD[Fragment]): FragmentDataset = {
    new RDDBoundFragmentDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def variantContextsToAlignmentRecordsConversionFn(
    gDataset: VariantContextDataset,
    rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    new RDDBoundAlignmentRecordDataset(rdd,
      gDataset.sequences,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  implicit def variantContextsToGenotypesConversionFn(
    gDataset: VariantContextDataset,
    rdd: RDD[Genotype]): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd,
      gDataset.sequences,
      gDataset.samples,
      gDataset.headerLines,
      None)
  }

  implicit def variantContextsToVariantsConversionFn(
    gDataset: VariantContextDataset,
    rdd: RDD[Variant]): VariantDataset = {
    new RDDBoundVariantDataset(rdd,
      gDataset.sequences,
      gDataset.headerLines,
      None)
  }

  implicit def variantContextsToVariantContextsConversionFn(gDataset: VariantContextDataset,
                                                            rdd: RDD[VariantContext]): VariantContextDataset = {
    // hijack the transform function to discard the old RDD
    gDataset.transform(oldRdd => rdd)
  }

  // Add ADAM Spark context methods
  implicit def sparkContextToADAMContext(sc: SparkContext): ADAMContext = new ADAMContext(sc)

  /**
   * Creates an ADAMContext from a SparkSession. Sets active session, including SQLContext, to input session.
   *
   * @param ss SparkSession
   * @return ADAMContext
   */
  def ADAMContextFromSession(ss: SparkSession): ADAMContext = {
    // this resets the sparkContext to read the session passed in.
    // 
    // this fixes the issue where if one sparkContext has already been started
    // in the scala backend, by replacing the started session with the provided
    // session
    SparkSession.setActiveSession(ss)
    new ADAMContext(ss.sparkContext)
  }

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
  private[rdd] def loadBamReadGroups(samHeader: SAMFileHeader): ReadGroupDictionary = {
    ReadGroupDictionary.fromSAMHeader(samHeader)
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
    val (sequences, samples, headerLines) = files.map(p => loadSingleVcfMetadata(p.toString)).reduce((p1, p2) => {
      (p1._1 ++ p2._1, p1._2 ++ p2._2, p1._3 ++ p2._3)
    })

    (sequences, samples.distinct, headerLines)
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
            .setId(s)
            .build()
        }).toSeq
      (sd, samples, VariantContextConverter.headerLines(vcfHeader))
    }

    headerToMetadata(readVcfHeader(pathName))
  }

  private def readVcfHeader(pathName: String): VCFHeader = {
    val is = WrapSeekable.openPath(sc.hadoopConfiguration,
      new Path(pathName))
    val header = VCFHeaderReader.readHeaderFrom(is)
    is.close()
    header
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
    getFsAndFilesWithFilter(pathName, new FileFilter("_processingSteps.avro"))
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
    getFsAndFilesWithFilter(pathName, new FileFilter("_references.avro"))
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
    val avroSd = loadAvro[Reference](pathName, Reference.SCHEMA$)
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
   * @param pathName The path name to load Avro read group dictionaries from.
   *   Globs/directories are supported.
   * @return Returns a ReadGroupDictionary.
   */
  private[rdd] def loadAvroReadGroupDictionary(pathName: String): ReadGroupDictionary = {
    getFsAndFilesWithFilter(pathName, new FileFilter("_readGroups.avro"))
      .map(p => loadSingleAvroReadGroupDictionary(p.toString))
      .reduce(_ ++ _)
  }

  /**
   * @see loadAvroReadGroupDictionary
   *
   * @param pathName The path name to load a single Avro read group dictionary from.
   *   Globs/directories are not supported.
   * @return Returns a ReadGroupDictionary.
   */
  private def loadSingleAvroReadGroupDictionary(pathName: String): ReadGroupDictionary = {
    val avroRgd = loadAvro[ReadGroupMetadata](pathName,
      ReadGroupMetadata.SCHEMA$)

    // convert avro to read group dictionary
    new ReadGroupDictionary(avroRgd.map(ReadGroup.fromAvro))
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
      if (paths.isEmpty) {
        throw new FileNotFoundException(
          s"Couldn't find any files matching ${path.toUri}, directory is empty"
        )
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
   * Load alignment records from BAM/CRAM/SAM into an AlignmentRecordDataset.
   *
   * This reads the sequence and read group dictionaries from the BAM/CRAM/SAM file
   * header. SAMRecords are read from the file and converted to the
   * AlignmentRecord schema.
   *
   * @param pathName The path name to load BAM/CRAM/SAM formatted alignment records from.
   *   Globs/directories are supported.
   * @param stringency The validation stringency to use when validating the
   *   BAM/CRAM/SAM format header. Defaults to ValidationStringency.STRICT.
   * @return Returns an AlignmentRecordDataset which wraps the genomic dataset of alignment records,
   *   sequence dictionary representing reference sequences the alignment records may be aligned to,
   *   and the read group dictionary for the alignment records if one is available.
   */
  def loadBam(
    pathName: String,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordDataset = LoadBam.time {

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

    AlignmentRecordDataset(records.map(p => samRecordConverter.convert(p._2.get)),
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
   * @return Returns an AlignmentRecordDataset which wraps the genomic dataset of alignment records,
   *   sequence dictionary representing reference sequences the alignment records may be aligned to,
   *   and the read group dictionary for the alignment records if one is available.
   */
  // todo: add stringency with default if possible
  def loadIndexedBam(
    pathName: String,
    viewRegion: ReferenceRegion): AlignmentRecordDataset = {
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
   * @return Returns an AlignmentRecordDataset which wraps the genomic dataset of alignment records,
   *   sequence dictionary representing reference sequences the alignment records may be aligned to,
   *   and the read group dictionary for the alignment records if one is available.
   */
  def loadIndexedBam(
    pathName: String,
    viewRegions: Iterable[ReferenceRegion],
    stringency: ValidationStringency = ValidationStringency.STRICT)(implicit s: DummyImplicit): AlignmentRecordDataset = LoadIndexedBam.time {

    val path = new Path(pathName)

    // If pathName is a single file or *.bam, append .bai to find all bam indices.
    // Otherwise, pathName is a directory and the entire path must be searched
    // for indices.
    val indexPath = if (pathName.endsWith(".bam")) {
      new Path(pathName.toString.replace(".bam", "*.bai"))
    } else {
      path
    }

    // currently only supports BAM files, see https://github.com/bigdatagenomics/adam/issues/1833
    val bamFiles = getFsAndFiles(path).filter(p => p.toString.endsWith(".bam"))

    val indexFiles = getFsAndFiles(indexPath).filter(p => p.toString.endsWith(".bai"))
      .map(r => r.toString)

    require(bamFiles.nonEmpty,
      "Did not find any BAM files at %s.".format(path))

    // look for index files named <pathname>.bam.bai and <pathname>.bai
    val missingIndices = bamFiles.filterNot(f => {
      indexFiles.contains(f.toString + ".bai") || indexFiles.contains(f.toString.dropRight(3) + "bai")
    })

    if (!missingIndices.isEmpty) {
      throw new FileNotFoundException("Missing indices for BAMs:\n%s".format(missingIndices.mkString("\n")))
    }

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
    AlignmentRecordDataset(records.map(p => samRecordConverter.convert(p._2.get)),
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
    // this function to load ReadGroupMetadata when creating a
    // ReadGroupDictionary.
    //
    // good news is, you can work around this by explicitly walking the iterator
    // and building a collection, which is what we do here. this would not be
    // efficient if we were loading a large amount of avro data (since we're
    // loading all the data into memory), but currently, we are just using this
    // code for building sequence/read group dictionaries, which are fairly
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

    try {
      val path = new Path(filename + "/_partitionMap.avro")
      val fs = path.getFileSystem(sc.hadoopConfiguration)

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
      case npe: NullPointerException => None
      case e: FileNotFoundException  => None
      case e: Throwable              => throw e
    }
  }

  /**
   * Load a path name in Parquet + Avro format into an AlignmentRecordDataset.
   *
   * @note The sequence dictionary is read from an Avro file stored at
   *   pathName/_references.avro and the read group dictionary is read from an
   *   Avro file stored at pathName/_readGroups.avro. These files are pure Avro,
   *   not Parquet + Avro.
   *
   * @param pathName The path name to load alignment records from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns an AlignmentRecordDataset which wraps the genomic dataset of alignment records,
   *   sequence dictionary representing reference sequences the alignment records may be aligned to,
   *   and the read group dictionary for the alignment records if one is available.
   */
  def loadParquetAlignments(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): AlignmentRecordDataset = {

    // convert avro to sequence dictionary
    val sd = loadAvroSequenceDictionary(pathName)

    // convert avro to read group dictionary
    val rgd = loadAvroReadGroupDictionary(pathName)

    // load processing step descriptions
    val pgs = loadAvroPrograms(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundAlignmentRecordDataset(sc, pathName, sd, rgd, pgs)
      }
      case (_, _) => {
        // load from disk
        val rdd = loadParquet[AlignmentRecord](pathName, optPredicate, optProjection)

        RDDBoundAlignmentRecordDataset(rdd, sd, rgd, pgs,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load a path name with range binned partitioned Parquet format into an AlignmentRecordDataset.
   *
   * @note The sequence dictionary is read from an Avro file stored at
   *   pathName/_references.avro and the read group dictionary is read from an
   *   Avro file stored at pathName/_readGroups.avro. These files are pure Avro,
   *   not Parquet + Avro.
   *
   * @param pathName The path name to load alignment records from.
   *   Globs/directories are supported.
   * @param regions Optional list of genomic regions to load.
   * @param optLookbackPartitions Number of partitions to lookback to find beginning of an overlapping
   *   region when using the filterByOverlappingRegions function on the returned dataset.
   *   Defaults to one partition.
   * @return Returns an AlignmentRecordDataset.
   */
  def loadPartitionedParquetAlignments(pathName: String,
                                       regions: Iterable[ReferenceRegion] = Iterable.empty,
                                       optLookbackPartitions: Option[Int] = Some(1)): AlignmentRecordDataset = {

    val partitionBinSize = getPartitionBinSize(pathName)
    val reads = loadParquetAlignments(pathName)
    val alignmentsDatasetBound = DatasetBoundAlignmentRecordDataset(reads.dataset,
      reads.sequences,
      reads.readGroups,
      reads.processingSteps,
      isPartitioned = true,
      Some(partitionBinSize),
      optLookbackPartitions
    )

    if (regions.nonEmpty) alignmentsDatasetBound.filterByOverlappingRegions(regions) else alignmentsDatasetBound
  }

  /**
   * Load unaligned alignment records from interleaved FASTQ into an AlignmentRecordDataset.
   *
   * In interleaved FASTQ, the two reads from a paired sequencing protocol are
   * interleaved in a single file. This is a zipped representation of the
   * typical paired FASTQ.
   *
   * @param pathName The path name to load unaligned alignment records from.
   *   Globs/directories are supported.
   * @return Returns an unaligned AlignmentRecordDataset.
   */
  def loadInterleavedFastq(
    pathName: String): AlignmentRecordDataset = LoadInterleavedFastq.time {

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
    AlignmentRecordDataset.unaligned(records.flatMap(fastqRecordConverter.convertPair))
  }

  /**
   * Load unaligned alignment records from (possibly paired) FASTQ into an AlignmentRecordDataset.
   *
   * @see loadPairedFastq
   * @see loadUnpairedFastq
   *
   * @param pathName1 The path name to load the first set of unaligned alignment records from.
   *   Globs/directories are supported.
   * @param optPathName2 The path name to load the second set of unaligned alignment records from,
   *   if provided. Globs/directories are supported.
   * @param optReadGroup The optional read group identifier to associate to the unaligned alignment
   *   records. Defaults to None.
   * @param stringency The validation stringency to use when validating (possibly paired) FASTQ format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns an unaligned AlignmentRecordDataset.
   */
  def loadFastq(
    pathName1: String,
    optPathName2: Option[String],
    optReadGroup: Option[String] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordDataset = LoadFastq.time {

    optPathName2.fold({
      loadUnpairedFastq(pathName1,
        optReadGroup = optReadGroup,
        stringency = stringency)
    })(filePath2 => {
      loadPairedFastq(pathName1,
        filePath2,
        optReadGroup = optReadGroup,
        stringency = stringency)
    })
  }

  /**
   * Load unaligned alignment records from paired FASTQ into an AlignmentRecordDataset.
   *
   * @param pathName1 The path name to load the first set of unaligned alignment records from.
   *   Globs/directories are supported.
   * @param pathName2 The path name to load the second set of unaligned alignment records from.
   *   Globs/directories are supported.
   * @param optReadGroup The optional read group identifier to associate to the unaligned alignment
   *   records. Defaults to None.
   * @param persistLevel An optional persistance level to set. If this level is
   *   set, then reads will be cached (at the given persistance) level as part of
   *   validation. Defaults to StorageLevel.MEMORY_ONLY.
   * @param stringency The validation stringency to use when validating paired FASTQ format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns an unaligned AlignmentRecordDataset.
   */
  def loadPairedFastq(
    pathName1: String,
    pathName2: String,
    optReadGroup: Option[String] = None,
    persistLevel: Option[StorageLevel] = Some(StorageLevel.MEMORY_ONLY),
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordDataset = LoadPairedFastq.time {

    val reads1 = loadUnpairedFastq(
      pathName1,
      setFirstOfPair = true,
      optReadGroup = optReadGroup,
      stringency = stringency
    )
    val reads2 = loadUnpairedFastq(
      pathName2,
      setSecondOfPair = true,
      optReadGroup = optReadGroup,
      stringency = stringency
    )

    stringency match {
      case ValidationStringency.STRICT | ValidationStringency.LENIENT =>
        val count1 = persistLevel.fold(reads1.rdd.count)(reads1.rdd.persist(_).count)
        val count2 = persistLevel.fold(reads2.rdd.count)(reads2.rdd.persist(_).count)

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

    AlignmentRecordDataset.unaligned(reads1.rdd ++ reads2.rdd)
  }

  /**
   * Load unaligned alignment records from unpaired FASTQ into an AlignmentRecordDataset.
   *
   * @param pathName The path name to load unaligned alignment records from.
   *   Globs/directories are supported.
   * @param setFirstOfPair If true, sets the unaligned alignment record as first from the fragment.
   *   Defaults to false.
   * @param setSecondOfPair If true, sets the unaligned alignment record as second from the fragment.
   *   Defaults to false.
   * @param optReadGroup The optional read group identifier to associate to the unaligned alignment
   *   records. Defaults to None.
   * @param stringency The validation stringency to use when validating unpaired FASTQ format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns an unaligned AlignmentRecordDataset.
   */
  def loadUnpairedFastq(
    pathName: String,
    setFirstOfPair: Boolean = false,
    setSecondOfPair: Boolean = false,
    optReadGroup: Option[String] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordDataset = LoadUnpairedFastq.time {

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
    AlignmentRecordDataset.unaligned(records.map(
      fastqRecordConverter.convertRead(
        _,
        optReadGroup.map(readGroup =>
          if (readGroup.isEmpty)
            pathName.substring(pathName.lastIndexOf("/") + 1)
          else
            readGroup),
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
   * Load variant context records from VCF into a VariantContextDataset.
   *
   * @param pathName The path name to load VCF variant context records from.
   *   Globs/directories are supported.
   * @param stringency The validation stringency to use when validating VCF format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a VariantContextDataset.
   */
  def loadVcf(
    pathName: String,
    stringency: ValidationStringency = ValidationStringency.STRICT): VariantContextDataset = LoadVcf.time {

    // load records from VCF
    val records = readVcfRecords(pathName, None)

    // attach instrumentation
    if (Metrics.isRecording) records.instrument() else records

    // load vcf metadata
    val (sd, samples, headers) = loadVcfMetadata(pathName)

    val vcc = VariantContextConverter(headers, stringency, sc.hadoopConfiguration)
    VariantContextDataset(records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples,
      VariantContextConverter.cleanAndMixInSupportedLines(headers, stringency, log))
  }

  /**
   * Load variant context records from VCF into a VariantContextDataset.
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
   * @return Returns a VariantContextDataset.
   */
  def loadVcfWithProjection(
    pathName: String,
    infoFields: Set[String],
    formatFields: Set[String],
    stringency: ValidationStringency = ValidationStringency.STRICT): VariantContextDataset = LoadVcf.time {

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
    VariantContextDataset(records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples,
      VariantContextConverter.cleanAndMixInSupportedLines(headers, stringency, log))
  }

  /**
   * Load variant context records from VCF indexed by tabix (tbi) into a VariantContextDataset.
   *
   * @param pathName The path name to load VCF variant context records from.
   *   Globs/directories are supported.
   * @param viewRegion ReferenceRegion we are filtering on.
   * @return Returns a VariantContextDataset.
   */
  // todo: add stringency with default if possible
  def loadIndexedVcf(
    pathName: String,
    viewRegion: ReferenceRegion): VariantContextDataset = {
    loadIndexedVcf(pathName, Iterable(viewRegion))
  }

  /**
   * Load variant context records from VCF indexed by tabix (tbi) into a VariantContextDataset.
   *
   * @param pathName The path name to load VCF variant context records from.
   *   Globs/directories are supported.
   * @param viewRegions Iterator of ReferenceRegions we are filtering on.
   * @param stringency The validation stringency to use when validating VCF format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a VariantContextDataset.
   */
  def loadIndexedVcf(
    pathName: String,
    viewRegions: Iterable[ReferenceRegion],
    stringency: ValidationStringency = ValidationStringency.STRICT)(implicit s: DummyImplicit): VariantContextDataset = LoadIndexedVcf.time {

    // load records from VCF
    val records = readVcfRecords(pathName, Some(viewRegions))

    // attach instrumentation
    if (Metrics.isRecording) records.instrument() else records

    // load vcf metadata
    val (sd, samples, headers) = loadVcfMetadata(pathName)

    val vcc = VariantContextConverter(headers, stringency, sc.hadoopConfiguration)
    VariantContextDataset(records.flatMap(p => vcc.convert(p._2.get)),
      sd,
      samples,
      VariantContextConverter.cleanAndMixInSupportedLines(headers, stringency, log))
  }

  /**
   * Load a path name in Parquet + Avro format into a GenotypeDataset.
   *
   * @param pathName The path name to load genotypes from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a GenotypeDataset.
   */
  def loadParquetGenotypes(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): GenotypeDataset = {

    // load header lines
    val headers = loadHeaderLines(pathName)

    // load sequence info
    val sd = loadAvroSequenceDictionary(pathName)

    // load avro read group dictionary and convert to samples
    val samples = loadAvroSamples(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundGenotypeDataset(sc, pathName, sd, samples, headers)
      }
      case (_, _) => {
        // load from disk
        val rdd = loadParquet[Genotype](pathName, optPredicate, optProjection)

        new RDDBoundGenotypeDataset(rdd, sd, samples, headers,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load a path name with range binned partitioned Parquet format into a GenotypeDataset.
   *
   * @param pathName The path name to load alignment records from.
   *   Globs/directories are supported.
   * @param regions Optional list of genomic regions to load.
   * @param optLookbackPartitions Number of partitions to lookback to find beginning of an overlapping
   *   region when using the filterByOverlappingRegions function on the returned dataset.
   *   Defaults to one partition.
   * @return Returns a GenotypeDataset.
   */
  def loadPartitionedParquetGenotypes(pathName: String,
                                      regions: Iterable[ReferenceRegion] = Iterable.empty,
                                      optLookbackPartitions: Option[Int] = Some(1)): GenotypeDataset = {

    val partitionedBinSize = getPartitionBinSize(pathName)
    val genotypes = loadParquetGenotypes(pathName)
    val genotypesDatasetBound = DatasetBoundGenotypeDataset(genotypes.dataset,
      genotypes.sequences,
      genotypes.samples,
      genotypes.headerLines,
      isPartitioned = true,
      Some(partitionedBinSize),
      optLookbackPartitions
    )

    if (regions.nonEmpty) genotypesDatasetBound.filterByOverlappingRegions(regions) else genotypesDatasetBound
  }

  /**
   * Load a path name in VCF or Parquet format into a VariantContextDataset.
   *
   * @param pathName The path name to load variant context records from.
   *   Globs/directories are supported.
   * @return Returns a VariantContextDataset.
   */
  def loadVariantContexts(
    pathName: String): VariantContextDataset = {

    if (isVcfExt(pathName)) {
      loadVcf(pathName)
    } else {
      loadParquetVariantContexts(pathName)
    }
  }

  /**
   * Load a path name in Parquet + Avro format into a VariantContextDataset.
   *
   * @param pathName The path name to load variant context records from.
   *   Globs/directories are supported.
   * @return Returns a VariantContextDataset.
   */
  def loadParquetVariantContexts(
    pathName: String): VariantContextDataset = {

    // load header lines
    val headers = loadHeaderLines(pathName)

    // load sequence info
    val sd = loadAvroSequenceDictionary(pathName)

    // load avro read group dictionary and convert to samples
    val samples = loadAvroSamples(pathName)

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val ds = sqlContext.read.parquet(pathName).as[VariantContextProduct]

    new DatasetBoundVariantContextDataset(ds, sd, samples, headers)
  }

  /**
   * Load a path name with range binned partitioned Parquet format into a VariantContextDataset.
   *
   * @param pathName The path name to load variant context records from.
   *   Globs/directories are supported.
   * @param regions Optional list of genomic regions to load.
   * @param optLookbackPartitions Number of partitions to lookback to find beginning of an overlapping
   *   region when using the filterByOverlappingRegions function on the returned dataset.
   *   Defaults to one partition.
   * @return Returns a VariantContextDataset.
   */
  def loadPartitionedParquetVariantContexts(pathName: String,
                                            regions: Iterable[ReferenceRegion] = Iterable.empty,
                                            optLookbackPartitions: Option[Int] = Some(1)): VariantContextDataset = {

    val partitionedBinSize = getPartitionBinSize(pathName)
    val variantContexts = loadParquetVariantContexts(pathName)
    val variantContextsDatasetBound = DatasetBoundVariantContextDataset(variantContexts.dataset,
      variantContexts.sequences,
      variantContexts.samples,
      variantContexts.headerLines,
      isPartitioned = true,
      Some(partitionedBinSize),
      optLookbackPartitions
    )

    if (regions.nonEmpty) variantContextsDatasetBound.filterByOverlappingRegions(regions) else variantContextsDatasetBound
  }

  /**
   * Load a path name in Parquet format into a VariantDataset.
   *
   * @param pathName The path name to load variants from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a VariantDataset.
   */
  def loadParquetVariants(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): VariantDataset = {

    val sd = loadAvroSequenceDictionary(pathName)

    // load header lines
    val headers = loadHeaderLines(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        new ParquetUnboundVariantDataset(sc, pathName, sd, headers)
      }
      case _ => {
        val rdd = loadParquet[Variant](pathName, optPredicate, optProjection)
        new RDDBoundVariantDataset(rdd, sd, headers,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load a path name with range binned partitioned Parquet format into a VariantDataset.
   *
   * @param pathName The path name to load alignment records from.
   *   Globs/directories are supported.
   * @param regions Optional list of genomic regions to load.
   * @param optLookbackPartitions Number of partitions to lookback to find beginning of an overlapping
   *   region when using the filterByOverlappingRegions function on the returned dataset.
   *   Defaults to one partition.
   * @return Returns a VariantDataset.
   */
  def loadPartitionedParquetVariants(pathName: String,
                                     regions: Iterable[ReferenceRegion] = Iterable.empty,
                                     optLookbackPartitions: Option[Int] = Some(1)): VariantDataset = {

    val partitionedBinSize = getPartitionBinSize(pathName)
    val variants = loadParquetVariants(pathName)
    val variantsDatasetBound = DatasetBoundVariantDataset(variants.dataset,
      variants.sequences,
      variants.headerLines,
      isPartitioned = true,
      Some(partitionedBinSize),
      optLookbackPartitions
    )

    if (regions.nonEmpty) variantsDatasetBound.filterByOverlappingRegions(regions) else variantsDatasetBound
  }

  /**
   * Load nucleotide contig fragments from FASTA into a NucleotideContigFragmentDataset.
   *
   * @param pathName The path name to load nucleotide contig fragments from.
   *   Globs/directories are supported.
   * @param maximumLength Maximum fragment length. Defaults to 10000L. Values greater
   *   than 1e9 should be avoided.
   * @return Returns a NucleotideContigFragmentDataset.
   */
  def loadFasta(
    pathName: String,
    maximumLength: Long = 10000L): NucleotideContigFragmentDataset = LoadFasta.time {

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

    NucleotideContigFragmentDataset(fragmentRdd)
  }

  /**
   * Load paired unaligned alignment records grouped by sequencing fragment
   * from interleaved FASTQ into an FragmentDataset.
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
   * @return Returns a FragmentDataset containing the paired reads grouped by
   *   sequencing fragment.
   */
  def loadInterleavedFastqAsFragments(
    pathName: String): FragmentDataset = LoadInterleavedFastqFragments.time {

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
    FragmentDataset.fromRdd(records.map(fastqRecordConverter.convertFragment))
  }

  /**
   * Load paired unaligned alignment records grouped by sequencing fragment
   * from paired FASTQ files into an FragmentDataset.
   *
   * Fragments represent all of the reads from a single sequenced fragment as
   * a single object, which is a useful representation for some tasks.
   *
   * @param pathName1 The path name to load the first set of unaligned alignment records from.
   *   Globs/directories are supported.
   * @param pathName2 The path name to load the second set of unaligned alignment records from.
   *   Globs/directories are supported.
   * @param optReadGroup The optional read group identifier to associate to the unaligned alignment
   *   records. Defaults to None.
   * @param persistLevel An optional persistance level to set. If this level is
   *   set, then reads will be cached (at the given persistance) level as part of
   *   validation. Defaults to StorageLevel.MEMORY_ONLY.
   * @param stringency The validation stringency to use when validating paired FASTQ format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FragmentDataset containing the paired reads grouped by
   *   sequencing fragment.
   */
  def loadPairedFastqAsFragments(
    pathName1: String,
    pathName2: String,
    optReadGroup: Option[String] = None,
    persistLevel: Option[StorageLevel] = Some(StorageLevel.MEMORY_ONLY),
    stringency: ValidationStringency = ValidationStringency.STRICT): FragmentDataset = LoadPairedFastqFragments.time {

    loadPairedFastq(pathName1, pathName2, optReadGroup, persistLevel, stringency).toFragments()
  }

  /**
   * Load features into a FeatureDataset and convert to a CoverageDataset.
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
   * @return Returns a FeatureDataset converted to a CoverageDataset.
   */
  def loadCoverage(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): CoverageDataset = LoadCoverage.time {

    loadFeatures(pathName,
      optSequenceDictionary = optSequenceDictionary,
      optMinPartitions = optMinPartitions,
      optPredicate = optPredicate,
      optProjection = optProjection,
      stringency = stringency).toCoverage
  }

  /**
   * Load a path name in Parquet + Avro format into a FeatureDataset and convert to a CoverageDataset.
   * Coverage is stored in the score field of Feature.
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param forceRdd Forces loading the RDD.
   * @return Returns a FeatureDataset converted to a CoverageDataset.
   */
  def loadParquetCoverage(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    forceRdd: Boolean = false): CoverageDataset = {

    if (optPredicate.isEmpty && !forceRdd) {
      // convert avro to sequence dictionary
      val sd = loadAvroSequenceDictionary(pathName)
      val samples = loadAvroSamples(pathName)

      new ParquetUnboundCoverageDataset(sc, pathName, sd, samples)
    } else {
      val coverageFields = Projection(FeatureField.referenceName,
        FeatureField.start,
        FeatureField.end,
        FeatureField.score,
        FeatureField.sampleId)
      loadParquetFeatures(pathName,
        optPredicate = optPredicate,
        optProjection = Some(coverageFields))
        .toCoverage
    }
  }

  /**
   * Load a path name in GFF3 format into a FeatureDataset.
   *
   * @param pathName The path name to load features in GFF3 format from.
   *   Globs/directories are supported.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating GFF3 format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureDataset.
   */
  def loadGff3(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureDataset = LoadGff3.time {

    val records = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new GFF3Parser().parse(_, stringency))
    if (Metrics.isRecording) records.instrument() else records

    optSequenceDictionary
      .fold(FeatureDataset(records))(FeatureDataset(records, _, Seq.empty))
  }

  /**
   * Load a path name in GTF/GFF2 format into a FeatureDataset.
   *
   * @param pathName The path name to load features in GTF/GFF2 format from.
   *   Globs/directories are supported.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating GTF/GFF2 format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureDataset.
   */
  def loadGtf(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureDataset = LoadGtf.time {

    val records = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new GTFParser().parse(_, stringency))
    if (Metrics.isRecording) records.instrument() else records

    optSequenceDictionary
      .fold(FeatureDataset(records))(FeatureDataset(records, _, Seq.empty))
  }

  /**
   * Load a path name in BED6/12 format into a FeatureDataset.
   *
   * @param pathName The path name to load features in BED6/12 format from.
   *   Globs/directories are supported.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating BED6/12 format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureDataset.
   */
  def loadBed(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureDataset = LoadBed.time {

    val records = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new BEDParser().parse(_, stringency))
    if (Metrics.isRecording) records.instrument() else records

    optSequenceDictionary
      .fold(FeatureDataset(records))(FeatureDataset(records, _, Seq.empty))
  }

  /**
   * Load a path name in NarrowPeak format into a FeatureDataset.
   *
   * @param pathName The path name to load features in NarrowPeak format from.
   *   Globs/directories are supported.
   * @param optSequenceDictionary Optional sequence dictionary. Defaults to None.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating NarrowPeak format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureDataset.
   */
  def loadNarrowPeak(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureDataset = LoadNarrowPeak.time {

    val records = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .flatMap(new NarrowPeakParser().parse(_, stringency))
    if (Metrics.isRecording) records.instrument() else records

    optSequenceDictionary
      .fold(FeatureDataset(records))(FeatureDataset(records, _, Seq.empty))
  }

  /**
   * Load a path name in IntervalList format into a FeatureDataset.
   *
   * @param pathName The path name to load features in IntervalList format from.
   *   Globs/directories are supported.
   * @param optMinPartitions An optional minimum number of partitions to load. If
   *   not set, falls back to the configured Spark default parallelism. Defaults to None.
   * @param stringency The validation stringency to use when validating IntervalList format.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns a FeatureDataset.
   */
  def loadIntervalList(
    pathName: String,
    optMinPartitions: Option[Int] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureDataset = LoadIntervalList.time {

    val parsedLines = sc.textFile(pathName, optMinPartitions.getOrElse(sc.defaultParallelism))
      .map(new IntervalListParser().parseWithHeader(_, stringency))
    val (seqDict, records) = (SequenceDictionary(parsedLines.flatMap(_._1).collect(): _*),
      parsedLines.flatMap(_._2))

    if (Metrics.isRecording) records.instrument() else records
    FeatureDataset(records, seqDict, Seq.empty)
  }

  /**
   * Load a path name in Parquet + Avro format into a FeatureDataset.
   *
   * @param pathName The path name to load features from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a FeatureDataset.
   */
  def loadParquetFeatures(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): FeatureDataset = {

    val sd = loadAvroSequenceDictionary(pathName)
    val samples = loadAvroSamples(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundFeatureDataset(sc, pathName, sd, samples)
      }
      case (_, _) => {
        // load from disk
        val rdd = loadParquet[Feature](pathName, optPredicate, optProjection)
        new RDDBoundFeatureDataset(rdd, sd, samples, optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load a path name with range binned partitioned Parquet format into a FeatureDataset.
   *
   * @param pathName The path name to load alignment records from.
   *   Globs/directories are supported.
   * @param regions Optional list of genomic regions to load.
   * @param optLookbackPartitions Number of partitions to lookback to find beginning of an overlapping
   *   region when using the filterByOverlappingRegions function on the returned dataset.
   *   Defaults to one partition.
   * @return Returns a FeatureDataset.
   */
  def loadPartitionedParquetFeatures(pathName: String,
                                     regions: Iterable[ReferenceRegion] = Iterable.empty,
                                     optLookbackPartitions: Option[Int] = Some(1)): FeatureDataset = {

    val partitionedBinSize = getPartitionBinSize(pathName)
    val features = loadParquetFeatures(pathName)
    val featureDatasetBound = DatasetBoundFeatureDataset(features.dataset,
      features.sequences,
      features.samples,
      isPartitioned = true,
      Some(partitionedBinSize),
      optLookbackPartitions
    )

    if (regions.nonEmpty) featureDatasetBound.filterByOverlappingRegions(regions) else featureDatasetBound
  }

  /**
   * Load a path name in Parquet + Avro format into a NucleotideContigFragmentDataset.
   *
   * @param pathName The path name to load nucleotide contig fragments from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a NucleotideContigFragmentDataset.
   */
  def loadParquetContigFragments(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): NucleotideContigFragmentDataset = {

    val sd = loadAvroSequenceDictionary(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundNucleotideContigFragmentDataset(
          sc, pathName, sd)
      }
      case (_, _) => {
        val rdd = loadParquet[NucleotideContigFragment](pathName, optPredicate, optProjection)
        new RDDBoundNucleotideContigFragmentDataset(rdd,
          sd,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load a path name with range binned partitioned Parquet format into a NucleotideContigFragmentDataset.
   *
   * @param pathName The path name to load alignment records from.
   *   Globs/directories are supported.
   * @param regions Optional list of genomic regions to load.
   * @param optLookbackPartitions Number of partitions to lookback to find beginning of an overlapping
   *   region when using the filterByOverlappingRegions function on the returned dataset.
   *   Defaults to one partition.
   * @return Returns a NucleotideContigFragmentDataset.
   */
  def loadPartitionedParquetContigFragments(pathName: String,
                                            regions: Iterable[ReferenceRegion] = Iterable.empty,
                                            optLookbackPartitions: Option[Int] = Some(1)): NucleotideContigFragmentDataset = {

    val partitionedBinSize = getPartitionBinSize(pathName)
    val contigs = loadParquetContigFragments(pathName)
    val contigsDatasetBound = DatasetBoundNucleotideContigFragmentDataset(contigs.dataset,
      contigs.sequences,
      isPartitioned = true,
      Some(partitionedBinSize),
      optLookbackPartitions
    )

    if (regions.nonEmpty) contigsDatasetBound.filterByOverlappingRegions(regions) else contigsDatasetBound
  }

  /**
   * Load a path name in Parquet + Avro format into a FragmentDataset.
   *
   * @param pathName The path name to load fragments from.
   *   Globs/directories are supported.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @return Returns a FragmentDataset.
   */
  def loadParquetFragments(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): FragmentDataset = {

    // convert avro to sequence dictionary
    val sd = loadAvroSequenceDictionary(pathName)

    // convert avro to read group dictionary
    val rgd = loadAvroReadGroupDictionary(pathName)

    // load processing step descriptions
    val pgs = loadAvroPrograms(pathName)

    (optPredicate, optProjection) match {
      case (None, None) => {
        ParquetUnboundFragmentDataset(sc, pathName, sd, rgd, pgs)
      }
      case (_, _) => {
        // load from disk
        val rdd = loadParquet[Fragment](pathName, optPredicate, optProjection)

        new RDDBoundFragmentDataset(rdd,
          sd,
          rgd,
          pgs,
          optPartitionMap = extractPartitionMap(pathName))
      }
    }
  }

  /**
   * Load features into a FeatureDataset.
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
   * @return Returns a FeatureDataset.
   */
  def loadFeatures(
    pathName: String,
    optSequenceDictionary: Option[SequenceDictionary] = None,
    optMinPartitions: Option[Int] = None,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FeatureDataset = LoadFeatures.time {

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
   * Load nucleotide contig fragments into a NucleotideContigFragmentDataset.
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
   * @return Returns a NucleotideContigFragmentDataset.
   */
  def loadContigFragments(
    pathName: String,
    maximumLength: Long = 10000L,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None): NucleotideContigFragmentDataset = LoadContigFragments.time {

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
   * Load genotypes into a GenotypeDataset.
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
   * @return Returns a GenotypeDataset.
   */
  def loadGenotypes(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): GenotypeDataset = LoadGenotypes.time {

    if (isVcfExt(pathName)) {
      log.info(s"Loading $pathName as VCF and converting to Genotypes.")
      loadVcf(pathName, stringency).toGenotypes
    } else {
      log.info(s"Loading $pathName as Parquet containing Genotypes. Sequence dictionary for translation is ignored.")
      loadParquetGenotypes(pathName, optPredicate = optPredicate, optProjection = optProjection)
    }
  }

  /**
   * Load variants into a VariantDataset.
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
   * @return Returns a VariantDataset.
   */
  def loadVariants(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): VariantDataset = LoadVariants.time {

    if (isVcfExt(pathName)) {
      log.info(s"Loading $pathName as VCF and converting to Variants.")
      loadVcf(pathName, stringency).toVariants
    } else {
      log.info(s"Loading $pathName as Parquet containing Variants. Sequence dictionary for translation is ignored.")
      loadParquetVariants(pathName, optPredicate = optPredicate, optProjection = optProjection)
    }
  }

  /**
   * Load alignment records into an AlignmentRecordDataset.
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
   * @param optReadGroup The optional read group identifier to associate to the alignment
   *   records. Defaults to None.
   * @param optPredicate An optional pushdown predicate to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param optProjection An option projection schema to use when reading Parquet + Avro.
   *   Defaults to None.
   * @param stringency The validation stringency to use when validating BAM/CRAM/SAM or FASTQ formats.
   *   Defaults to ValidationStringency.STRICT.
   * @return Returns an AlignmentRecordDataset which wraps the genomic dataset of alignment records,
   *   sequence dictionary representing reference sequences the alignment records may be aligned to,
   *   and the read group dictionary for the alignment records if one is available.
   */
  def loadAlignments(
    pathName: String,
    optPathName2: Option[String] = None,
    optReadGroup: Option[String] = None,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecordDataset = LoadAlignments.time {

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
      loadFastq(pathName, optPathName2, optReadGroup, stringency)
    } else if (isFastaExt(trimmedPathName)) {
      log.info(s"Loading $pathName as FASTA and converting to AlignmentRecords.")
      AlignmentRecordDataset.unaligned(loadFasta(pathName, maximumLength = 10000L).toReads)
    } else {
      log.info(s"Loading $pathName as Parquet of AlignmentRecords.")
      loadParquetAlignments(pathName, optPredicate = optPredicate, optProjection = optProjection)
    }
  }

  /**
   * Load fragments into a FragmentDataset.
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
   * @return Returns a FragmentDataset.
   */
  def loadFragments(
    pathName: String,
    optPredicate: Option[FilterPredicate] = None,
    optProjection: Option[Schema] = None,
    stringency: ValidationStringency = ValidationStringency.STRICT): FragmentDataset = LoadFragments.time {

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

  /**
   * Return length of partitions in base pairs if the specified path of Parquet + Avro files is partitioned.
   *
   * @param pathName Path in which to look for partitioned flag.
   * @return Return length of partitions in base pairs if file is partitioned
   *
   * If a glob is used, all directories within the blog must be partitioned, and must have been saved
   * using the same partitioned bin size.  Behavior is undefined if this requirement is not met.
   */
  private def getPartitionBinSize(pathName: String): Int = {

    val partitionSizes = getFsAndFilesWithFilter(pathName, new FileFilter("_partitionedByStartPos")).map(f => {
      val is = f.getFileSystem(sc.hadoopConfiguration).open(f)
      val partitionSize = is.readInt
      is.close()
      partitionSize
    }).toSet

    require(partitionSizes.nonEmpty, "Input Parquet files (%s) are not partitioned.".format(pathName))
    require(partitionSizes.size == 1, "Found multiple partition sizes (%s).".format(partitionSizes.mkString(", ")))
    partitionSizes.head
  }

  /**
   * Return true if the specified path of Parquet + Avro files is partitioned.
   *
   * @param pathName Path in which to look for partitioned flag.
   * @return Return true if the specified path of Parquet + Avro files is partitioned.
   * Behavior is undefined if some paths in glob contain _partitionedByStartPos flag file and some do not.
   */
  def isPartitioned(pathName: String): Boolean = {
    try {
      getPartitionBinSize(pathName)
      true
    } catch {
      case e: FileNotFoundException => false
    }
  }
}
