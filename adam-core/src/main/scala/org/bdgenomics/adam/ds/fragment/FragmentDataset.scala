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
package org.bdgenomics.adam.ds.fragment

import grizzled.slf4j.Logging
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.bdgenomics.adam.converters.AlignmentConverter
import org.bdgenomics.adam.models.{
  ReadGroupDictionary,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.{
  DatasetBoundGenomicDataset,
  AvroReadGroupGenomicDataset,
  JavaSaveArgs
}
import org.bdgenomics.adam.ds.read.{
  AlignmentDataset,
  BinQualities,
  DatasetBoundAlignmentDataset,
  MarkDuplicates,
  QualityScoreBin
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{
  Fragment => FragmentProduct,
  Alignment => AlignmentProduct
}
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class FragmentArray(
    array: Array[(ReferenceRegion, Fragment)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Fragment] {

  def duplicate(): IntervalArray[ReferenceRegion, Fragment] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Fragment)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Fragment] = {
    FragmentArray(arr, maxWidth)
  }
}

private[adam] class FragmentArraySerializer extends IntervalArraySerializer[ReferenceRegion, Fragment, FragmentArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Fragment]

  protected def builder(arr: Array[(ReferenceRegion, Fragment)],
                        maxIntervalWidth: Long): FragmentArray = {
    FragmentArray(arr, maxIntervalWidth)
  }
}

/**
 * Helper singleton object for building FragmentDatasets.
 */
object FragmentDataset {

  /**
   * Hadoop configuration path to check for a boolean value indicating whether
   * the current or original read quality scores should be written. True indicates
   * to write the original quality scores. The default is false.
   */
  val WRITE_ORIGINAL_QUALITY_SCORES = "org.bdgenomics.adam.rdd.fragment.FragmentDataset.writeOriginalQualityScores"

  /**
   * Hadoop configuration path to check for a boolean value indicating whether
   * to write the "/1" "/2" suffixes to the read name that indicate whether a
   * read is first or second in a pair. Default is false (no suffixes).
   */
  val WRITE_SUFFIXES = "org.bdgenomics.adam.rdd.fragment.FragmentDataset.writeSuffixes"

  /**
   * Creates a FragmentDataset where no read groups or sequence info are attached.
   *
   * @param rdd RDD of fragments.
   * @return Returns a FragmentDataset with an empty read group dictionary and sequence dictionary.
   */
  private[adam] def fromRdd(rdd: RDD[Fragment]): FragmentDataset = {
    FragmentDataset(rdd,
      SequenceDictionary.empty,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  /**
   * Builds a FragmentDataset without a partition map.
   *
   * @param rdd The underlying Fragment RDD.
   * @param sequences The sequence dictionary for the genomic dataset.
   * @param readGroups The read group dictionary for the genomic dataset.
   * @param processingSteps The processing steps that have been applied to this data.
   * @return A new FragmentDataset.
   */
  def apply(rdd: RDD[Fragment],
            sequences: SequenceDictionary,
            readGroups: ReadGroupDictionary,
            processingSteps: Seq[ProcessingStep]): FragmentDataset = {

    new RDDBoundFragmentDataset(rdd,
      sequences,
      readGroups,
      processingSteps,
      None)
  }

  /**
   * A genomic dataset that supports Datasets of Fragments.
   *
   * @param ds The underlying Dataset of Fragment data.
   * @return A new FragmentDataset.
   */
  def apply(ds: Dataset[FragmentProduct]): FragmentDataset = {
    FragmentDataset(ds,
      SequenceDictionary.empty,
      ReadGroupDictionary.empty,
      Seq.empty)
  }

  /**
   * A genomic dataset that supports Datasets of Fragments.
   *
   * @param ds The underlying Dataset of Fragment data.
   * @param sequences The genomic sequences this data was aligned to, if any.
   * @param readGroups The read groups these Fragments came from.
   * @param processingSteps The processing steps that have been applied to this data.
   * @return A new FragmentDataset.
   */
  def apply(ds: Dataset[FragmentProduct],
            sequences: SequenceDictionary,
            readGroups: ReadGroupDictionary,
            processingSteps: Seq[ProcessingStep]): FragmentDataset = {
    DatasetBoundFragmentDataset(ds, sequences, readGroups, processingSteps)
  }
}

case class ParquetUnboundFragmentDataset private[ds] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    references: SequenceDictionary,
    readGroups: ReadGroupDictionary,
    @transient val processingSteps: Seq[ProcessingStep]) extends FragmentDataset {

  lazy val rdd: RDD[Fragment] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    import spark.implicits._
    spark.read.parquet(parquetFilename).as[FragmentProduct]
  }

  def replaceReferences(
    newReferences: SequenceDictionary): FragmentDataset = {
    copy(references = newReferences)
  }

  def replaceReadGroups(newReadGroups: ReadGroupDictionary): FragmentDataset = {
    copy(readGroups = newReadGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): FragmentDataset = {
    copy(processingSteps = newProcessingSteps)
  }
}

case class DatasetBoundFragmentDataset private[ds] (
  dataset: Dataset[FragmentProduct],
  references: SequenceDictionary,
  readGroups: ReadGroupDictionary,
  @transient val processingSteps: Seq[ProcessingStep],
  override val isPartitioned: Boolean = false,
  override val optPartitionBinSize: Option[Int] = None,
  override val optLookbackPartitions: Option[Int] = None) extends FragmentDataset
    with DatasetBoundGenomicDataset[Fragment, FragmentProduct, FragmentDataset] {

  lazy val rdd = dataset.rdd.map(_.toAvro)

  protected lazy val optPartitionMap = None

  override def saveAsParquet(filePath: String,
                             blockSize: Int = 128 * 1024 * 1024,
                             pageSize: Int = 1 * 1024 * 1024,
                             compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                             disableDictionaryEncoding: Boolean = false) {
    info("Saving directly as Parquet from SQL. Options other than compression codec are ignored.")
    dataset.toDF()
      .write
      .format("parquet")
      .option("spark.sql.parquet.compression.codec", compressionCodec.toString.toLowerCase())
      .save(filePath)
    saveMetadata(filePath)
  }

  override def transformDataset(
    tFn: Dataset[FragmentProduct] => Dataset[FragmentProduct]): FragmentDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[FragmentProduct], Dataset[FragmentProduct]]): FragmentDataset = {
    copy(dataset = tFn.call(dataset))
  }

  override def toAlignments(): AlignmentDataset = {
    import dataset.sparkSession.implicits._
    val df = dataset.select(explode(col("alignments")).as("rec")).select("rec.*")
    DatasetBoundAlignmentDataset(df.as[AlignmentProduct],
      references,
      readGroups,
      processingSteps)
  }

  def replaceReferences(
    newReferences: SequenceDictionary): FragmentDataset = {
    copy(references = newReferences)
  }

  def replaceReadGroups(newReadGroups: ReadGroupDictionary): FragmentDataset = {
    copy(readGroups = newReadGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): FragmentDataset = {
    copy(processingSteps = newProcessingSteps)
  }
}

case class RDDBoundFragmentDataset private[ds] (
    rdd: RDD[Fragment],
    references: SequenceDictionary,
    readGroups: ReadGroupDictionary,
    @transient val processingSteps: Seq[ProcessingStep],
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends FragmentDataset {

  /**
   * A SQL Dataset of fragments.
   */
  lazy val dataset: Dataset[FragmentProduct] = {
    import spark.implicits._
    spark.createDataset(rdd.map(FragmentProduct.fromAvro))
  }

  def replaceReferences(
    newReferences: SequenceDictionary): FragmentDataset = {
    copy(references = newReferences)
  }

  def replaceReadGroups(newReadGroups: ReadGroupDictionary): FragmentDataset = {
    copy(readGroups = newReadGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): FragmentDataset = {
    copy(processingSteps = newProcessingSteps)
  }
}

sealed abstract class FragmentDataset extends AvroReadGroupGenomicDataset[Fragment, FragmentProduct, FragmentDataset] {

  protected val productFn = FragmentProduct.fromAvro(_)
  protected val unproductFn = (f: FragmentProduct) => f.toAvro

  @transient val uTag: TypeTag[FragmentProduct] = typeTag[FragmentProduct]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Fragment)])(
    implicit tTag: ClassTag[Fragment]): IntervalArray[ReferenceRegion, Fragment] = {
    IntervalArray(rdd, FragmentArray.apply(_, _))
  }

  /**
   * Replaces the underlying RDD with a new RDD.
   *
   * @param newRdd The RDD to replace our underlying RDD with.
   * @return Returns a new FragmentDataset where the underlying RDD has been
   *   swapped out.
   */
  protected def replaceRdd(newRdd: RDD[Fragment],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): FragmentDataset = {
    RDDBoundFragmentDataset(newRdd,
      references,
      readGroups,
      processingSteps,
      newPartitionMap)
  }

  def union(datasets: FragmentDataset*): FragmentDataset = {
    val iterableDatasets = datasets.toSeq
    FragmentDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.references).fold(references)(_ ++ _),
      iterableDatasets.map(_.readGroups).fold(readGroups)(_ ++ _),
      iterableDatasets.map(_.processingSteps).fold(processingSteps)(_ ++ _))
  }

  override def transformDataset(
    tFn: Dataset[FragmentProduct] => Dataset[FragmentProduct]): FragmentDataset = {
    DatasetBoundFragmentDataset(tFn(dataset),
      references,
      readGroups,
      processingSteps)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[FragmentProduct], Dataset[FragmentProduct]]): FragmentDataset = {
    DatasetBoundFragmentDataset(tFn.call(dataset),
      references,
      readGroups,
      processingSteps)
  }

  /**
   * Splits up the reads in a Fragment back into alignments.
   *
   * @return Returns this genomic dataset converted to alignments.
   */
  def toAlignments(): AlignmentDataset = {
    val converter = new AlignmentConverter

    // convert the fragments to alignments
    val newRdd = rdd.flatMap(converter.convertFragment)

    AlignmentDataset(newRdd,
      references,
      readGroups,
      processingSteps)
  }

  /**
   * Marks reads as possible fragment duplicates.
   *
   * @return A new genomic dataset where reads have the duplicate read flag set. Duplicate
   *   reads are NOT filtered out.
   */
  def markDuplicates(): FragmentDataset = {
    replaceRdd(MarkDuplicates(this))
  }

  /**
   * Saves Fragments to Parquet.
   *
   * @param filePath Path to save fragments at.
   */
  def save(filePath: java.lang.String) {
    saveAsParquet(new JavaSaveArgs(filePath))
  }

  /**
   * (Java-specific) Rewrites the quality scores of fragments to place all quality scores in bins.
   *
   * Quality score binning maps all quality scores to a limited number of
   * discrete values, thus reducing the entropy of the quality score
   * distribution, and reducing the amount of space that fragments consume on disk.
   *
   * @param bins The bins to use.
   * @return Fragments whose quality scores are binned.
   */
  def binQualityScores(bins: java.util.List[QualityScoreBin]): FragmentDataset = {
    binQualityScores(bins.asScala)
  }

  /**
   * (Scala-specific) Rewrites the quality scores of fragments to place all quality scores in bins.
   *
   * Quality score binning maps all quality scores to a limited number of
   * discrete values, thus reducing the entropy of the quality score
   * distribution, and reducing the amount of space that fragments consume on disk.
   *
   * @param bins The bins to use.
   * @return Fragments whose quality scores are binned.
   */
  def binQualityScores(bins: Seq[QualityScoreBin]): FragmentDataset = {
    AlignmentDataset.validateBins(bins)
    BinQualities(this, bins)
  }

  /**
   * Returns the regions that this fragment covers.
   *
   * Since a fragment may be chimeric or multi-mapped, we do not try to compute
   * the hull of the underlying element.
   *
   * @param elem The Fragment to get the region from.
   * @return Returns all regions covered by this fragment.
   */
  protected def getReferenceRegions(elem: Fragment): Seq[ReferenceRegion] = {
    elem.getAlignments
      .asScala
      .flatMap(r => ReferenceRegion.opt(r))
      .toSeq
  }
}
