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
package org.bdgenomics.adam.rdd.fragment

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{
  DatasetBoundGenomicDataset,
  AvroRecordGroupGenomicDataset,
  JavaSaveArgs
}
import org.bdgenomics.adam.rdd.read.{
  AlignmentRecordRDD,
  BinQualities,
  MarkDuplicates,
  QualityScoreBin
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Fragment => FragmentProduct }
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._
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
 * Helper singleton object for building FragmentRDDs.
 */
object FragmentRDD {

  /**
   * Hadoop configuration path to check for a boolean value indicating whether
   * the current or original read qualities should be written. True indicates
   * to write the original qualities. The default is false.
   */
  val WRITE_ORIGINAL_QUALITIES = "org.bdgenomics.adam.rdd.fragment.FragmentRDD.writeOriginalQualities"

  /**
   * Hadoop configuration path to check for a boolean value indicating whether
   * to write the "/1" "/2" suffixes to the read name that indicate whether a
   * read is first or second in a pair. Default is false (no suffixes).
   */
  val WRITE_SUFFIXES = "org.bdgenomics.adam.rdd.fragment.FragmentRDD.writeSuffixes"

  /**
   * Creates a FragmentRDD where no record groups or sequence info are attached.
   *
   * @param rdd RDD of fragments.
   * @return Returns a FragmentRDD with an empty record group dictionary and sequence dictionary.
   */
  private[adam] def fromRdd(rdd: RDD[Fragment]): FragmentRDD = {
    FragmentRDD(rdd,
      SequenceDictionary.empty,
      RecordGroupDictionary.empty,
      Seq.empty)
  }

  /**
   * Builds a FragmentRDD without a partition map.
   *
   * @param rdd The underlying Franment RDD.
   * @param sequences The sequence dictionary for the RDD.
   * @param recordGroupDictionary The record group dictionary for the RDD.
   * @param processingSteps The processing steps that have been applied to this data.
   * @return A new FragmentRDD.
   */
  def apply(rdd: RDD[Fragment],
            sequences: SequenceDictionary,
            recordGroupDictionary: RecordGroupDictionary,
            processingSteps: Seq[ProcessingStep]): FragmentRDD = {

    new RDDBoundFragmentRDD(rdd,
      sequences,
      recordGroupDictionary,
      processingSteps,
      None)
  }

  /**
   * A genomic RDD that supports Datasets of Fragments.
   *
   * @param ds The underlying Dataset of Fragment data.
   * @param sequences The genomic sequences this data was aligned to, if any.
   * @param recordGroups The record groups these Fragments came from.
   * @param processingSteps The processing steps that have been applied to this data.
   * @return A new FragmentRDD.
   */
  def apply(ds: Dataset[FragmentProduct],
            sequences: SequenceDictionary,
            recordGroups: RecordGroupDictionary,
            processingSteps: Seq[ProcessingStep]): FragmentRDD = {
    DatasetBoundFragmentRDD(ds, sequences, recordGroups, processingSteps)
  }
}

case class ParquetUnboundFragmentRDD private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary,
    recordGroups: RecordGroupDictionary,
    @transient val processingSteps: Seq[ProcessingStep]) extends FragmentRDD {

  lazy val rdd: RDD[Fragment] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[FragmentProduct]
  }

  def replaceSequences(
    newSequences: SequenceDictionary): FragmentRDD = {
    copy(sequences = newSequences)
  }

  def replaceRecordGroups(
    newRecordGroups: RecordGroupDictionary): FragmentRDD = {
    copy(recordGroups = newRecordGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): FragmentRDD = {
    copy(processingSteps = newProcessingSteps)
  }
}

case class DatasetBoundFragmentRDD private[rdd] (
  dataset: Dataset[FragmentProduct],
  sequences: SequenceDictionary,
  recordGroups: RecordGroupDictionary,
  @transient val processingSteps: Seq[ProcessingStep],
  override val isPartitioned: Boolean = false,
  override val optPartitionBinSize: Option[Int] = None,
  override val optLookbackPartitions: Option[Int] = None) extends FragmentRDD
    with DatasetBoundGenomicDataset[Fragment, FragmentProduct, FragmentRDD] {

  lazy val rdd = dataset.rdd.map(_.toAvro)

  protected lazy val optPartitionMap = None

  override def saveAsParquet(filePath: String,
                             blockSize: Int = 128 * 1024 * 1024,
                             pageSize: Int = 1 * 1024 * 1024,
                             compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
                             disableDictionaryEncoding: Boolean = false) {
    log.info("Saving directly as Parquet from SQL. Options other than compression codec are ignored.")
    dataset.toDF()
      .write
      .format("parquet")
      .option("spark.sql.parquet.compression.codec", compressCodec.toString.toLowerCase())
      .save(filePath)
    saveMetadata(filePath)
  }

  override def transformDataset(
    tFn: Dataset[FragmentProduct] => Dataset[FragmentProduct]): FragmentRDD = {
    replaceDataset(tFn(dataset))
  }

  def replaceDataset(newDataset: Dataset[FragmentProduct]): FragmentRDD = {
    copy(dataset = newDataset)
  }

  def replaceSequences(
    newSequences: SequenceDictionary): FragmentRDD = {
    copy(sequences = newSequences)
  }

  def replaceRecordGroups(
    newRecordGroups: RecordGroupDictionary): FragmentRDD = {
    copy(recordGroups = newRecordGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): FragmentRDD = {
    copy(processingSteps = newProcessingSteps)
  }
}

case class RDDBoundFragmentRDD private[rdd] (
    rdd: RDD[Fragment],
    sequences: SequenceDictionary,
    recordGroups: RecordGroupDictionary,
    @transient val processingSteps: Seq[ProcessingStep],
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends FragmentRDD {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[FragmentProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(FragmentProduct.fromAvro))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): FragmentRDD = {
    copy(sequences = newSequences)
  }

  def replaceRecordGroups(
    newRecordGroups: RecordGroupDictionary): FragmentRDD = {
    copy(recordGroups = newRecordGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): FragmentRDD = {
    copy(processingSteps = newProcessingSteps)
  }
}

sealed abstract class FragmentRDD extends AvroRecordGroupGenomicDataset[Fragment, FragmentProduct, FragmentRDD] {

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
   * @return Returns a new FragmentRDD where the underlying RDD has been
   *   swapped out.
   */
  protected def replaceRdd(newRdd: RDD[Fragment],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): FragmentRDD = {
    RDDBoundFragmentRDD(newRdd,
      sequences,
      recordGroups,
      processingSteps,
      newPartitionMap)
  }

  def union(rdds: FragmentRDD*): FragmentRDD = {
    val iterableRdds = rdds.toSeq
    FragmentRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      iterableRdds.map(_.recordGroups).fold(recordGroups)(_ ++ _),
      iterableRdds.map(_.processingSteps).fold(processingSteps)(_ ++ _))
  }

  /**
   * Applies a function that transforms the underlying RDD into a new RDD using
   * the Spark SQL API.
   *
   * @param tFn A function that transforms the underlying RDD as a Dataset.
   * @return A new RDD where the RDD of genomic data has been replaced, but the
   *   metadata (sequence dictionary, and etc) is copied without modification.
   */
  def transformDataset(
    tFn: Dataset[FragmentProduct] => Dataset[FragmentProduct]): FragmentRDD = {
    DatasetBoundFragmentRDD(tFn(dataset),
      sequences,
      recordGroups,
      processingSteps)
  }

  /**
   * Essentially, splits up the reads in a Fragment.
   *
   * @return Returns this RDD converted back to reads.
   */
  def toReads(): AlignmentRecordRDD = {
    val converter = new AlignmentRecordConverter

    // convert the fragments to reads
    val newRdd = rdd.flatMap(converter.convertFragment)

    // are we aligned?
    AlignmentRecordRDD(newRdd,
      sequences,
      recordGroups,
      processingSteps)
  }

  /**
   * Marks reads as possible fragment duplicates.
   *
   * @return A new RDD where reads have the duplicate read flag set. Duplicate
   *   reads are NOT filtered out.
   */
  def markDuplicates(): FragmentRDD = MarkDuplicatesInDriver.time {
    replaceRdd(MarkDuplicates(this.rdd, this.recordGroups))
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
   * Rewrites the quality scores of fragments to place all quality scores in bins.
   *
   * Quality score binning maps all quality scores to a limited number of
   * discrete values, thus reducing the entropy of the quality score
   * distribution, and reducing the amount of space that fragments consume on disk.
   *
   * @param bins The bins to use.
   * @return Fragments whose quality scores are binned.
   */
  def binQualityScores(bins: Seq[QualityScoreBin]): FragmentRDD = {
    AlignmentRecordRDD.validateBins(bins)
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
      .flatMap(r => ReferenceRegion.opt(r))
      .toSeq
  }
}
