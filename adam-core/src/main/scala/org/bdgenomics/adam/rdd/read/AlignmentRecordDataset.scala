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
package org.bdgenomics.adam.rdd.read

import htsjdk.samtools._
import htsjdk.samtools.cram.ref.ReferenceSource
import htsjdk.samtools.util.{ BinaryCodec, BlockCompressedOutputStream }
import java.io.{ File, OutputStream, StringWriter, Writer }
import java.net.URI
import java.nio.file.Paths
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row, SQLContext }
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.algorithms.consensus.{
  ConsensusGenerator,
  ConsensusGeneratorFromReads,
  NormalizationUtils
}
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd._
import org.bdgenomics.adam.rdd.feature.{
  CoverageDataset,
  DatasetBoundCoverageDataset,
  RDDBoundCoverageDataset
}
import org.bdgenomics.adam.rdd.read.realignment.RealignIndels
import org.bdgenomics.adam.rdd.read.recalibration.BaseQualityRecalibration
import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.adam.rdd.variant.VariantDataset
import org.bdgenomics.adam.sql.{ AlignmentRecord => AlignmentRecordProduct }
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.util.{ FileMerger, ReferenceFile }
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.seqdoop.hadoop_bam._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.math.{ abs, min }
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class AlignmentRecordArray(
    array: Array[(ReferenceRegion, AlignmentRecord)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, AlignmentRecord] {

  def duplicate(): IntervalArray[ReferenceRegion, AlignmentRecord] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, AlignmentRecord)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, AlignmentRecord] = {
    AlignmentRecordArray(arr, maxWidth)
  }
}

private[adam] class AlignmentRecordArraySerializer extends IntervalArraySerializer[ReferenceRegion, AlignmentRecord, AlignmentRecordArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[AlignmentRecord]

  protected def builder(arr: Array[(ReferenceRegion, AlignmentRecord)],
                        maxIntervalWidth: Long): AlignmentRecordArray = {
    AlignmentRecordArray(arr, maxIntervalWidth)
  }
}

object AlignmentRecordDataset extends Serializable {

  /**
   * Hadoop configuration path to check for a boolean value indicating whether
   * the current or original read qualities should be written. True indicates
   * to write the original qualities. The default is false.
   */
  val WRITE_ORIGINAL_QUALITIES = "org.bdgenomics.adam.rdd.read.AlignmentRecordDataset.writeOriginalQualities"

  /**
   * Hadoop configuration path to check for a boolean value indicating whether
   * to write the "/1" "/2" suffixes to the read name that indicate whether a
   * read is first or second in a pair. Default is false (no suffixes).
   */
  val WRITE_SUFFIXES = "org.bdgenomics.adam.rdd.read.AlignmentRecordDataset.writeSuffixes"

  /**
   * Converts a processing step back to the SAM representation.
   *
   * @param ps The processing step to convert.
   * @return Returns an HTSJDK program group.
   */
  private[adam] def processingStepToSam(
    ps: ProcessingStep): SAMProgramRecord = {
    require(ps.getId != null,
      "Processing stage ID cannot be null (%s).".format(ps))
    val pg = new SAMProgramRecord(ps.getId)
    Option(ps.getPreviousId).foreach(pg.setPreviousProgramGroupId(_))
    Option(ps.getProgramName).foreach(pg.setProgramName)
    Option(ps.getVersion).foreach(pg.setProgramVersion)
    Option(ps.getCommandLine).foreach(pg.setCommandLine)
    pg
  }

  /**
   * Builds an AlignmentRecordDataset for unaligned reads.
   *
   * @param rdd The underlying AlignmentRecord RDD.
   * @return A new AlignmentRecordDataset.
   */
  def unaligned(rdd: RDD[AlignmentRecord]): AlignmentRecordDataset = {
    RDDBoundAlignmentRecordDataset(rdd,
      SequenceDictionary.empty,
      ReadGroupDictionary.empty,
      Seq.empty,
      None)
  }

  /**
   * Validates that there are no gaps in a set of quality score bins.
   *
   * @param bins Bins to validate.
   *
   * @throws IllegalArgumentException Throws exception if the bins are empty,
   *   there is a gap between bins, or two bins overlap.
   */
  private[rdd] def validateBins(bins: Seq[QualityScoreBin]) {
    require(bins.nonEmpty, "Bins cannot be empty.")

    // if we have multiple bins, validate them
    // - check that we don't have gaps between bins
    // - check that we don't have overlapping bins
    if (bins.size > 1) {
      val sortedBins = bins.sortBy(_.low)
      (0 until (sortedBins.size - 1)).foreach(idx => {
        if (sortedBins(idx).high < sortedBins(idx + 1).low) {
          throw new IllegalArgumentException("Gap between bins %s and %s (all bins: %s).".format(
            sortedBins(idx), sortedBins(idx + 1), bins.mkString(",")))
        } else if (sortedBins(idx).high > sortedBins(idx + 1).low) {
          throw new IllegalArgumentException("Bins %s and %s overlap (all bins: %s).".format(
            sortedBins(idx), sortedBins(idx + 1), bins.mkString(",")))
        }
      })
    }
  }

  /**
   * Builds an AlignmentRecordDataset without a partition map from an RDD.
   *
   * @param rdd The underlying AlignmentRecord RDD.
   * @param sequences The sequence dictionary for the genomic dataset.
   * @param readGroups The read group dictionary for the genomic dataset.
   * @return A new AlignmentRecordDataset.
   */
  def apply(rdd: RDD[AlignmentRecord],
            sequences: SequenceDictionary,
            readGroups: ReadGroupDictionary,
            processingSteps: Seq[ProcessingStep]): AlignmentRecordDataset = {
    RDDBoundAlignmentRecordDataset(rdd,
      sequences,
      readGroups,
      processingSteps,
      None)
  }

  /**
   * Builds an AlignmentRecordDataset without a partition map from a Dataset.
   *
   * @param ds The underlying AlignmentRecord Dataset.
   * @param sequences The sequence dictionary for the genomic dataset.
   * @param readGroups The read group dictionary for the genomic dataset.
   * @return A new AlignmentRecordDataset.
   */
  def apply(ds: Dataset[AlignmentRecordProduct],
            sequences: SequenceDictionary,
            readGroups: ReadGroupDictionary,
            processingSteps: Seq[ProcessingStep]): AlignmentRecordDataset = {
    DatasetBoundAlignmentRecordDataset(ds,
      sequences,
      readGroups,
      processingSteps)
  }
}

case class ParquetUnboundAlignmentRecordDataset private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary,
    readGroups: ReadGroupDictionary,
    @transient val processingSteps: Seq[ProcessingStep]) extends AlignmentRecordDataset {

  lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val rdd: RDD[AlignmentRecord] = {
    sc.loadParquet(parquetFilename)
  }

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[AlignmentRecordProduct]
  }

  def replaceSequences(
    newSequences: SequenceDictionary): AlignmentRecordDataset = {
    copy(sequences = newSequences)
  }

  def replaceReadGroups(newReadGroups: ReadGroupDictionary): AlignmentRecordDataset = {
    copy(readGroups = newReadGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): AlignmentRecordDataset = {
    copy(processingSteps = newProcessingSteps)
  }
}

case class DatasetBoundAlignmentRecordDataset private[rdd] (
  dataset: Dataset[AlignmentRecordProduct],
  sequences: SequenceDictionary,
  readGroups: ReadGroupDictionary,
  @transient val processingSteps: Seq[ProcessingStep],
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends AlignmentRecordDataset
    with DatasetBoundGenomicDataset[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

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
    tFn: Dataset[AlignmentRecordProduct] => Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[AlignmentRecordProduct], Dataset[AlignmentRecordProduct]]): AlignmentRecordDataset = {
    copy(dataset = tFn.call(dataset))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): AlignmentRecordDataset = {
    copy(sequences = newSequences)
  }

  def replaceReadGroups(newReadGroups: ReadGroupDictionary): AlignmentRecordDataset = {
    copy(readGroups = newReadGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): AlignmentRecordDataset = {
    copy(processingSteps = newProcessingSteps)
  }

  override def filterByMappingQuality(minimumMappingQuality: Int): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("mappingQuality") >= minimumMappingQuality))
  }

  override def filterUnalignedReads(): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("readMapped")))
  }

  override def filterUnpairedReads(): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("readPaired")))
  }

  override def filterDuplicateReads(): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(!dataset.col("duplicateRead")))
  }

  override def filterToPrimaryAlignments(): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("primaryAlignment")))
  }

  override def filterToReadGroup(readGroupId: String): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("readGroupId") === readGroupId))
  }

  override def filterToReadGroups(readGroupIds: Seq[String]): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("readGroupId") isin (readGroupIds: _*)))
  }

  override def filterToSample(readGroupSampleId: String): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("readGroupSampleId") === readGroupSampleId))
  }

  override def filterToSamples(readGroupSampleIds: Seq[String]): AlignmentRecordDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("readGroupSampleId") isin (readGroupSampleIds: _*)))
  }
}

case class RDDBoundAlignmentRecordDataset private[rdd] (
    rdd: RDD[AlignmentRecord],
    sequences: SequenceDictionary,
    readGroups: ReadGroupDictionary,
    @transient val processingSteps: Seq[ProcessingStep],
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends AlignmentRecordDataset {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[AlignmentRecordProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(AlignmentRecordProduct.fromAvro))
  }

  override def toCoverage(): CoverageDataset = {
    val covCounts =
      rdd.filter(r => {
        val readMapped = r.getReadMapped

        // validate alignment fields
        if (readMapped) {
          require(r.getStart != null && r.getEnd != null && r.getReferenceName != null,
            "Read was mapped but was missing alignment start/end/reference (%s).".format(r))
        }

        readMapped
      }).flatMap(r => {
        val positions: List[Long] = List.range(r.getStart, r.getEnd)
        positions.map(n => ((r.getReadGroupSampleId, ReferencePosition(r.getReferenceName, n)), 1))
      }).reduceByKey(_ + _)
        .map(r => Coverage(r._1._2, r._2.toDouble, Option(r._1._1)))

    RDDBoundCoverageDataset(covCounts, sequences, readGroups.toSamples, None)
  }

  def replaceSequences(
    newSequences: SequenceDictionary): AlignmentRecordDataset = {
    copy(sequences = newSequences)
  }

  def replaceReadGroups(newReadGroups: ReadGroupDictionary): AlignmentRecordDataset = {
    copy(readGroups = newReadGroups)
  }

  def replaceProcessingSteps(
    newProcessingSteps: Seq[ProcessingStep]): AlignmentRecordDataset = {
    copy(processingSteps = newProcessingSteps)
  }
}

private case class AlignmentWindow(referenceName: String, start: Long, end: Long, sampleId: String) {
}

sealed abstract class AlignmentRecordDataset extends AvroReadGroupGenomicDataset[AlignmentRecord, AlignmentRecordProduct, AlignmentRecordDataset] {

  protected val productFn = AlignmentRecordProduct.fromAvro(_)
  protected val unproductFn = (a: AlignmentRecordProduct) => a.toAvro

  @transient val uTag: TypeTag[AlignmentRecordProduct] = typeTag[AlignmentRecordProduct]

  override def transformDataset(
    tFn: Dataset[AlignmentRecordProduct] => Dataset[AlignmentRecordProduct]): AlignmentRecordDataset = {
    DatasetBoundAlignmentRecordDataset(dataset,
      sequences,
      readGroups,
      processingSteps)
      .transformDataset(tFn)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[AlignmentRecordProduct], Dataset[AlignmentRecordProduct]]): AlignmentRecordDataset = {
    DatasetBoundAlignmentRecordDataset(dataset,
      sequences,
      readGroups,
      processingSteps)
      .transformDataset(tFn)
  }

  /**
   * Replaces the underlying RDD and SequenceDictionary and emits a new object.
   *
   * @param newRdd New RDD to replace current RDD.
   * @param newSequences New sequence dictionary to replace current dictionary.
   * @return Returns a new AlignmentRecordDataset.
   */
  protected def replaceRddAndSequences(newRdd: RDD[AlignmentRecord],
                                       newSequences: SequenceDictionary,
                                       partitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): AlignmentRecordDataset = {
    RDDBoundAlignmentRecordDataset(newRdd,
      newSequences,
      readGroups,
      processingSteps,
      partitionMap)
  }

  protected def replaceRdd(newRdd: RDD[AlignmentRecord],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): AlignmentRecordDataset = {
    RDDBoundAlignmentRecordDataset(newRdd,
      sequences,
      readGroups,
      processingSteps,
      newPartitionMap)
  }

  protected def buildTree(rdd: RDD[(ReferenceRegion, AlignmentRecord)])(
    implicit tTag: ClassTag[AlignmentRecord]): IntervalArray[ReferenceRegion, AlignmentRecord] = {
    IntervalArray(rdd, AlignmentRecordArray.apply(_, _))
  }

  def union(datasets: AlignmentRecordDataset*): AlignmentRecordDataset = {
    val iterableDatasets = datasets.toSeq
    AlignmentRecordDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.sequences).fold(sequences)(_ ++ _),
      iterableDatasets.map(_.readGroups).fold(readGroups)(_ ++ _),
      iterableDatasets.map(_.processingSteps).fold(processingSteps)(_ ++ _))
  }

  /**
   * Convert this set of reads into fragments.
   *
   * @return Returns a FragmentDataset where all reads have been grouped together by
   *   the original sequence fragment they come from.
   */
  def toFragments(): FragmentDataset = {
    FragmentDataset(groupReadsByFragment().map(_.toFragment),
      sequences,
      readGroups,
      processingSteps)
  }

  /**
   * Groups all reads by read group and read name.
   *
   * @return SingleReadBuckets with primary, secondary and unmapped reads
   */
  private def locallyGroupReadsByFragment(): RDD[SingleReadBucket] = {
    SingleReadBucket.fromQuerynameSorted(rdd)
  }

  /**
   * Convert this set of reads into fragments.
   *
   * Assumes that reads are sorted by readname.
   *
   * @return Returns a FragmentDataset where all reads have been grouped together by
   *   the original sequence fragment they come from.
   */
  private[rdd] def querynameSortedToFragments: FragmentDataset = {
    FragmentDataset(locallyGroupReadsByFragment().map(_.toFragment),
      sequences,
      readGroups,
      processingSteps)
  }

  /**
   * Converts this set of reads into a corresponding CoverageDataset.
   *
   * @return CoverageDataset containing mapped genomic dataset of Coverage.
   */
  def toCoverage(): CoverageDataset = {
    import dataset.sqlContext.implicits._
    val covCounts = dataset.toDF
      .where($"readMapped")
      .select($"referenceName", $"start", $"end", $"readGroupSampleId")
      .withColumnRenamed("readGroupSampleId", "sampleId")
      .as[AlignmentWindow]
      .flatMap(w => {
        val width = (w.end - w.start).toInt
        val buffer = new Array[Coverage](width)
        var idx = 0
        var pos = w.start
        while (idx < width) {
          val lastPos = pos
          pos += 1L
          buffer(idx) = Coverage(w.referenceName, lastPos, pos, 1.0, Option(w.sampleId))
          idx += 1
        }
        buffer
      }).toDF
      .withColumnRenamed("sampleId", "optSampleId")
      .groupBy("referenceName", "start", "end", "optSampleId")
      .sum("count")
      .withColumnRenamed("sum(count)", "count")
      .as[Coverage]

    DatasetBoundCoverageDataset(covCounts, sequences, readGroups.toSamples)
  }

  /**
   * Returns all reference regions that overlap this read.
   *
   * If a read is unaligned, it covers no reference region. If a read is aligned
   * we expect it to cover a single region. A chimeric read would cover multiple
   * regions, but we store chimeric reads in a way similar to BAM, where the
   * split alignments are stored in multiple separate reads.
   *
   * @param elem Read to produce regions for.
   * @return The seq of reference regions this read covers.
   */
  protected def getReferenceRegions(elem: AlignmentRecord): Seq[ReferenceRegion] = {
    ReferenceRegion.opt(elem).toSeq
  }

  /**
   * Saves this genomic dataset as BAM, CRAM, or SAM if the extension provided is .sam, .cram,
   * or .bam.
   *
   * @param args Arguments defining where to save the file.
   * @param isSorted True if input data is sorted. Sets the ordering in the SAM
   *   file header.
   * @return Returns true if the extension in args ended in .sam/.bam and the
   *   file was saved.
   */
  private[rdd] def maybeSaveBam(args: ADAMSaveAnyArgs,
                                isSorted: Boolean = false): Boolean = {

    if (args.outputPath.endsWith(".sam") ||
      args.outputPath.endsWith(".bam") ||
      args.outputPath.endsWith(".cram")) {
      log.info("Saving data in SAM/BAM/CRAM format")
      saveAsSam(
        args.outputPath,
        isSorted = isSorted,
        asSingleFile = args.asSingleFile,
        deferMerging = args.deferMerging,
        disableFastConcat = args.disableFastConcat
      )
      true
    } else {
      false
    }
  }

  /**
   * Saves this genomic dataset as FASTQ if the file has the proper extension.
   *
   * @param args Save arguments defining the file path to save at.
   * @return True if the file extension ended in ".fq" or ".fastq" and the file
   *   was saved as FASTQ, or if the file extension ended in ".ifq" and the file
   *   was saved as interleaved FASTQ.
   */
  private[rdd] def maybeSaveFastq(args: ADAMSaveAnyArgs): Boolean = {
    if (args.outputPath.endsWith(".fq") || args.outputPath.endsWith(".fastq") ||
      args.outputPath.endsWith(".ifq")) {
      saveAsFastq(args.outputPath,
        sort = args.sortFastqOutput,
        asSingleFile = args.asSingleFile,
        disableFastConcat = args.disableFastConcat
      )
      true
    } else
      false
  }

  /**
   * Saves AlignmentRecords as a directory of Parquet files or as SAM/BAM.
   *
   * This method infers the output format from the file extension. Filenames
   * ending in .sam/.bam are saved as SAM/BAM, and all other files are saved
   * as Parquet.
   *
   * @param args Save configuration arguments.
   * @param isSorted If the output is sorted, this will modify the SAM/BAM header.
   * @return Returns true if saving succeeded.
   */
  def save(args: ADAMSaveAnyArgs,
           isSorted: Boolean = false): Boolean = {

    (maybeSaveBam(args, isSorted) ||
      maybeSaveFastq(args) ||
      { saveAsParquet(args); true })
  }

  /**
   * Saves this genomic dataset to disk, with the type identified by the extension.
   *
   * @param filePath Path to save the file at.
   * @param isSorted Whether the file is sorted or not.
   * @return Returns true if saving succeeded.
   */
  def save(filePath: java.lang.String,
           isSorted: java.lang.Boolean): java.lang.Boolean = {
    save(new JavaSaveArgs(filePath), isSorted)
  }

  /**
   * Converts a genomic dataset into the SAM spec string it represents.
   *
   * This method converts a genomic dataset of AlignmentRecords back to an RDD of
   * SAMRecordWritables and a SAMFileHeader, and then maps this RDD into a
   * string on the driver that represents this file in SAM.
   *
   * @return A string on the driver representing this genomic dataset of reads in SAM format.
   */
  def saveAsSamString(): String = {

    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) = convertToSam()

    // collect the records to the driver
    val records = convertRecords.collect()

    // get a header writing codec
    val samHeaderCodec = new SAMTextHeaderCodec
    samHeaderCodec.setValidationStringency(ValidationStringency.SILENT)

    // create a stringwriter and write the header to it
    val samStringWriter = new StringWriter()
    samHeaderCodec.encode(samStringWriter, header)

    // create a sam text writer
    val samWriter: SAMTextWriter = new SAMTextWriter(samStringWriter)

    // write all records to the writer
    records.foreach(record => samWriter.writeAlignment(record.get))

    // return the writer as a string
    samStringWriter.toString
  }

  /**
   * Converts boolean sorted state into SortOrder enum.
   *
   * @param isSorted Whether the file is sorted or not.
   * @return Returns coordinate order if sorted, and unsorted otherwise.
   */
  private def isSortedToSortOrder(isSorted: Boolean = false): SAMFileHeader.SortOrder = {
    if (isSorted) {
      SAMFileHeader.SortOrder.coordinate
    } else {
      SAMFileHeader.SortOrder.unsorted
    }
  }

  /**
   * Converts an RDD of ADAM read records into SAM records.
   *
   * @return Returns a SAM/BAM formatted RDD of reads, as well as the file header.
   */
  def convertToSam(isSorted: Boolean = false): (RDD[SAMRecordWritable], SAMFileHeader) = ConvertToSAM.time {
    convertToSam(isSortedToSortOrder(isSorted))
  }

  def convertToSam(sortOrder: SAMFileHeader.SortOrder): (RDD[SAMRecordWritable], SAMFileHeader) = ConvertToSAM.time {

    // create conversion object
    val adamRecordConverter = new AlignmentRecordConverter

    // create header and set sort order if needed
    val header = adamRecordConverter.createSAMHeader(sequences, readGroups)
    header.setSortOrder(sortOrder)

    // get program records and attach to header
    val pgRecords = processingSteps.map(r => {
      AlignmentRecordDataset.processingStepToSam(r)
    })
    header.setProgramRecords(pgRecords)

    // broadcast for efficiency
    val hdrBcast = rdd.context.broadcast(SAMFileHeaderWritable(header))

    // map across RDD to perform conversion
    val convertedRDD: RDD[SAMRecordWritable] = rdd.map(r => {
      // must wrap record for serializability
      val srw = new SAMRecordWritable()
      srw.set(adamRecordConverter.convert(r, hdrBcast.value, readGroups))
      srw
    })

    (convertedRDD, header)
  }

  /**
   * Cuts reads into _k_-mers, and then counts the number of occurrences of each _k_-mer.
   *
   * @param kmerLength The value of _k_ to use for cutting _k_-mers.
   * @return Returns an RDD containing k-mer/count pairs.
   */
  def countKmers(kmerLength: Int): RDD[(String, Long)] = {
    rdd.flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getSequence
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }

  /**
   * Cuts reads into _k_-mers, and then counts the number of occurrences of each _k_-mer.
   *
   * @param kmerLength The value of _k_ to use for cutting _k_-mers.
   * @return Returns a Dataset containing k-mer/count pairs.
   */
  def countKmersAsDataset(kmerLength: Int): Dataset[(String, Long)] = {
    import dataset.sqlContext.implicits._
    val kmers = dataset.select($"sequence".as[String])
      .flatMap(_.sliding(kmerLength))
      .as[String]

    kmers.toDF()
      .groupBy($"value")
      .count()
      .select($"value".as("kmer"), $"count".as("count"))
      .as[(String, Long)]
  }

  /**
   * Saves this genomic dataset of ADAM read data into the SAM/BAM format.
   *
   * @param filePath Path to save files to.
   * @param asType Selects whether to save as SAM, BAM, or CRAM. The default
   *   value is None, which means the file type is inferred from the extension.
   * @param asSingleFile If true, saves output as a single file.
   * @param isSorted If the output is sorted, this will modify the header.
   * @param deferMerging If true and asSingleFile is true, we will save the
   *   output shards as a headerless file, but we will not merge the shards.
   * @param disableFastConcat If asSingleFile is true and deferMerging is false,
   *   disables the use of the parallel file merging engine.
   */
  def saveAsSam(
    filePath: String,
    asType: Option[SAMFormat] = None,
    asSingleFile: Boolean = false,
    isSorted: Boolean = false,
    deferMerging: Boolean = false,
    disableFastConcat: Boolean = false): Unit = SAMSave.time {
    saveAsSam(filePath, asType, asSingleFile, isSortedToSortOrder(isSorted), deferMerging, disableFastConcat)
  }

  def saveAsSam(
    filePath: String,
    asType: Option[SAMFormat],
    asSingleFile: Boolean,
    sortOrder: SAMFileHeader.SortOrder,
    deferMerging: Boolean,
    disableFastConcat: Boolean): Unit = SAMSave.time {
    val fileType = asType.getOrElse(SAMFormat.inferFromFilePath(filePath))

    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) =
      convertToSam(sortOrder)

    // add keys to our records
    val withKey = convertRecords.keyBy(v => new LongWritable(v.get.getAlignmentStart))

    // write file to disk
    val conf = rdd.context.hadoopConfiguration

    // get file system
    val headPath = new Path(filePath + "_head")
    val tailPath = new Path(filePath + "_tail")
    val outputPath = new Path(filePath)
    val fs = headPath.getFileSystem(rdd.context.hadoopConfiguration)

    // TIL: sam and bam are written in completely different ways!
    if (fileType == SAMFormat.SAM) {
      SAMHeaderWriter.writeHeader(fs, headPath, header)
    } else if (fileType == SAMFormat.BAM) {

      // get an output stream
      val os = fs.create(headPath)
        .asInstanceOf[OutputStream]

      // create htsjdk specific streams for writing the bam header
      val compressedOut: OutputStream = new BlockCompressedOutputStream(os, null.asInstanceOf[File])
      val binaryCodec = new BinaryCodec(compressedOut)

      // write a bam header - cribbed from Hadoop-BAM
      binaryCodec.writeBytes("BAM\001".getBytes())
      val sw: Writer = new StringWriter()
      new SAMTextHeaderCodec().encode(sw, header)
      binaryCodec.writeString(sw.toString, true, false)

      // write sequence dictionary
      val ssd = header.getSequenceDictionary
      binaryCodec.writeInt(ssd.size())
      ssd.getSequences
        .toList
        .foreach(r => {
          binaryCodec.writeString(r.getSequenceName(), true, true)
          binaryCodec.writeInt(r.getSequenceLength())
        })

      // flush and close all the streams
      compressedOut.flush()
      os.flush()
      os.close()
    } else {
      // from https://samtools.github.io/hts-specs/CRAMv3.pdf
      // cram has a variety of addtional constraints:
      //
      // * file definition has 20 byte identifier field
      // * header must have SO:pos
      // * sequence records must have attached MD5s (we don't support
      //   embedding reference sequences)
      //
      // we'll defer the writing to the cram container stream writer, and will
      // do validation here

      require(sortOrder != SAMFileHeader.SortOrder.unsorted, "To save as CRAM, input must be sorted.")
      require(sequences.records.forall(_.md5.isDefined),
        "To save as CRAM, all sequences must have an attached MD5. See %s".format(
          sequences))
      val refSource = conf.get(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY)
      require(refSource != null,
        "To save as CRAM, the reference source must be set in your config as %s.".format(
          CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY))

      // get an output stream
      val os = fs.create(headPath)
        .asInstanceOf[OutputStream]

      // create a cram container writer
      val csw = new CRAMContainerStreamWriter(os, null, // null -> do not write index
        new ReferenceSource(Paths.get(URI.create(refSource))),
        header,
        filePath) // write filepath as id

      // write the header
      csw.writeHeader(header)

      // finish the cram container, but don't write EOF
      csw.finish(false)

      // flush and close the output stream
      os.flush()
      os.close()
    }

    // set path to header file
    conf.set("org.bdgenomics.adam.rdd.read.bam_header_path", headPath.toString)

    if (!asSingleFile) {
      val headeredOutputFormat = fileType match {
        case SAMFormat.SAM  => classOf[InstrumentedADAMSAMOutputFormat[LongWritable]]
        case SAMFormat.BAM  => classOf[InstrumentedADAMBAMOutputFormat[LongWritable]]
        case SAMFormat.CRAM => classOf[InstrumentedADAMCRAMOutputFormat[LongWritable]]
      }
      withKey.saveAsNewAPIHadoopFile(
        filePath,
        classOf[LongWritable],
        classOf[SAMRecordWritable],
        headeredOutputFormat,
        conf
      )

      // clean up the header after writing
      fs.delete(headPath, true)
    } else {
      log.info(s"Writing single ${fileType} file (not Hadoop-style directory)")

      val tailPath = new Path(filePath + "_tail")
      val outputPath = new Path(filePath)

      // set up output format
      val headerLessOutputFormat = fileType match {
        case SAMFormat.SAM  => classOf[InstrumentedADAMSAMOutputFormatHeaderLess[LongWritable]]
        case SAMFormat.BAM  => classOf[InstrumentedADAMBAMOutputFormatHeaderLess[LongWritable]]
        case SAMFormat.CRAM => classOf[InstrumentedADAMCRAMOutputFormatHeaderLess[LongWritable]]
      }

      // save rdd
      withKey.saveAsNewAPIHadoopFile(
        tailPath.toString,
        classOf[LongWritable],
        classOf[SAMRecordWritable],
        headerLessOutputFormat,
        conf
      )

      if (!deferMerging) {
        FileMerger.mergeFiles(rdd.context,
          fs,
          outputPath,
          tailPath,
          optHeaderPath = Some(headPath),
          writeEmptyGzipBlock = (fileType == SAMFormat.BAM),
          writeCramEOF = (fileType == SAMFormat.CRAM),
          disableFastConcat = disableFastConcat)
      }
    }
  }

  /**
   * Saves this genomic dataset to disk as a SAM/BAM/CRAM file.
   *
   * @param filePath Path to save the file at.
   * @param asType The SAMFormat to save as. If left null, we will infer the
   *   format from the file extension.
   * @param asSingleFile If true, saves output as a single file.
   * @param isSorted If the output is sorted, this will modify the header.
   */
  def saveAsSam(
    filePath: java.lang.String,
    asType: SAMFormat,
    asSingleFile: java.lang.Boolean,
    isSorted: java.lang.Boolean) {
    saveAsSam(filePath,
      asType = Option(asType),
      asSingleFile = asSingleFile,
      isSorted = isSorted)
  }

  /**
   * Sorts our read data by read name.
   *
   * @return Returns a new RDD containing sorted reads.
   */
  def sortReadsByReadName(): AlignmentRecordDataset = SortReads.time {
    log.info("Sorting reads by read name")

    transformDataset(_.orderBy("readName", "readInFragment"))
  }

  /**
   * Sorts our read data by reference positions, with references ordered by name.
   *
   * Sorts reads by the location where they are aligned. Unaligned reads are
   * put at the end and sorted by read name. References are ordered
   * lexicographically.
   *
   * @return Returns a new genomic dataset containing sorted reads.
   *
   * @see sortReadsByReferencePositionAndIndex
   */
  def sortReadsByReferencePosition(): AlignmentRecordDataset = SortReads.time {
    log.info("Sorting reads by reference position")

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we sort the unmapped reads by read name. We prefix with tildes ("~";
    // ASCII 126) to ensure that the read name is lexicographically "after" the
    // reference names.
    replaceRddAndSequences(rdd.sortBy(r => {
      if (r.getReadMapped) {
        ReferencePosition(r)
      } else {
        ReferencePosition(s"~~~${r.getReadName}", 0)
      }
    }), sequences.stripIndices.sorted)
  }

  /**
   * Sorts our read data by reference positions, with references ordered by index.
   *
   * Sorts reads by the location where they are aligned. Unaligned reads are
   * put at the end and sorted by read name. References are ordered by index
   * that they are ordered in the SequenceDictionary.
   *
   * @return Returns a new genomic dataset containing sorted reads.
   *
   * @see sortReadsByReferencePosition
   */
  def sortReadsByReferencePositionAndIndex(): AlignmentRecordDataset = SortByIndex.time {
    log.info("Sorting reads by reference index, using %s.".format(sequences))

    import scala.math.Ordering.{ Int => ImplicitIntOrdering, _ }

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we sort the unmapped reads by read name. To do this, we hash the sequence name
    // and add the max reference index
    val maxReferenceIndex = sequences.records.flatMap(_.index).max
    replaceRdd(rdd.sortBy(r => {
      if (r.getReadMapped) {
        val sr = sequences(r.getReferenceName)
        require(sr.isDefined, "Read %s has reference name %s not in dictionary %s.".format(
          r, r.getReferenceName, sequences))
        require(sr.get.index.isDefined,
          "Reference %s from sequence dictionary lacks an index.".format(sr))

        (sr.get.index.get, r.getStart: Long)
      } else {
        val readHash = abs(r.getReadName.hashCode + maxReferenceIndex)
        val idx = if (readHash > maxReferenceIndex) readHash else Int.MaxValue
        (idx, 0L)
      }
    }))
  }

  /**
   * Marks reads as possible fragment duplicates.
   *
   * @return A new genomic dataset where reads have the duplicate read flag set. Duplicate
   *   reads are NOT filtered out.
   */
  def markDuplicates(): AlignmentRecordDataset = MarkDuplicatesInDriver.time {
    replaceRdd(MarkDuplicates(this))
  }

  /**
   * (Java-specific) Runs base quality score recalibration on a set of reads. Uses a table of
   * known SNPs to mask true variation during the recalibration process.
   *
   * @param knownSnps A table of known SNPs to mask valid variants.
   * @param minAcceptableQuality The minimum quality score to recalibrate.
   * @param storageLevel An optional storage level to set for the output
   *   of the first stage of BQSR. Set to null to omit.
   * @return Returns a genomic dataset of recalibrated reads.
   */
  def recalibrateBaseQualities(
    knownSnps: VariantDataset,
    minAcceptableQuality: java.lang.Integer,
    storageLevel: StorageLevel): AlignmentRecordDataset = {
    val snpTable = SnpTable(knownSnps)
    val bcastSnps = rdd.context.broadcast(snpTable)
    val sMinQual: Int = minAcceptableQuality
    recalibrateBaseQualities(bcastSnps,
      minAcceptableQuality = sMinQual,
      optStorageLevel = Option(storageLevel))
  }

  /**
   * (Java-specific) Runs base quality score recalibration on a set of reads. Uses a table of
   * known SNPs to mask true variation during the recalibration process.
   *
   * @param knownSnps A table of known SNPs to mask valid variants.
   * @param minAcceptableQuality The minimum quality score to recalibrate.
   * @param storageLevel Storage level to set for the output
   *   of the first stage of BQSR. Set to null to omit.
   * @param samplingFraction Fraction of reads to sample when
   *   generating the covariate table.
   * @param samplingSeed Seed to provide if downsampling reads.
   * @return Returns a genomic dataset of recalibrated reads.
   */
  def recalibrateBaseQualities(
    knownSnps: VariantDataset,
    minAcceptableQuality: java.lang.Integer,
    storageLevel: StorageLevel,
    samplingFraction: java.lang.Double,
    samplingSeed: java.lang.Long): AlignmentRecordDataset = {
    val snpTable = SnpTable(knownSnps)
    val bcastSnps = rdd.context.broadcast(snpTable)
    val sMinQual: Int = minAcceptableQuality
    recalibrateBaseQualities(bcastSnps,
      minAcceptableQuality = sMinQual,
      optStorageLevel = Option(storageLevel),
      optSamplingFraction = Option(samplingFraction),
      optSamplingSeed = Option(samplingSeed))
  }

  /**
   * (Scala-specific) Runs base quality score recalibration on a set of reads. Uses a table of
   * known SNPs to mask true variation during the recalibration process.
   *
   * @param knownSnps A table of known SNPs to mask valid variants.
   * @param minAcceptableQuality The minimum quality score to recalibrate.
   * @param optStorageLevel An optional storage level to set for the output
   *   of the first stage of BQSR. Defaults to StorageLevel.MEMORY_ONLY.
   * @param optSamplingFraction An optional fraction of reads to sample when
   *   generating the covariate table.
   * @param optSamplingSeed An optional seed to provide if downsampling reads.
   * @return Returns a genomic dataset of recalibrated reads.
   */
  def recalibrateBaseQualities(
    knownSnps: Broadcast[SnpTable],
    minAcceptableQuality: Int = 5,
    optStorageLevel: Option[StorageLevel] = Some(StorageLevel.MEMORY_ONLY),
    optSamplingFraction: Option[Double] = None,
    optSamplingSeed: Option[Long] = None): AlignmentRecordDataset = BQSRInDriver.time {
    replaceRdd(BaseQualityRecalibration(rdd,
      knownSnps,
      readGroups,
      minAcceptableQuality,
      optStorageLevel = optStorageLevel,
      optSamplingFraction = optSamplingFraction,
      optSamplingSeed = optSamplingSeed))
  }

  /**
   * (Java-specific) Realigns indels using a consensus-based heuristic with
   * default parameters.
   *
   * @return Returns a genomic dataset of mapped reads which have been realigned.
   */
  def realignIndels(): AlignmentRecordDataset = {
    realignIndels(consensusModel = new ConsensusGeneratorFromReads)
  }

  /**
   * (Java-specific) Realigns indels using a consensus-based heuristic with
   * the specified reference and default parameters.
   *
   * @param referenceFile Reference file.
   * @return Returns a genomic dataset of mapped reads which have been realigned.
   */
  def realignIndels(referenceFile: ReferenceFile): AlignmentRecordDataset = {
    realignIndels(consensusModel = new ConsensusGeneratorFromReads,
      optReferenceFile = Option(referenceFile)
    )
  }

  /**
   * (Java-specific) Realigns indels using a consensus-based heuristic.
   *
   * @param consensusModel The model to use for generating consensus sequences
   *   to realign against.
   * @param isSorted If the input data is sorted, setting this parameter to
   *   true avoids a second sort.
   * @param maxIndelSize The size of the largest indel to use for realignment.
   * @param maxConsensusNumber The maximum number of consensus sequences to
   *   realign against per target region.
   * @param lodThreshold Log-odds threshold to use when realigning; realignments
   *   are only finalized if the log-odds threshold is exceeded.
   * @param maxTargetSize The maximum width of a single target region for
   *   realignment.
   * @param maxReadsPerTarget Maximum number of reads per target.
   * @param unclipReads If true, unclips reads prior to realignment. Else,
   *   omits clipped bases during realignment.
   * @return Returns a genomic dataset of mapped reads which have been realigned.
   */
  def realignIndels(
    consensusModel: ConsensusGenerator,
    isSorted: java.lang.Boolean,
    maxIndelSize: java.lang.Integer,
    maxConsensusNumber: java.lang.Integer,
    lodThreshold: java.lang.Double,
    maxTargetSize: java.lang.Integer,
    maxReadsPerTarget: java.lang.Integer,
    unclipReads: java.lang.Boolean): AlignmentRecordDataset = {
    realignIndels(consensusModel,
      isSorted = isSorted,
      maxIndelSize = maxIndelSize,
      maxConsensusNumber = maxConsensusNumber,
      lodThreshold = lodThreshold,
      maxTargetSize = maxTargetSize,
      maxReadsPerTarget = maxReadsPerTarget,
      unclipReads = unclipReads,
      optReferenceFile = None)
  }

  /**
   * (Java-specific) Realigns indels using a consensus-based heuristic with
   * the specified reference.
   *
   * @param consensusModel The model to use for generating consensus sequences
   *   to realign against.
   * @param isSorted If the input data is sorted, setting this parameter to
   *   true avoids a second sort.
   * @param maxIndelSize The size of the largest indel to use for realignment.
   * @param maxConsensusNumber The maximum number of consensus sequences to
   *   realign against per target region.
   * @param lodThreshold Log-odds threshold to use when realigning; realignments
   *   are only finalized if the log-odds threshold is exceeded.
   * @param maxTargetSize The maximum width of a single target region for
   *   realignment.
   * @param maxReadsPerTarget Maximum number of reads per target.
   * @param unclipReads If true, unclips reads prior to realignment. Else,
   *   omits clipped bases during realignment.
   * @param referenceFile Reference file.
   * @return Returns a genomic dataset of mapped reads which have been realigned.
   */
  def realignIndels(
    consensusModel: ConsensusGenerator,
    isSorted: java.lang.Boolean,
    maxIndelSize: java.lang.Integer,
    maxConsensusNumber: java.lang.Integer,
    lodThreshold: java.lang.Double,
    maxTargetSize: java.lang.Integer,
    maxReadsPerTarget: java.lang.Integer,
    unclipReads: java.lang.Boolean,
    referenceFile: ReferenceFile): AlignmentRecordDataset = {
    realignIndels(consensusModel,
      isSorted = isSorted,
      maxIndelSize = maxIndelSize,
      maxConsensusNumber = maxConsensusNumber,
      lodThreshold = lodThreshold,
      maxTargetSize = maxTargetSize,
      maxReadsPerTarget = maxReadsPerTarget,
      unclipReads = unclipReads,
      optReferenceFile = Some(referenceFile))
  }

  /**
   * (Scala-specific) Realigns indels using a consensus-based heuristic.
   *
   * @param consensusModel The model to use for generating consensus sequences
   *   to realign against.
   * @param isSorted If the input data is sorted, setting this parameter to
   *   true avoids a second sort. Defaults to false.
   * @param maxIndelSize The size of the largest indel to use for realignment.
   *   Defaults to 500.
   * @param maxConsensusNumber The maximum number of consensus sequences to
   *   realign against per target region. Defaults to 30.
   * @param lodThreshold Log-odds threshold to use when realigning; realignments
   *   are only finalized if the log-odds threshold is exceeded. Defaults to 5.0.
   * @param maxTargetSize The maximum width of a single target region for
   *   realignment. Defaults to 3000.
   * @param maxReadsPerTarget Maximum number of reads per target. Defaults to 20000.
   * @param unclipReads If true, unclips reads prior to realignment. Else,
   *   omits clipped bases during realignment. Defaults to false.
   * @param optReferenceFile An optional reference. If not provided, reference
   *   will be inferred from MD tags. Defaults to None.
   * @return Returns a genomic dataset of mapped reads which have been realigned.
   */
  def realignIndels(
    consensusModel: ConsensusGenerator = new ConsensusGeneratorFromReads,
    isSorted: Boolean = false,
    maxIndelSize: Int = 500,
    maxConsensusNumber: Int = 30,
    lodThreshold: Double = 5.0,
    maxTargetSize: Int = 3000,
    maxReadsPerTarget: Int = 20000,
    unclipReads: Boolean = false,
    optReferenceFile: Option[ReferenceFile] = None): AlignmentRecordDataset = RealignIndelsInDriver.time {
    replaceRdd(RealignIndels(rdd,
      consensusModel = consensusModel,
      dataIsSorted = isSorted,
      maxIndelSize = maxIndelSize,
      maxConsensusNumber = maxConsensusNumber,
      lodThreshold = lodThreshold,
      maxTargetSize = maxTargetSize,
      maxReadsPerTarget = maxReadsPerTarget,
      unclipReads = unclipReads,
      optReferenceFile = optReferenceFile))
  }

  /**
   * (Java-specific) Computes the mismatching positions field (SAM "MD" tag).
   *
   * @param referenceFile A reference file that can be broadcast to all nodes.
   * @param overwriteExistingTags If true, overwrites the MD tags on reads where
   *   it is already populated. If false, we only tag reads that are currently
   *   missing an MD tag.
   * @param validationStringency If we are recalculating existing tags and we
   *   find that the MD tag that was previously on the read doesn't match our
   *   new tag, LENIENT will log a warning message, STRICT will throw an
   *   exception, and SILENT will ignore.
   * @return Returns a new AlignmentRecordDataset where all reads have the
   *   mismatchingPositions field populated.
   */
  def computeMismatchingPositions(
    referenceFile: ReferenceFile,
    overwriteExistingTags: java.lang.Boolean,
    validationStringency: ValidationStringency): AlignmentRecordDataset = {
    computeMismatchingPositions(referenceFile = referenceFile,
      overwriteExistingTags = overwriteExistingTags,
      validationStringency = validationStringency)
  }

  /**
   * (Scala-specific) Computes the mismatching positions field (SAM "MD" tag).
   *
   * @param referenceFile A reference file that can be broadcast to all nodes.
   * @param overwriteExistingTags If true, overwrites the MD tags on reads where
   *   it is already populated. If false, we only tag reads that are currently
   *   missing an MD tag. Default is false.
   * @param validationStringency If we are recalculating existing tags and we
   *   find that the MD tag that was previously on the read doesn't match our
   *   new tag, LENIENT will log a warning message, STRICT will throw an
   *   exception, and SILENT will ignore. Default is LENIENT.
   * @return Returns a new AlignmentRecordDataset where all reads have the
   *   mismatchingPositions field populated.
   */
  def computeMismatchingPositions(
    referenceFile: ReferenceFile,
    overwriteExistingTags: Boolean = false,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT): AlignmentRecordDataset = {
    replaceRdd(MDTagging(rdd,
      referenceFile,
      overwriteExistingTags = overwriteExistingTags,
      validationStringency = validationStringency).taggedReads)
  }

  /**
   * Runs a quality control pass akin to the Samtools FlagStat tool.
   *
   * @return Returns a tuple of (failedQualityMetrics, passedQualityMetrics)
   */
  def flagStat(): (FlagStatMetrics, FlagStatMetrics) = {
    FlagStat(rdd)
  }

  /**
   * Groups all reads by read group and read name.
   *
   * @return SingleReadBuckets with primary, secondary and unmapped reads
   */
  private[read] def groupReadsByFragment(): RDD[SingleReadBucket] = {
    SingleReadBucket(rdd)
  }

  /**
   * (Java-specific) Saves these AlignmentRecords to two FASTQ files.
   *
   * The files are one for the first mate in each pair, and the other for the
   * second mate in the pair.
   *
   * @param fileName1 Path at which to save a FASTQ file containing the first
   *   mate of each pair.
   * @param fileName2 Path at which to save a FASTQ file containing the second
   *   mate of each pair.
   * @param outputOriginalBaseQualities If true, writes out reads with the base
   *   qualities from the original qualities (SAM "OQ") field. If false, writes
   *   out reads with the base qualities from the qual field. Default is false.
   * @param asSingleFile If false, writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param validationStringency Iff strict, throw an exception if any read in
   *   this genomic dataset is not accompanied by its mate.
   * @param persistLevel The persistence level to cache reads at between passes.
   */
  def saveAsPairedFastq(
    fileName1: String,
    fileName2: String,
    outputOriginalBaseQualities: java.lang.Boolean,
    asSingleFile: java.lang.Boolean,
    disableFastConcat: java.lang.Boolean,
    validationStringency: ValidationStringency,
    persistLevel: StorageLevel) {
    saveAsPairedFastq(fileName1, fileName2,
      outputOriginalBaseQualities = outputOriginalBaseQualities: Boolean,
      asSingleFile = asSingleFile: Boolean,
      disableFastConcat = disableFastConcat: Boolean,
      validationStringency = validationStringency,
      persistLevel = Some(persistLevel))
  }

  /**
   * Saves these AlignmentRecords to two FASTQ files.
   *
   * The files are one for the first mate in each pair, and the other for the
   * second mate in the pair.
   *
   * @param fileName1 Path at which to save a FASTQ file containing the first
   *   mate of each pair.
   * @param fileName2 Path at which to save a FASTQ file containing the second
   *   mate of each pair.
   * @param outputOriginalBaseQualities If true, writes out reads with the base
   *   qualities from the original qualities (SAM "OQ") field. If false, writes
   *   out reads with the base qualities from the qual field. Default is false.
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param validationStringency Iff strict, throw an exception if any read in
   *   this genomic dataset is not accompanied by its mate.
   * @param persistLevel An optional persistance level to set. If this level is
   *   set, then reads will be cached (at the given persistance) level between
   *   passes.
   */
  def saveAsPairedFastq(
    fileName1: String,
    fileName2: String,
    outputOriginalBaseQualities: Boolean = false,
    asSingleFile: Boolean = false,
    disableFastConcat: Boolean = false,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT,
    persistLevel: Option[StorageLevel] = None) {

    def maybePersist[T](r: RDD[T]): Unit = {
      persistLevel.foreach(r.persist(_))
    }
    def maybeUnpersist[T](r: RDD[T]): Unit = {
      persistLevel.foreach(_ => r.unpersist())
    }

    maybePersist(rdd)
    val numRecords = rdd.count()

    val readsByID: RDD[(String, Iterable[AlignmentRecord])] =
      rdd.groupBy(record => {
        if (!AlignmentRecordConverter.readNameHasPairedSuffix(record))
          record.getReadName
        else
          record.getReadName.dropRight(2)
      })

    validationStringency match {
      case ValidationStringency.STRICT | ValidationStringency.LENIENT =>
        val readIDsWithCounts: RDD[(String, Int)] = readsByID.mapValues(_.size)
        val unpairedReadIDsWithCounts: RDD[(String, Int)] = readIDsWithCounts.filter(_._2 != 2)
        maybePersist(unpairedReadIDsWithCounts)

        val numUnpairedReadIDsWithCounts: Long = unpairedReadIDsWithCounts.count()
        if (numUnpairedReadIDsWithCounts != 0) {
          val readNameOccurrencesMap: collection.Map[Int, Long] = unpairedReadIDsWithCounts.map(_._2).countByValue()

          val msg =
            List(
              s"Found $numUnpairedReadIDsWithCounts read names that don't occur exactly twice:",

              readNameOccurrencesMap.take(100).map({
                case (numOccurrences, numReadNames) => s"${numOccurrences}x:\t$numReadNames"
              }).mkString("\t", "\n\t", if (readNameOccurrencesMap.size > 100) "\n\t…" else ""),
              "",

              "Samples:",
              unpairedReadIDsWithCounts
                .take(100)
                .map(_._1)
                .mkString("\t", "\n\t", if (numUnpairedReadIDsWithCounts > 100) "\n\t…" else "")
            ).mkString("\n")

          if (validationStringency == ValidationStringency.STRICT)
            throw new IllegalArgumentException(msg)
          else if (validationStringency == ValidationStringency.LENIENT)
            logError(msg)
        }
      case ValidationStringency.SILENT =>
    }

    val pairedRecords: RDD[AlignmentRecord] = readsByID.filter(_._2.size == 2).map(_._2).flatMap(x => x)
    maybePersist(pairedRecords)
    val numPairedRecords = pairedRecords.count()

    maybeUnpersist(rdd.unpersist())

    val firstInPairRecords: RDD[AlignmentRecord] = pairedRecords.filter(_.getReadInFragment == 0)
    maybePersist(firstInPairRecords)
    val numFirstInPairRecords = firstInPairRecords.count()

    val secondInPairRecords: RDD[AlignmentRecord] = pairedRecords.filter(_.getReadInFragment == 1)
    maybePersist(secondInPairRecords)
    val numSecondInPairRecords = secondInPairRecords.count()

    maybeUnpersist(pairedRecords)

    log.info(
      "%d/%d records are properly paired: %d firsts, %d seconds".format(
        numPairedRecords,
        numRecords,
        numFirstInPairRecords,
        numSecondInPairRecords
      )
    )

    assert(
      numFirstInPairRecords == numSecondInPairRecords,
      "Different numbers of first- and second-reads: %d vs. %d".format(numFirstInPairRecords, numSecondInPairRecords)
    )

    val arc = new AlignmentRecordConverter

    val firstToWrite = firstInPairRecords
      .sortBy(_.getReadName)
      .map(record => arc.convertToFastq(record, maybeAddSuffix = true, outputOriginalBaseQualities = outputOriginalBaseQualities))

    writeTextRdd(firstToWrite,
      fileName1,
      asSingleFile = asSingleFile,
      disableFastConcat = disableFastConcat,
      optHeaderPath = None)

    val secondToWrite = secondInPairRecords
      .sortBy(_.getReadName)
      .map(record => arc.convertToFastq(record, maybeAddSuffix = true, outputOriginalBaseQualities = outputOriginalBaseQualities))

    writeTextRdd(secondToWrite,
      fileName2,
      asSingleFile = asSingleFile,
      disableFastConcat = disableFastConcat,
      optHeaderPath = None)

    maybeUnpersist(firstInPairRecords)
    maybeUnpersist(secondInPairRecords)
  }

  /**
   * (Java-specific) Saves reads in FASTQ format.
   *
   * @param fileName Path to save files at.
   * @param outputOriginalBaseQualities If true, writes out reads with the base
   *   qualities from the original qualities (SAM "OQ") field. If false, writes
   *   out reads with the base qualities from the qual field. Default is false.
   * @param sort Whether to sort the FASTQ files by read name or not. Defaults
   *   to false. Sorting the output will recover pair order, if desired.
   * @param asSingleFile If false, writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param validationStringency Iff strict, throw an exception if any read in
   *   this genomic dataset is not accompanied by its mate.
   */
  def saveAsFastq(
    fileName: String,
    outputOriginalBaseQualities: java.lang.Boolean,
    sort: java.lang.Boolean,
    asSingleFile: java.lang.Boolean,
    disableFastConcat: java.lang.Boolean,
    validationStringency: ValidationStringency) {

    saveAsFastq(fileName, fileName2Opt = None,
      outputOriginalBaseQualities = outputOriginalBaseQualities: Boolean,
      sort = sort: Boolean,
      asSingleFile = asSingleFile: Boolean,
      disableFastConcat = disableFastConcat: Boolean,
      validationStringency = validationStringency,
      persistLevel = None)
  }

  /**
   * Saves reads in FASTQ format.
   *
   * @param fileName Path to save files at.
   * @param fileName2Opt Optional second path for saving files. If set, two
   *   files will be saved.
   * @param outputOriginalBaseQualities If true, writes out reads with the base
   *   qualities from the original qualities (SAM "OQ") field. If false, writes
   *   out reads with the base qualities from the qual field. Default is false.
   * @param sort Whether to sort the FASTQ files by read name or not. Defaults
   *   to false. Sorting the output will recover pair order, if desired.
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param validationStringency Iff strict, throw an exception if any read in
   *   this genomic dataset is not accompanied by its mate.
   * @param persistLevel An optional persistance level to set. If this level is
   *   set, then reads will be cached (at the given persistance) level between
   *   passes.
   */
  def saveAsFastq(
    fileName: String,
    fileName2Opt: Option[String] = None,
    outputOriginalBaseQualities: Boolean = false,
    sort: Boolean = false,
    asSingleFile: Boolean = false,
    disableFastConcat: Boolean = false,
    validationStringency: ValidationStringency = ValidationStringency.LENIENT,
    persistLevel: Option[StorageLevel] = None) {

    log.info("Saving data in FASTQ format.")
    fileName2Opt match {
      case Some(fileName2) =>
        saveAsPairedFastq(
          fileName,
          fileName2,
          outputOriginalBaseQualities = outputOriginalBaseQualities,
          asSingleFile = asSingleFile,
          disableFastConcat = disableFastConcat,
          validationStringency = validationStringency,
          persistLevel = persistLevel
        )
      case _ =>
        val arc = new AlignmentRecordConverter

        // sort the rdd if desired
        val outputRdd = if (sort || fileName2Opt.isDefined) {
          rdd.sortBy(_.getReadName)
        } else {
          rdd
        }

        // convert the rdd and save as a text file
        val toWrite = outputRdd
          .map(record => arc.convertToFastq(record, outputOriginalBaseQualities = outputOriginalBaseQualities))

        writeTextRdd(toWrite,
          fileName,
          asSingleFile = asSingleFile,
          disableFastConcat = disableFastConcat,
          optHeaderPath = None)
    }
  }

  /**
   * (Java-specific) Reassembles read pairs from two sets of unpaired reads. The assumption is that the two sets
   * were _originally_ paired together.
   *
   * @note The RDD that this is called on should be the RDD with the first read from the pair.
   * @param secondPairRdd The rdd containing the second read from the pairs.
   * @param validationStringency How stringently to validate the reads.
   * @return Returns a genomic dataset with the pair information recomputed.
   */
  def reassembleReadPairs(
    secondPairRdd: JavaRDD[AlignmentRecord],
    validationStringency: ValidationStringency): AlignmentRecordDataset = {
    reassembleReadPairs(secondPairRdd.rdd,
      validationStringency = validationStringency)
  }

  /**
   * (Scala-specific) Reassembles read pairs from two sets of unpaired reads. The assumption is that the two sets
   * were _originally_ paired together.
   *
   * @note The RDD that this is called on should be the RDD with the first read from the pair.
   * @param secondPairRdd The rdd containing the second read from the pairs.
   * @param validationStringency How stringently to validate the reads.
   * @return Returns a genomic dataset with the pair information recomputed.
   */
  def reassembleReadPairs(
    secondPairRdd: RDD[AlignmentRecord],
    validationStringency: ValidationStringency = ValidationStringency.LENIENT): AlignmentRecordDataset = {
    // cache rdds
    val firstPairRdd = rdd.cache()
    secondPairRdd.cache()

    val firstRDDKeyedByReadName = firstPairRdd.keyBy(_.getReadName.dropRight(2))
    val secondRDDKeyedByReadName = secondPairRdd.keyBy(_.getReadName.dropRight(2))

    // all paired end reads should have the same name, except for the last two
    // characters, which will be _1/_2
    val joinedRDD: RDD[(String, (AlignmentRecord, AlignmentRecord))] =
      if (validationStringency == ValidationStringency.STRICT) {
        firstRDDKeyedByReadName.cogroup(secondRDDKeyedByReadName).map {
          case (readName, (firstReads, secondReads)) =>
            (firstReads.toList, secondReads.toList) match {
              case (firstRead :: Nil, secondRead :: Nil) =>
                (readName, (firstRead, secondRead))
              case _ =>
                throw new Exception(
                  "Expected %d first reads and %d second reads for name %s; expected exactly one of each:\n%s\n%s".format(
                    firstReads.size,
                    secondReads.size,
                    readName,
                    firstReads.map(_.getReadName).mkString("\t", "\n\t", ""),
                    secondReads.map(_.getReadName).mkString("\t", "\n\t", "")
                  )
                )
            }
        }

      } else {
        firstRDDKeyedByReadName.join(secondRDDKeyedByReadName)
      }

    val finalRdd = joinedRDD
      .flatMap(kv => Seq(
        AlignmentRecord.newBuilder(kv._2._1)
          .setReadPaired(true)
          .setProperPair(true)
          .setReadInFragment(0)
          .build(),
        AlignmentRecord.newBuilder(kv._2._2)
          .setReadPaired(true)
          .setProperPair(true)
          .setReadInFragment(1)
          .build()
      ))

    // uncache temp rdds
    firstPairRdd.unpersist()
    secondPairRdd.unpersist()

    // return
    replaceRdd(finalRdd)
  }

  /**
   * (Java-specific) Rewrites the quality scores of reads to place all quality scores in bins.
   *
   * Quality score binning maps all quality scores to a limited number of
   * discrete values, thus reducing the entropy of the quality score
   * distribution, and reducing the amount of space that reads consume on disk.
   *
   * @param bins The bins to use.
   * @return Reads whose quality scores are binned.
   */
  def binQualityScores(bins: java.util.List[QualityScoreBin]): AlignmentRecordDataset = {
    binQualityScores(asScalaBuffer(bins))
  }

  /**
   * (Scala-specific) Rewrites the quality scores of reads to place all quality scores in bins.
   *
   * Quality score binning maps all quality scores to a limited number of
   * discrete values, thus reducing the entropy of the quality score
   * distribution, and reducing the amount of space that reads consume on disk.
   *
   * @param bins The bins to use.
   * @return Reads whose quality scores are binned.
   */
  def binQualityScores(bins: Seq[QualityScoreBin]): AlignmentRecordDataset = {
    AlignmentRecordDataset.validateBins(bins)
    BinQualities(this, bins)
  }

  /**
   * Left normalizes the INDELs in reads containing INDELs.
   *
   * @return Returns a new genomic dataset where the reads that contained INDELs have their
   *   INDELs left normalized.
   */
  def leftNormalizeIndels(): AlignmentRecordDataset = {
    transform(rdd => {
      rdd.map(r => {
        if (!r.getReadMapped || r.getCigar == null) {
          r
        } else {
          val origCigar = r.getCigar
          val newCigar = NormalizationUtils.leftAlignIndel(r).toString

          // update cigar if changed
          if (origCigar != newCigar) {
            AlignmentRecord.newBuilder(r)
              .setCigar(newCigar)
              .build
          } else {
            r
          }
        }
      })
    })
  }

  /**
   * Filter this AlignmentRecordDataset by mapping quality.
   *
   * @param minimumMappingQuality Minimum mapping quality to filter by, inclusive.
   * @return AlignmentRecordDataset filtered by mapping quality.
   */
  def filterByMappingQuality(minimumMappingQuality: Int): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(g => Option(g.getMappingQuality).exists(_ >= minimumMappingQuality)))
  }

  /**
   * Filter unaligned reads from this AlignmentRecordDataset.
   *
   * @return AlignmentRecordDataset filtered to remove unaligned reads.
   */
  def filterUnalignedReads(): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(_.getReadMapped))
  }

  /**
   * Filter unpaired reads from this AlignmentRecordDataset.
   *
   * @return AlignmentRecordDataset filtered to remove unpaired reads.
   */
  def filterUnpairedReads(): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(_.getReadPaired))
  }

  /**
   * Filter duplicate reads from this AlignmentRecordDataset.
   *
   * @return AlignmentRecordDataset filtered to remove duplicate reads.
   */
  def filterDuplicateReads(): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(!_.getDuplicateRead))
  }

  /**
   * Filter this AlignmentRecordDataset to include only primary alignments.
   *
   * @return AlignmentRecordDataset filtered to include only primary alignments.
   */
  def filterToPrimaryAlignments(): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(_.getPrimaryAlignment))
  }

  /**
   * Filter this AlignmentRecordDataset by read group to those that match the specified read group.
   *
   * @param readGroupId Read group to filter by.
   * @return AlignmentRecordDataset filtered by read group.
   */
  def filterToReadGroup(readGroupId: String): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(g => Option(g.getReadGroupId).exists(_ == readGroupId)))
  }

  /**
   * (Java-specific) Filter this AlignmentRecordDataset by read group to those that match the specified read groups.
   *
   * @param readGroupIds List of read groups to filter by.
   * @return AlignmentRecordDataset filtered by one or more read groups.
   */
  def filterToReadGroups(readGroupIds: java.util.List[String]): AlignmentRecordDataset = {
    filterToReadGroups(asScalaBuffer(readGroupIds))
  }

  /**
   * (Scala-specific) Filter this AlignmentRecordDataset by read group to those that match the specified read groups.
   *
   * @param readGroupIds Sequence of read groups to filter by.
   * @return AlignmentRecordDataset filtered by one or more read groups.
   */
  def filterToReadGroups(readGroupIds: Seq[String]): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(g => Option(g.getReadGroupId).exists(readGroupIds.contains(_))))
  }

  /**
   * Filter this AlignmentRecordDataset by sample to those that match the specified sample.
   *
   * @param readGroupSampleId Sample to filter by.
   * @return AlignmentRecordDataset filtered by the specified sample.
   */
  def filterToSample(readGroupSampleId: String): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(g => Option(g.getReadGroupSampleId).exists(_ == readGroupSampleId)))
  }

  /**
   * (Java-specific) Filter this AlignmentRecordDataset by sample to those that match the specified samples.
   *
   * @param readGroupSampleIds List of samples to filter by.
   * @return AlignmentRecordDataset filtered by the specified samples.
   */
  def filterToSamples(readGroupSampleIds: java.util.List[String]): AlignmentRecordDataset = {
    filterToSamples(asScalaBuffer(readGroupSampleIds))
  }

  /**
   * (Scala-specific) Filter this AlignmentRecordDataset by sample to those that match the specified samples.
   *
   * @param readGroupSampleIds Sequence of samples to filter by.
   * @return AlignmentRecordDataset filtered by the specified samples.
   */
  def filterToSamples(readGroupSampleIds: Seq[String]): AlignmentRecordDataset = {
    transform(rdd => rdd.filter(g => Option(g.getReadGroupSampleId).exists(readGroupSampleIds.contains(_))))
  }
}
