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
package org.bdgenomics.adam.ds.read

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.sequence.{ SequenceDataset, SliceDataset }
import org.bdgenomics.adam.ds.{
  DatasetBoundGenomicDataset,
  AvroGenomicDataset,
  JavaSaveArgs
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Read => ReadProduct }
import org.bdgenomics.formats.avro.{
  Alignment,
  Read,
  Sequence,
  Slice,
  Strand
}
import org.bdgenomics.utils.interval.array.{ IntervalArray, IntervalArraySerializer }
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class ReadArray(
    array: Array[(ReferenceRegion, Read)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Read] {

  def duplicate(): IntervalArray[ReferenceRegion, Read] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Read)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Read] = {
    ReadArray(arr, maxWidth)
  }
}

private[adam] class ReadArraySerializer extends IntervalArraySerializer[ReferenceRegion, Read, ReadArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Read]

  protected def builder(arr: Array[(ReferenceRegion, Read)],
                        maxIntervalWidth: Long): ReadArray = {
    ReadArray(arr, maxIntervalWidth)
  }
}

object ReadDataset {

  /**
   * A genomic dataset that wraps a dataset of Read data.
   *
   * @param ds A Dataset of genomic Reads.
   */
  def apply(ds: Dataset[ReadProduct]): ReadDataset = {
    DatasetBoundReadDataset(ds, SequenceDictionary.empty)
  }

  /**
   * A genomic dataset that wraps a dataset of Read data.
   *
   * @param ds A Dataset of genomic Reads.
   * @param sequences The reference genome these data are aligned to.
   */
  def apply(ds: Dataset[ReadProduct],
            sequences: SequenceDictionary): ReadDataset = {
    DatasetBoundReadDataset(ds, sequences)
  }

  /**
   * Builds a ReadDataset with an empty sequence dictionary.
   *
   * @param rdd The underlying Read RDD to build from.
   * @return Returns a new ReadDataset.
   */
  def apply(rdd: RDD[Read]): ReadDataset = {
    ReadDataset(rdd, SequenceDictionary.empty)
  }

  /**
   * Builds a ReadDataset given a sequence dictionary.
   *
   * @param rdd The underlying Read RDD to build from.
   * @param sd The sequence dictionary for this ReadDataset.
   * @return Returns a new ReadDataset.
   */
  def apply(rdd: RDD[Read], sd: SequenceDictionary): ReadDataset = {
    RDDBoundReadDataset(rdd, sd, None)
  }
}

case class ParquetUnboundReadDataset private[ds] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    references: SequenceDictionary) extends ReadDataset {

  lazy val rdd: RDD[Read] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    import spark.implicits._
    spark.read.parquet(parquetFilename).as[ReadProduct]
  }

  def replaceReferences(newReferences: SequenceDictionary): ReadDataset = {
    copy(references = newReferences)
  }
}

case class DatasetBoundReadDataset private[ds] (
  dataset: Dataset[ReadProduct],
  references: SequenceDictionary,
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends ReadDataset
    with DatasetBoundGenomicDataset[Read, ReadProduct, ReadDataset] {

  lazy val rdd = dataset.rdd.map(_.toAvro)
  protected lazy val optPartitionMap = None

  override def saveAsParquet(filePath: String,
                             blockSize: Int = 128 * 1024 * 1024,
                             pageSize: Int = 1 * 1024 * 1024,
                             compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                             disableDictionaryEncoding: Boolean = false) {
    warn("Saving directly as Parquet from SQL. Options other than compression codec are ignored.")
    dataset.toDF()
      .write
      .format("parquet")
      .option("spark.sql.parquet.compression.codec", compressionCodec.toString.toLowerCase())
      .save(filePath)
    saveMetadata(filePath)
  }

  override def transformDataset(
    tFn: Dataset[ReadProduct] => Dataset[ReadProduct]): ReadDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[ReadProduct], Dataset[ReadProduct]]): ReadDataset = {
    copy(dataset = tFn.call(dataset))
  }

  def replaceReferences(newReferences: SequenceDictionary): ReadDataset = {
    copy(references = newReferences)
  }
}

case class RDDBoundReadDataset private[ds] (
    rdd: RDD[Read],
    references: SequenceDictionary,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends ReadDataset {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[ReadProduct] = {
    import spark.implicits._
    spark.createDataset(rdd.map(ReadProduct.fromAvro))
  }

  def replaceReferences(newReferences: SequenceDictionary): ReadDataset = {
    copy(references = newReferences)
  }
}

sealed abstract class ReadDataset extends AvroGenomicDataset[Read, ReadProduct, ReadDataset] {

  protected val productFn = ReadProduct.fromAvro(_)
  protected val unproductFn = (r: ReadProduct) => r.toAvro

  @transient val uTag: TypeTag[ReadProduct] = typeTag[ReadProduct]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Read)])(
    implicit tTag: ClassTag[Read]): IntervalArray[ReferenceRegion, Read] = {
    IntervalArray(rdd, ReadArray.apply(_, _))
  }

  def union(datasets: ReadDataset*): ReadDataset = {
    val iterableDatasets = datasets.toSeq
    ReadDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.references).fold(references)(_ ++ _))
  }

  override def transformDataset(
    tFn: Dataset[ReadProduct] => Dataset[ReadProduct]): ReadDataset = {
    DatasetBoundReadDataset(tFn(dataset), references)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[ReadProduct], Dataset[ReadProduct]]): ReadDataset = {
    DatasetBoundReadDataset(tFn.call(dataset), references)
  }

  /**
   * Convert this genomic dataset of reads into alignments.
   *
   * @return Returns a new AlignmentDataset converted from this genomic dataset of alignments.
   */
  def toAlignments: AlignmentDataset = {
    def toAlignment(read: Read): Alignment = {
      Alignment.newBuilder()
        .setReadName(read.getName)
        .setSequence(read.getSequence)
        .setQualityScores(read.getQualityScores)
        .build()
    }
    AlignmentDataset(rdd.map(toAlignment),
      references,
      ReadGroupDictionary.empty,
      processingSteps = Seq.empty)
  }

  /**
   * Convert this genomic dataset of reads into sequences.
   *
   * @return Returns a new SequenceDataset converted from this genomic dataset of reads.
   */
  def toSequences: SequenceDataset = {
    def toSequence(read: Read): Sequence = {
      Sequence.newBuilder()
        .setName(read.getName)
        .setDescription(read.getDescription)
        .setAlphabet(read.getAlphabet)
        .setSequence(read.getSequence)
        .setLength(read.getLength)
        .setAttributes(read.getAttributes)
        .build()
    }
    SequenceDataset(rdd.map(toSequence), references)
  }

  /**
   * Convert this genomic dataset of reads into slices.
   *
   * @return Returns a new SliceDataset converted from this genomic dataset of reads.
   */
  def toSlices: SliceDataset = {
    def toSlice(read: Read): Slice = {
      Slice.newBuilder()
        .setName(read.getName)
        .setDescription(read.getDescription)
        .setAlphabet(read.getAlphabet)
        .setSequence(read.getSequence)
        .setLength(read.getLength)
        .setTotalLength(read.getLength)
        .setStart(0L)
        .setEnd(read.getLength)
        .setStrand(Strand.INDEPENDENT)
        .setAttributes(read.getAttributes)
        .build()
    }
    SliceDataset(rdd.map(toSlice), references)
  }

  /**
   * Save reads as Parquet or FASTQ.
   *
   * If filename ends in .fq or .fastq, saves as FASTQ. If not, saves reads
   * to Parquet.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   */
  def save(filePath: java.lang.String, asSingleFile: java.lang.Boolean) {
    if (filePath.endsWith(".fq") || filePath.endsWith(".fastq")) {
      saveAsFastq(filePath, asSingleFile = asSingleFile)
    } else {
      if (asSingleFile) {
        warn("asSingleFile = true ignored when saving as Parquet.")
      }
      saveAsParquet(new JavaSaveArgs(filePath))
    }
  }

  /**
   * Save reads in FASTQ format.
   *
   * @param filePath Path to save files to.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param asSingleFile If true, saves output as a single file.
   */
  def saveAsFastq(filePath: String,
                  asSingleFile: Boolean = false,
                  disableFastConcat: Boolean = false) {

    def toFastq(read: Read): String = {
      val sb = new StringBuilder()
      sb.append("@")
      sb.append(read.getName)
      Option(read.getDescription).foreach(n => sb.append(" ").append(n))
      sb.append("\n")
      sb.append(read.getSequence)
      sb.append("\n+\n")
      sb.append(read.getQualityScores)
      sb.append("\n")
      sb.toString
    }

    writeTextRdd(rdd.map(toFastq),
      filePath,
      asSingleFile = asSingleFile,
      disableFastConcat = disableFastConcat)
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @param newPartitionMap New partition map, if any.
   * @return Returns a new ReadDataset with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Read],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): ReadDataset = {
    RDDBoundReadDataset(newRdd, references, newPartitionMap)
  }

  /**
   * @param read Read to extract a region from.
   * @return Returns a reference region that covers the entirety of the read.
   */
  protected def getReferenceRegions(read: Read): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(read.getName, 0L, read.getLength))
  }
}
