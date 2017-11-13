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

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.sequence.{SequenceRDD, SliceRDD}
import org.bdgenomics.adam.rdd.{AvroGenomicRDD, JavaSaveArgs}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Read => ReadProduct }
import org.bdgenomics.formats.avro.{
  Read,
  Sequence,
  Slice,
  Strand
}
import org.bdgenomics.utils.interval.array.{IntervalArray, IntervalArraySerializer}
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

object ReadRDD {

  /**
   * A GenomicRDD that wraps a dataset of Read data.
   *
   * @param ds A Dataset of genomic Reads.
   * @param sequences The reference genome these data are aligned to.
   */
  def apply(ds: Dataset[ReadProduct],
            sequences: SequenceDictionary): ReadRDD = {
    new DatasetBoundReadRDD(ds, sequences)
  }

  /**
   * Builds a ReadRDD with an empty sequence dictionary.
   *
   * @param rdd The underlying Read RDD to build from.
   * @return Returns a new ReadRDD.
   */
  def apply(rdd: RDD[Read]): ReadRDD = {
    ReadRDD(rdd, SequenceDictionary.empty)
  }

  /**
   * Builds a ReadRDD given a sequence dictionary.
   *
   * @param rdd The underlying Read RDD to build from.
   * @param sd The sequence dictionary for this ReadRDD.
   * @return Returns a new ReadRDD.
   */
  def apply(rdd: RDD[Read], sd: SequenceDictionary): ReadRDD = {
    new RDDBoundReadRDD(rdd, sd, None)
  }
}

case class ParquetUnboundReadRDD private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary) extends ReadRDD {

  lazy val rdd: RDD[Read] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[ReadProduct]
  }

  def replaceSequences(newSequences: SequenceDictionary): ReadRDD = {
    copy(sequences = newSequences)
  }
}

case class DatasetBoundReadRDD private[rdd] (
    dataset: Dataset[ReadProduct],
    sequences: SequenceDictionary) extends ReadRDD {

  lazy val rdd = dataset.rdd.map(_.toAvro)
  protected lazy val optPartitionMap = None

  override def saveAsParquet(filePath: String,
                             blockSize: Int = 128 * 1024 * 1024,
                             pageSize: Int = 1 * 1024 * 1024,
                             compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
                             disableDictionaryEncoding: Boolean = false) {
    log.warn("Saving directly as Parquet from SQL. Options other than compression codec are ignored.")
    dataset.toDF()
      .write
      .format("parquet")
      .option("spark.sql.parquet.compression.codec", compressCodec.toString.toLowerCase())
      .save(filePath)
    saveMetadata(filePath)
  }

  override def transformDataset(
    tFn: Dataset[ReadProduct] => Dataset[ReadProduct]): ReadRDD = {
    copy(dataset = tFn(dataset))
  }

  def replaceSequences(newSequences: SequenceDictionary): ReadRDD = {
    copy(sequences = newSequences)
  }
}

case class RDDBoundReadRDD private[rdd] (
    rdd: RDD[Read],
    sequences: SequenceDictionary,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends ReadRDD {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[ReadProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(ReadProduct.fromAvro))
  }

  def replaceSequences(newSequences: SequenceDictionary): ReadRDD = {
    copy(sequences = newSequences)
  }
}

sealed abstract class ReadRDD extends AvroGenomicRDD[Read, ReadProduct, ReadRDD] {

  @transient val uTag: TypeTag[ReadProduct] = typeTag[ReadProduct]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Read)])(
    implicit tTag: ClassTag[Read]): IntervalArray[ReferenceRegion, Read] = {
    IntervalArray(rdd, ReadArray.apply(_, _))
  }

  def union(rdds: ReadRDD*): ReadRDD = {
    val iterableRdds = rdds.toSeq
    ReadRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _))
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
    tFn: Dataset[ReadProduct] => Dataset[ReadProduct]): ReadRDD = {
    DatasetBoundReadRDD(tFn(dataset), sequences)
  }

  /**
   * Convert this RDD of reads into sequences.
   *
   * @return Returns a new SequenceRDD converted from this RDD of reads.
   */
  def toSequences: SequenceRDD = {
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
    SequenceRDD(rdd.map(toSequence), sequences)
  }

  /**
   * Convert this RDD of reads into slices.
   *
   * @return Returns a new SliceRDD converted from this RDD of reads.
   */
  def toSlices: SliceRDD = {
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
    SliceRDD(rdd.map(toSlice), sequences)
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
        log.warn("asSingleFile = true ignored when saving as Parquet.")
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
   * @return Returns a new ReadRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Read],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): ReadRDD = {
    new RDDBoundReadRDD(newRdd, sequences, newPartitionMap)
  }

  /**
   * @param read Read to extract a region from.
   * @return Returns a reference region that covers the entirety of the read.
   */
  protected def getReferenceRegions(read: Read): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(read.getName, 0L, read.getLength))
  }
}
