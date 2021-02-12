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
package org.bdgenomics.adam.ds.sequence

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.ds.read.ReadDataset
import org.bdgenomics.adam.ds.{ AvroGenomicDataset, DatasetBoundGenomicDataset, JavaSaveArgs }
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Sequence => SequenceProduct }
import org.bdgenomics.formats.avro.{ Read, Sequence, Slice, Strand }
import org.bdgenomics.utils.interval.array.{ IntervalArray, IntervalArraySerializer }

import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class SequenceArray(
    array: Array[(ReferenceRegion, Sequence)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Sequence] {

  def duplicate(): IntervalArray[ReferenceRegion, Sequence] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Sequence)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Sequence] = {
    SequenceArray(arr, maxWidth)
  }
}

private[adam] class SequenceArraySerializer extends IntervalArraySerializer[ReferenceRegion, Sequence, SequenceArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Sequence]

  protected def builder(arr: Array[(ReferenceRegion, Sequence)],
                        maxIntervalWidth: Long): SequenceArray = {
    SequenceArray(arr, maxIntervalWidth)
  }
}

object SequenceDataset {

  /**
   * Hadoop configuration path to specify line width at
   * which to hard wrap FASTA formatted sequences. Defaults to 60.
   */
  val FASTA_LINE_WIDTH = "org.bdgenomics.adam.rdd.sequence.SequenceDataset.lineWidth"

  /**
   * A genomic dataset that wraps a dataset of Sequence data.
   *
   * @param ds A Dataset of sequences.
   */
  def apply(ds: Dataset[SequenceProduct]): SequenceDataset = {
    DatasetBoundSequenceDataset(ds, SequenceDictionary.empty)
  }

  /**
   * A genomic dataset that wraps a dataset of Sequence data.
   *
   * @param ds A Dataset of sequences.
   * @param sequences The reference genome these data are aligned to.
   */
  def apply(ds: Dataset[SequenceProduct],
            sequences: SequenceDictionary): SequenceDataset = {
    DatasetBoundSequenceDataset(ds, sequences)
  }

  /**
   * Builds a SequenceDataset with an empty sequence dictionary.
   *
   * @param rdd The underlying Sequence RDD to build from.
   * @return Returns a new SequenceDataset.
   */
  def apply(rdd: RDD[Sequence]): SequenceDataset = {
    SequenceDataset(rdd, SequenceDictionary.empty)
  }

  /**
   * Builds a SequenceDataset given a sequence dictionary.
   *
   * @param rdd The underlying Sequence RDD to build from.
   * @param sd The sequence dictionary for this SequenceDataset.
   * @return Returns a new SequenceDataset.
   */
  def apply(rdd: RDD[Sequence], sd: SequenceDictionary): SequenceDataset = {
    RDDBoundSequenceDataset(rdd, sd, None)
  }
}

case class ParquetUnboundSequenceDataset private[ds] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    references: SequenceDictionary) extends SequenceDataset {

  lazy val rdd: RDD[Sequence] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    import spark.implicits._
    spark.read.parquet(parquetFilename).as[SequenceProduct]
  }

  def replaceReferences(newReferences: SequenceDictionary): SequenceDataset = {
    copy(references = newReferences)
  }
}

case class DatasetBoundSequenceDataset private[ds] (
  dataset: Dataset[SequenceProduct],
  references: SequenceDictionary,
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends SequenceDataset
    with DatasetBoundGenomicDataset[Sequence, SequenceProduct, SequenceDataset] {

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
    tFn: Dataset[SequenceProduct] => Dataset[SequenceProduct]): SequenceDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[SequenceProduct], Dataset[SequenceProduct]]): SequenceDataset = {
    copy(dataset = tFn.call(dataset))
  }

  def replaceReferences(newReferences: SequenceDictionary): SequenceDataset = {
    copy(references = newReferences)
  }
}

case class RDDBoundSequenceDataset private[ds] (
    rdd: RDD[Sequence],
    references: SequenceDictionary,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends SequenceDataset {

  /**
   * A SQL Dataset of sequences.
   */
  lazy val dataset: Dataset[SequenceProduct] = {
    import spark.implicits._
    spark.createDataset(rdd.map(SequenceProduct.fromAvro))
  }

  def replaceReferences(newReferences: SequenceDictionary): SequenceDataset = {
    copy(references = newReferences)
  }
}

sealed abstract class SequenceDataset extends AvroGenomicDataset[Sequence, SequenceProduct, SequenceDataset] {

  protected val productFn = SequenceProduct.fromAvro(_)
  protected val unproductFn = (s: SequenceProduct) => s.toAvro

  @transient val uTag: TypeTag[SequenceProduct] = typeTag[SequenceProduct]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Sequence)])(
    implicit tTag: ClassTag[Sequence]): IntervalArray[ReferenceRegion, Sequence] = {
    IntervalArray(rdd, SequenceArray.apply(_, _))
  }

  def union(datasets: SequenceDataset*): SequenceDataset = {
    val iterableDatasets = datasets.toSeq
    SequenceDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.references).fold(references)(_ ++ _))
  }

  override def transformDataset(
    tFn: Dataset[SequenceProduct] => Dataset[SequenceProduct]): SequenceDataset = {
    DatasetBoundSequenceDataset(tFn(dataset), references)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[SequenceProduct], Dataset[SequenceProduct]]): SequenceDataset = {
    DatasetBoundSequenceDataset(tFn.call(dataset), references)
  }

  /**
   * Slice the sequences in this genomic dataset to the specified maximum length.
   *
   * @param maximumLength Maximum length.
   * @return Returns a new SliceDataset from the sequences in this genomic dataset sliced
   *    to the specified maximum length.
   */
  def slice(maximumLength: Long): SliceDataset = {
    def sliceSequence(sequence: Sequence): Seq[Slice] = {
      val slices: mutable.MutableList[Slice] = mutable.MutableList()

      val sb = Slice.newBuilder
        .setName(sequence.getName)
        .setDescription(sequence.getDescription)
        .setAlphabet(sequence.getAlphabet)
        .setSequence(sequence.getSequence)
        .setStrand(Strand.INDEPENDENT)
        .setTotalLength(sequence.getLength)
        .setAttributes(sequence.getAttributes)

      var index = 0
      var count = (sequence.getLength / maximumLength).toInt
      if (sequence.getLength % maximumLength != 0) count += 1
      for (start <- 0L until sequence.getLength by maximumLength) {
        val end = math.min(sequence.getLength, start + maximumLength)
        slices += sb
          .setStart(start)
          .setEnd(end)
          .setLength(end - start)
          .setSequence(sequence.getSequence.substring(start.toInt, end.toInt))
          .setIndex(index)
          .setSlices(count)
          .build()
        index += 1
      }
      slices
    }
    SliceDataset(rdd.flatMap(sliceSequence), references)
  }

  /**
   * Slice the specified sequence overlapping the specified region.
   *
   * @param region Region to overlap.
   * @return Returns a new Slice from the sequence overlapping the specified region.
   */
  private def slice(sequence: Sequence, region: ReferenceRegion): Slice = {
    // region may be open-ended
    val end = math.min(sequence.getLength, region.end)
    Slice.newBuilder()
      .setName(sequence.getName)
      .setDescription(sequence.getDescription)
      .setAlphabet(sequence.getAlphabet)
      .setSequence(sequence.getSequence.substring(region.start.toInt, end.toInt))
      .setLength(end - region.start)
      .setTotalLength(sequence.getLength)
      .setStart(region.start)
      .setEnd(end)
      .setStrand(region.strand) // perhaps Sequence should have strand?
      .setAttributes(sequence.getAttributes)
      .build()
  }

  /**
   * Slice the sequences in this genomic dataset overlapping the specified region.
   *
   * @param region Region to overlap.
   * @return Returns a new SliceDataset from the sequences in this genomic dataset sliced
   *    to overlap the specified region.
   */
  def slice(region: ReferenceRegion): SliceDataset = {
    SliceDataset(filterByOverlappingRegion(region).rdd.map(sequence => slice(sequence, region)))
  }

  /**
   * Slice the specified sequence overlapping the specified regions.
   *
   * @param sequence Sequence to slice.
   * @param regions Regions to overlap.
   * @return Returns one or more slices from the sequence overlapping the specified regions.
   */
  private def slice(sequence: Sequence, regions: Iterable[ReferenceRegion]): Iterable[Slice] = {
    val sequenceRegion = ReferenceRegion(sequence).get
    regions.flatMap(region =>
      if (region.covers(sequenceRegion)) {
        Some(slice(sequence, region))
      } else {
        None
      })
  }

  /**
   * Slice the sequences in this genomic dataset overlapping the specified regions.
   *
   * @param regions Regions to overlap.
   * @return Returns a new SliceDataset from the sequences in this genomic dataset sliced
   *    to overlap the specified regions.
   */
  def slice(regions: Iterable[ReferenceRegion]): SliceDataset = {
    SliceDataset(filterByOverlappingRegions(regions).rdd.flatMap(sequence => slice(sequence, regions)))
  }

  /**
   * Convert this genomic dataset of sequences into reads.
   *
   * @return Returns a new ReadRDD converted from this genomic dataset of sequences.
   */
  def toReads: ReadDataset = {
    def toRead(sequence: Sequence): Read = {
      Read.newBuilder()
        .setName(sequence.getName)
        .setDescription(sequence.getDescription)
        .setAlphabet(sequence.getAlphabet)
        .setSequence(sequence.getSequence)
        .setLength(sequence.getLength)
        .setQualityScores("B" * (if (sequence.getLength == null) 0 else sequence.getLength.toInt))
        .setAttributes(sequence.getAttributes)
        .build()
    }
    ReadDataset(rdd.map(toRead), references)
  }

  /**
   * Convert this genomic dataset of sequences into slices.
   *
   * @return Returns a new SliceDataset converted from this genomic dataset of sequences.
   */
  def toSlices: SliceDataset = {
    def toSlice(sequence: Sequence): Slice = {
      Slice.newBuilder()
        .setName(sequence.getName)
        .setDescription(sequence.getDescription)
        .setAlphabet(sequence.getAlphabet)
        .setSequence(sequence.getSequence)
        .setLength(sequence.getLength)
        .setTotalLength(sequence.getLength)
        .setStart(0L)
        .setEnd(sequence.getLength)
        .setStrand(Strand.INDEPENDENT)
        .setAttributes(sequence.getAttributes)
        .build()
    }
    SliceDataset(rdd.map(toSlice), references)
  }

  /**
   * Replace the sequence dictionary for this SequenceDataset with one
   * created from the sequences in this SequenceDataset.
   *
   * @return Returns a new SequenceDataset with the sequence dictionary replaced.
   */
  def createSequenceDictionary(): SequenceDataset = {
    val sd = new SequenceDictionary(rdd.flatMap(sequence => {
      if (sequence.getName != null) {
        Some(SequenceRecord.fromSequence(sequence))
      } else {
        None
      }
    }).distinct
      .collect
      .toVector)

    replaceReferences(sd)
  }

  /**
   * Save sequences as Parquet or FASTA.
   *
   * If filename ends in .fa or .fasta, saves as FASTA. If not, saves fragments
   * to Parquet. Defaults to 60 character line length, if saving to FASTA.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   */
  def save(
    filePath: java.lang.String,
    asSingleFile: java.lang.Boolean,
    disableFastConcat: java.lang.Boolean) {
    if (filePath.endsWith(".fa") || filePath.endsWith(".fasta")) {
      saveAsFasta(filePath, asSingleFile = asSingleFile, disableFastConcat = disableFastConcat)
    } else {
      if (asSingleFile) {
        warn("asSingleFile = true ignored when saving as Parquet.")
      }
      saveAsParquet(new JavaSaveArgs(filePath))
    }
  }

  /**
   * Save sequences in FASTA format.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param lineWidth Hard wrap FASTA formatted sequence at line width, default 60.
   */
  def saveAsFasta(filePath: String,
                  asSingleFile: Boolean = false,
                  disableFastConcat: Boolean = false,
                  lineWidth: Int = 60) {

    def toFasta(sequence: Sequence): String = {
      val sb = new StringBuilder()
      sb.append(">")
      sb.append(sequence.getName)
      Option(sequence.getDescription).foreach(n => sb.append(" ").append(n))
      sequence.getSequence.grouped(lineWidth).foreach(line => {
        sb.append("\n")
        sb.append(line)
      })
      sb.toString
    }

    writeTextRdd(rdd.map(toFasta),
      filePath,
      asSingleFile = asSingleFile,
      disableFastConcat = disableFastConcat)
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @param newPartitionMap New partition map, if any.
   * @return Returns a new SequenceRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Sequence],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): SequenceDataset = {
    RDDBoundSequenceDataset(newRdd, references, newPartitionMap)
  }

  /**
   * @param sequence Sequence to extract a region from.
   * @return Returns a reference region that covers the entirety of the sequence.
   */
  protected def getReferenceRegions(sequence: Sequence): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(sequence).get)
  }
}
