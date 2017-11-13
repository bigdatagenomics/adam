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
package org.bdgenomics.adam.rdd.variant

import htsjdk.variant.vcf.{ VCFHeader, VCFHeaderLine }
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{
  AvroGenomicRDD,
  VCFHeaderUtils
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Variant => VariantProduct }
import org.bdgenomics.formats.avro.{ Sample, Variant }
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class VariantArray(
    array: Array[(ReferenceRegion, Variant)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Variant] {

  def duplicate(): IntervalArray[ReferenceRegion, Variant] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Variant)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Variant] = {
    VariantArray(arr, maxWidth)
  }
}

private[adam] class VariantArraySerializer extends IntervalArraySerializer[ReferenceRegion, Variant, VariantArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Variant]

  protected def builder(arr: Array[(ReferenceRegion, Variant)],
                        maxIntervalWidth: Long): VariantArray = {
    VariantArray(arr, maxIntervalWidth)
  }
}

object VariantRDD extends Serializable {

  /**
   * Builds a VariantRDD without a partition map.
   *
   * @param rdd The underlying Variant RDD.
   * @param sequences The sequence dictionary for the RDD.
   * @param headerLines The header lines for the RDD.
   * @return A new Variant RDD.
   */
  def apply(rdd: RDD[Variant],
            sequences: SequenceDictionary,
            headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines): VariantRDD = {

    new RDDBoundVariantRDD(rdd, sequences, headerLines, None)
  }

  /**
   * An dataset containing variants called against a given reference genome.
   *
   * @param ds Variants.
   * @param sequences A dictionary describing the reference genome.
   * @param headerLines The VCF header lines that cover all INFO/FORMAT fields
   *   needed to represent this RDD of Variants.
   */
  def apply(ds: Dataset[VariantProduct],
            sequences: SequenceDictionary,
            headerLines: Seq[VCFHeaderLine]): VariantRDD = {
    new DatasetBoundVariantRDD(ds, sequences, headerLines)
  }
}

case class ParquetUnboundVariantRDD private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary,
    @transient headerLines: Seq[VCFHeaderLine]) extends VariantRDD {

  lazy val rdd: RDD[Variant] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[VariantProduct]
  }

  def replaceSequences(
    newSequences: SequenceDictionary): VariantRDD = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantRDD = {
    copy(headerLines = newHeaderLines)
  }
}

case class DatasetBoundVariantRDD private[rdd] (
    dataset: Dataset[VariantProduct],
    sequences: SequenceDictionary,
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines) extends VariantRDD {

  protected lazy val optPartitionMap = None

  lazy val rdd = dataset.rdd.map(_.toAvro)

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
    tFn: Dataset[VariantProduct] => Dataset[VariantProduct]): VariantRDD = {
    copy(dataset = tFn(dataset))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): VariantRDD = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantRDD = {
    copy(headerLines = newHeaderLines)
  }
}

case class RDDBoundVariantRDD private[rdd] (
    rdd: RDD[Variant],
    sequences: SequenceDictionary,
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None) extends VariantRDD {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[VariantProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(VariantProduct.fromAvro))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): VariantRDD = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantRDD = {
    copy(headerLines = newHeaderLines)
  }
}

sealed abstract class VariantRDD extends AvroGenomicRDD[Variant, VariantProduct, VariantRDD] {

  @transient val uTag: TypeTag[VariantProduct] = typeTag[VariantProduct]

  val headerLines: Seq[VCFHeaderLine]

  /**
   * Replaces the header lines attached to this RDD.
   *
   * @param newHeaderLines The new header lines to attach to this RDD.
   * @return A new RDD with the header lines replaced.
   */
  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantRDD

  /**
   * Appends new header lines to the existing lines.
   *
   * @param headerLinesToAdd Zero or more header lines to add.
   * @return A new RDD with the new header lines added.
   */
  def addHeaderLines(headerLinesToAdd: Seq[VCFHeaderLine]): VariantRDD = {
    replaceHeaderLines(headerLines ++ headerLinesToAdd)
  }

  /**
   * Appends a new header line to the existing lines.
   *
   * @param headerLineToAdd A header line to add.
   * @return A new RDD with the new header line added.
   */
  def addHeaderLine(headerLineToAdd: VCFHeaderLine): VariantRDD = {
    addHeaderLines(Seq(headerLineToAdd))
  }

  /**
   * Save the VCF headers to disk.
   *
   * @param filePath The filepath to the file where we will save the VCF headers.
   */
  def saveVcfHeaders(filePath: String): Unit = {
    // write vcf headers to file
    VCFHeaderUtils.write(new VCFHeader(headerLines.toSet),
      new Path("%s/_header".format(filePath)),
      rdd.context.hadoopConfiguration,
      false,
      false)
  }

  override protected def saveMetadata(filePath: String): Unit = {
    savePartitionMap(filePath)
    saveSequences(filePath)
    saveVcfHeaders(filePath)
  }

  protected def buildTree(rdd: RDD[(ReferenceRegion, Variant)])(
    implicit tTag: ClassTag[Variant]): IntervalArray[ReferenceRegion, Variant] = {
    IntervalArray(rdd, VariantArray.apply(_, _))
  }

  def union(rdds: VariantRDD*): VariantRDD = {
    val iterableRdds = rdds.toSeq
    VariantRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      (headerLines ++ iterableRdds.flatMap(_.headerLines)).distinct)
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
    tFn: Dataset[VariantProduct] => Dataset[VariantProduct]): VariantRDD = {
    DatasetBoundVariantRDD(tFn(dataset), sequences, headerLines)
  }

  /**
   * @return Returns this VariantRDD as a VariantContextRDD.
   */
  def toVariantContexts(): VariantContextRDD = {
    VariantContextRDD(rdd.map(VariantContext(_)),
      sequences,
      Seq.empty[Sample],
      headerLines,
      optPartitionMap = optPartitionMap)
  }

  /**
   * @param newRdd An RDD to replace the underlying RDD with.
   * @return Returns a new VariantRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Variant],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): VariantRDD = {
    RDDBoundVariantRDD(newRdd, sequences, headerLines, newPartitionMap)
  }

  /**
   * @param elem The variant to get a reference region for.
   * @return Returns the singular region this variant covers.
   */
  protected def getReferenceRegions(elem: Variant): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
