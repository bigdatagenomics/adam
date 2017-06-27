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
import org.apache.spark.{ Partitioner, SparkContext }
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
  GenomicRangePartitioner,
  SortedGenomicRDD,
  VCFHeaderUtils
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Variant => VariantProduct }
import org.bdgenomics.formats.avro.{
  Contig,
  Sample,
  Variant
}
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

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

    new RDDBoundVariantRDD(rdd, sequences, headerLines)
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
    private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary,
    @transient headerLines: Seq[VCFHeaderLine]) extends VariantRDD {

  lazy val rdd: RDD[Variant] = {
    sc.loadParquet(parquetFilename)
  }

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[VariantProduct]
  }
}

case class SortedParquetUnboundVariantRDD private[rdd] (
  private val sc: SparkContext,
  private val parquetFilename: String,
  sequences: SequenceDictionary,
  @transient headerLines: Seq[VCFHeaderLine]) extends VariantRDD
    with SortedVariantRDD {

  lazy val partitioner: Partitioner = {
    GenomicRangePartitioner.fromRdd(flattenRddByRegions(), sequences)
  }

  lazy val rdd: RDD[Variant] = {
    sc.loadParquet(parquetFilename)
  }

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[VariantProduct]
  }

  override def toVariantContextRDD(): VariantContextRDD = {
    SortedVariantContextRDD(rdd.map(VariantContext(_)),
      sequences,
      partitioner,
      Seq.empty[Sample],
      headerLines)
  }
}

case class DatasetBoundVariantRDD private[rdd] (
    dataset: Dataset[VariantProduct],
    sequences: SequenceDictionary,
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines) extends VariantRDD {

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
}

case class RDDBoundVariantRDD private[rdd] (
    rdd: RDD[Variant],
    sequences: SequenceDictionary,
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines) extends VariantRDD {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[VariantProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(VariantProduct.fromAvro))
  }
}

private[rdd] object SortedRDDBoundVariantRDD {

  def apply(rdd: RDD[Variant],
            sequences: SequenceDictionary,
            headerLines: Seq[VCFHeaderLine]): SortedRDDBoundVariantRDD = {
    val partitioner = GenomicRangePartitioner.fromRdd(rdd.map(v => {
      (ReferenceRegion(v), v)
    }), sequences)

    new SortedRDDBoundVariantRDD(rdd,
      sequences,
      partitioner,
      headerLines)
  }
}

case class SortedRDDBoundVariantRDD private[rdd] (
  rdd: RDD[Variant],
  sequences: SequenceDictionary,
  partitioner: Partitioner,
  @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines) extends VariantRDD
    with SortedVariantRDD {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[VariantProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(VariantProduct.fromAvro))
  }

  override def toVariantContextRDD(): VariantContextRDD = {
    SortedVariantContextRDD(rdd.map(VariantContext(_)),
      sequences,
      partitioner,
      Seq.empty[Sample],
      headerLines)
  }
}

sealed trait SortedVariantRDD extends SortedGenomicRDD[Variant, VariantRDD] {

  val headerLines: Seq[VCFHeaderLine]

  override protected def replaceRdd(
    newRdd: RDD[Variant],
    isSorted: Boolean = false,
    preservesPartitioning: Boolean = false): VariantRDD = {
    if (isSorted || preservesPartitioning) {
      SortedRDDBoundVariantRDD(newRdd, sequences, partitioner, headerLines)
    } else {
      RDDBoundVariantRDD(newRdd, sequences, headerLines)
    }
  }
}

sealed abstract class VariantRDD extends AvroGenomicRDD[Variant, VariantProduct, VariantRDD] {

  val headerLines: Seq[VCFHeaderLine]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Variant)])(
    implicit tTag: ClassTag[Variant]): IntervalArray[ReferenceRegion, Variant] = {
    IntervalArray(rdd, VariantArray.apply(_, _))
  }

  override protected def saveMetadata(filePath: String) {

    SortedGenomicRDD.touchSortedMetadataFile(this,
      rdd.context,
      filePath)

    // write vcf headers to file
    VCFHeaderUtils.write(new VCFHeader(headerLines.toSet),
      new Path("%s/_header".format(filePath)),
      rdd.context.hadoopConfiguration)

    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro
    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)
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
  def toVariantContextRDD(): VariantContextRDD = {
    UnorderedVariantContextRDD(rdd.map(VariantContext(_)),
      sequences,
      Seq.empty[Sample],
      headerLines)
  }

  protected def replaceRdd(
    newRdd: RDD[Variant],
    isSorted: Boolean = false,
    preservesPartitioning: Boolean = false): VariantRDD = {
    if (isSorted) {
      SortedRDDBoundVariantRDD(newRdd,
        sequences,
        extractPartitioner(newRdd),
        headerLines)
    } else {
      RDDBoundVariantRDD(newRdd, sequences, headerLines)
    }
  }

  /**
   * @param elem The variant to get a reference region for.
   * @return Returns the singular region this variant covers.
   */
  protected def getReferenceRegions(elem: Variant): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
