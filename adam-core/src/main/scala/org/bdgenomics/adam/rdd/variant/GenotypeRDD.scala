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

import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{
  GenomicRangePartitioner,
  JavaSaveArgs,
  MultisampleAvroGenomicRDD,
  SortedGenomicRDD
}
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Genotype => GenotypeProduct }
import org.bdgenomics.utils.cli.SaveArgs
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.bdgenomics.formats.avro.{ Genotype, Sample }
import scala.reflect.ClassTag

private[adam] case class GenotypeArray(
    array: Array[(ReferenceRegion, Genotype)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Genotype] {

  def duplicate(): IntervalArray[ReferenceRegion, Genotype] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Genotype)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Genotype] = {
    GenotypeArray(arr, maxWidth)
  }
}

private[adam] class GenotypeArraySerializer extends IntervalArraySerializer[ReferenceRegion, Genotype, GenotypeArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Genotype]

  protected def builder(arr: Array[(ReferenceRegion, Genotype)],
                        maxIntervalWidth: Long): GenotypeArray = {
    GenotypeArray(arr, maxIntervalWidth)
  }
}

object GenotypeRDD extends Serializable {

  /**
   * An RDD containing genotypes called in a set of samples against a given
   * reference genome.
   *
   * @param rdd Called genotypes.
   * @param sequences A dictionary describing the reference genome.
   * @param samples The samples called.
   * @param headerLines The VCF header lines that cover all INFO/FORMAT fields
   *   needed to represent this RDD of Genotypes.
   */
  def apply(rdd: RDD[Genotype],
            sequences: SequenceDictionary,
            samples: Seq[Sample],
            headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines): GenotypeRDD = {
    RDDBoundGenotypeRDD(rdd, sequences, samples, headerLines)
  }

  /**
   * An RDD containing genotypes called in a set of samples against a given
   * reference genome, populated from a SQL Dataset.
   *
   * @param ds Called genotypes.
   * @param sequences A dictionary describing the reference genome.
   * @param samples The samples called.
   * @param headerLines The VCF header lines that cover all INFO/FORMAT fields
   *   needed to represent this RDD of Genotypes.
   */
  def apply(ds: Dataset[GenotypeProduct],
            sequences: SequenceDictionary,
            samples: Seq[Sample],
            headerLines: Seq[VCFHeaderLine]): GenotypeRDD = {
    DatasetBoundGenotypeRDD(ds, sequences, samples, headerLines)
  }
}

case class ParquetUnboundGenotypeRDD private[rdd] (
    private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine]) extends GenotypeRDD {

  lazy val rdd: RDD[Genotype] = {
    sc.loadParquet(parquetFilename)
  }

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[GenotypeProduct]
  }
}

case class SortedParquetUnboundGenotypeRDD private[rdd] (
  private val sc: SparkContext,
  private val parquetFilename: String,
  sequences: SequenceDictionary,
  @transient samples: Seq[Sample],
  @transient headerLines: Seq[VCFHeaderLine]) extends GenotypeRDD
    with SortedGenotypeRDD {

  lazy val partitioner: Partitioner = {
    GenomicRangePartitioner.fromRdd(flattenRddByRegions(), sequences)
  }

  lazy val rdd: RDD[Genotype] = {
    sc.loadParquet(parquetFilename)
  }

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[GenotypeProduct]
  }
}

case class DatasetBoundGenotypeRDD private[rdd] (
    dataset: Dataset[GenotypeProduct],
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines) extends GenotypeRDD {

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
    tFn: Dataset[GenotypeProduct] => Dataset[GenotypeProduct]): GenotypeRDD = {
    copy(dataset = tFn(dataset))
  }
}

case class RDDBoundGenotypeRDD private[rdd] (
    rdd: RDD[Genotype],
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines) extends GenotypeRDD {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[GenotypeProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(GenotypeProduct.fromAvro))
  }
}

private[rdd] object SortedRDDBoundGenotypeRDD {

  def apply(rdd: RDD[Genotype],
            sequences: SequenceDictionary,
            samples: Seq[Sample],
            headerLines: Seq[VCFHeaderLine]): SortedRDDBoundGenotypeRDD = {
    val partitioner = GenomicRangePartitioner.fromRdd(rdd.map(v => {
      (ReferenceRegion(v), v)
    }), sequences)

    SortedRDDBoundGenotypeRDD(rdd,
      sequences,
      partitioner,
      samples,
      headerLines)
  }
}

case class SortedRDDBoundGenotypeRDD private[rdd] (
  rdd: RDD[Genotype],
  sequences: SequenceDictionary,
  partitioner: Partitioner,
  @transient samples: Seq[Sample],
  @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines) extends GenotypeRDD
    with SortedGenotypeRDD {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[GenotypeProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(GenotypeProduct.fromAvro))
  }
}

sealed trait SortedGenotypeRDD extends SortedGenomicRDD[Genotype, GenotypeRDD] {

  val samples: Seq[Sample]

  val headerLines: Seq[VCFHeaderLine]

  override protected def replaceRdd(
    newRdd: RDD[Genotype],
    isSorted: Boolean = false,
    preservesPartitioning: Boolean = false): GenotypeRDD = {
    if (isSorted) {
      SortedRDDBoundGenotypeRDD(newRdd, sequences, samples, headerLines)
    } else {
      RDDBoundGenotypeRDD(newRdd, sequences, samples, headerLines)
    }
  }
}

sealed abstract class GenotypeRDD extends MultisampleAvroGenomicRDD[Genotype, GenotypeProduct, GenotypeRDD] {

  def union(rdds: GenotypeRDD*): GenotypeRDD = {
    val iterableRdds = rdds.toSeq
    GenotypeRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      (samples ++ iterableRdds.flatMap(_.samples)).distinct,
      (headerLines ++ iterableRdds.flatMap(_.headerLines)).distinct)
  }

  protected def buildTree(rdd: RDD[(ReferenceRegion, Genotype)])(
    implicit tTag: ClassTag[Genotype]): IntervalArray[ReferenceRegion, Genotype] = {
    IntervalArray(rdd, GenotypeArray.apply(_, _))
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
    tFn: Dataset[GenotypeProduct] => Dataset[GenotypeProduct]): GenotypeRDD = {
    DatasetBoundGenotypeRDD(tFn(dataset), sequences, samples, headerLines)
  }

  /**
   * @return Returns this GenotypeRDD squared off as a VariantContextRDD.
   */
  def toVariantContextRDD(): VariantContextRDD = {
    val vcIntRdd: RDD[(RichVariant, Genotype)] = rdd.keyBy(g => {
      RichVariant.genotypeToRichVariant(g)
    })
    val vcRdd = vcIntRdd.groupByKey
      .map {
        case (v: RichVariant, g) => {
          new VariantContext(ReferencePosition(v.variant), v, g)
        }
      }

    UnorderedVariantContextRDD(vcRdd, sequences, samples, headerLines)
  }

  protected def replaceRdd(
    newRdd: RDD[Genotype],
    isSorted: Boolean = false,
    preservesPartitioning: Boolean = false): GenotypeRDD = {
    if (isSorted) {
      SortedRDDBoundGenotypeRDD(newRdd,
        sequences,
        extractPartitioner(newRdd),
        samples,
        headerLines)
    } else {
      RDDBoundGenotypeRDD(newRdd, sequences, samples, headerLines)
    }
  }

  /**
   * @param elem The genotype to get a reference region for.
   * @return Returns the singular region this genotype covers.
   */
  protected def getReferenceRegions(elem: Genotype): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
