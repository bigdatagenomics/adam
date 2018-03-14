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
  ReferencePosition,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{
  DatasetBoundGenomicDataset,
  MultisampleAvroGenomicRDD,
  VCFHeaderUtils,
  VCFSupportingGenomicRDD
}
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{
  Genotype => GenotypeProduct,
  Variant => VariantProduct
}
import org.bdgenomics.utils.interval.array.{ IntervalArray, IntervalArraySerializer }
import org.bdgenomics.formats.avro.{ Genotype, Sample }
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

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
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines): GenotypeRDD = {
    RDDBoundGenotypeRDD(rdd, sequences, samples.toSeq, headerLines, None)
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
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine]): GenotypeRDD = {
    DatasetBoundGenotypeRDD(ds, sequences, samples.toSeq, headerLines)
  }
}

case class ParquetUnboundGenotypeRDD private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine]) extends GenotypeRDD {

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val rdd: RDD[Genotype] = {
    sc.loadParquet(parquetFilename)
  }

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[GenotypeProduct]
  }

  def replaceSequences(
    newSequences: SequenceDictionary): GenotypeRDD = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): GenotypeRDD = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): GenotypeRDD = {
    copy(samples = newSamples.toSeq)
  }
}

case class DatasetBoundGenotypeRDD private[rdd] (
  dataset: Dataset[GenotypeProduct],
  sequences: SequenceDictionary,
  @transient samples: Seq[Sample],
  @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends GenotypeRDD
    with DatasetBoundGenomicDataset[Genotype, GenotypeProduct, GenotypeRDD] {

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
    tFn: Dataset[GenotypeProduct] => Dataset[GenotypeProduct]): GenotypeRDD = {
    copy(dataset = tFn(dataset))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): GenotypeRDD = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): GenotypeRDD = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): GenotypeRDD = {
    copy(samples = newSamples.toSeq)
  }
}

case class RDDBoundGenotypeRDD private[rdd] (
    rdd: RDD[Genotype],
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None) extends GenotypeRDD {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[GenotypeProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(GenotypeProduct.fromAvro))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): GenotypeRDD = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): GenotypeRDD = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): GenotypeRDD = {
    copy(samples = newSamples.toSeq)
  }
}

sealed abstract class GenotypeRDD extends MultisampleAvroGenomicRDD[Genotype, GenotypeProduct, GenotypeRDD] with VCFSupportingGenomicRDD[Genotype, GenotypeRDD] {

  @transient val uTag: TypeTag[GenotypeProduct] = typeTag[GenotypeProduct]

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
    saveSamples(filePath)
    saveVcfHeaders(filePath)
  }

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
  def toVariantContexts(): VariantContextRDD = {
    val vcIntRdd: RDD[(RichVariant, Genotype)] = rdd.keyBy(g => {
      RichVariant.genotypeToRichVariant(g)
    })
    val vcRdd = vcIntRdd.groupByKey
      .map {
        case (v: RichVariant, g) => {
          new VariantContext(ReferencePosition(v.variant), v, g)
        }
      }

    VariantContextRDD(vcRdd, sequences, samples, headerLines)
  }

  /**
   * Extracts the variants contained in this RDD of genotypes.
   *
   * Does not perform any filtering looking at whether the variant was called or
   * not. Does not dedupe the variants.
   *
   * @return Returns the variants described by this GenotypeRDD.
   */
  def toVariants(): VariantRDD = {
    toVariants(dedupe = false)
  }

  /**
   * Extracts the variants contained in this RDD of genotypes.
   *
   * Does not perform any filtering looking at whether the variant was called or
   * not.
   *
   * @param dedupe If true, drops variants described in more than one genotype
   *   record.
   * @return Returns the variants described by this GenotypeRDD.
   */
  def toVariants(dedupe: java.lang.Boolean): VariantRDD = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._

    val notDedupedVariants = dataset.select($"variant.*")
      .as[VariantProduct]

    val maybeDedupedVariants = if (dedupe) {
      // we can't call dropDuplicates without specifying fields,
      // because you can't call a set operation on a schema that includes
      // map/array types
      notDedupedVariants.dropDuplicates("contigName",
        "start",
        "end",
        "referenceAllele",
        "alternateAllele")
    } else {
      notDedupedVariants
    }

    VariantRDD(maybeDedupedVariants, sequences, headerLines)
  }

  /**
   * @param newRdd An RDD to replace the underlying RDD with.
   * @return Returns a new GenotypeRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Genotype],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): GenotypeRDD = {
    RDDBoundGenotypeRDD(newRdd, sequences, samples, headerLines, newPartitionMap)
  }

  /**
   * @param elem The genotype to get a reference region for.
   * @return Returns the singular region this genotype covers.
   */
  protected def getReferenceRegions(elem: Genotype): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
