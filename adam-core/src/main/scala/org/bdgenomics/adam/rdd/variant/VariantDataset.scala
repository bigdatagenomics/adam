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
import org.apache.spark.api.java.function.{ Function => JFunction }
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
  DatasetBoundGenomicDataset,
  AvroGenomicDataset,
  VCFHeaderUtils,
  VCFSupportingGenomicDataset
}
import org.bdgenomics.adam.rich.RichVariant
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

object VariantDataset extends Serializable {

  /**
   * Builds a VariantDataset without a partition map.
   *
   * @param rdd The underlying Variant RDD.
   * @param sequences The sequence dictionary for the RDD.
   * @param headerLines The header lines for the RDD.
   * @return A new Variant RDD.
   */
  def apply(rdd: RDD[Variant],
            sequences: SequenceDictionary,
            headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines): VariantDataset = {

    new RDDBoundVariantDataset(rdd, sequences, headerLines, None)
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
            headerLines: Seq[VCFHeaderLine]): VariantDataset = {
    new DatasetBoundVariantDataset(ds, sequences, headerLines)
  }
}

case class ParquetUnboundVariantDataset private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary,
    @transient headerLines: Seq[VCFHeaderLine]) extends VariantDataset {

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
    newSequences: SequenceDictionary): VariantDataset = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantDataset = {
    copy(headerLines = newHeaderLines)
  }
}

case class DatasetBoundVariantDataset private[rdd] (
  dataset: Dataset[VariantProduct],
  sequences: SequenceDictionary,
  @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends VariantDataset
    with DatasetBoundGenomicDataset[Variant, VariantProduct, VariantDataset] {

  protected lazy val optPartitionMap = None

  lazy val rdd = dataset.rdd.map(_.toAvro)

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
    tFn: Dataset[VariantProduct] => Dataset[VariantProduct]): VariantDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[VariantProduct], Dataset[VariantProduct]]): VariantDataset = {
    copy(dataset = tFn.call(dataset))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): VariantDataset = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantDataset = {
    copy(headerLines = newHeaderLines)
  }

  override def filterToFiltersPassed(): VariantDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("filtersPassed")))
  }

  override def filterByQuality(minimumQuality: Double): VariantDataset = {
    transformDataset(dataset => dataset.filter(!dataset.col("splitFromMultiAllelic") && dataset.col("quality") >= minimumQuality))
  }

  override def filterByReadDepth(minimumReadDepth: Int): VariantDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("annotation.readDepth") >= minimumReadDepth))
  }

  override def filterByReferenceReadDepth(minimumReferenceReadDepth: Int): VariantDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("annotation.referenceReadDepth") >= minimumReferenceReadDepth))
  }

  override def filterSingleNucleotideVariants(): VariantDataset = {
    transformDataset(dataset => dataset.filter("LENGTH(referenceAllele) > 1 OR LENGTH(alternateAllele) > 1"))
  }

  override def filterMultipleNucleotideVariants(): VariantDataset = {
    transformDataset(dataset => dataset.filter("(LENGTH(referenceAllele) == 1 AND LENGTH(alternateAllele) == 1) OR LENGTH(referenceAllele) != LENGTH(alternateAllele)"))
  }

  override def filterIndels(): VariantDataset = {
    transformDataset(dataset => dataset.filter("LENGTH(referenceAllele) == LENGTH(alternateAllele)"))
  }

  override def filterToSingleNucleotideVariants(): VariantDataset = {
    transformDataset(dataset => dataset.filter("LENGTH(referenceAllele) == 1 AND LENGTH(alternateAllele) == 1"))
  }

  override def filterToMultipleNucleotideVariants(): VariantDataset = {
    transformDataset(dataset => dataset.filter("(LENGTH(referenceAllele) > 1 OR LENGTH(alternateAllele) > 1) AND LENGTH(referenceAllele) == LENGTH(alternateAllele)"))
  }

  override def filterToIndels(): VariantDataset = {
    transformDataset(dataset => dataset.filter("LENGTH(referenceAllele) != LENGTH(alternateAllele)"))
  }
}

case class RDDBoundVariantDataset private[rdd] (
    rdd: RDD[Variant],
    sequences: SequenceDictionary,
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None) extends VariantDataset {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[VariantProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(VariantProduct.fromAvro))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): VariantDataset = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantDataset = {
    copy(headerLines = newHeaderLines)
  }
}

sealed abstract class VariantDataset extends AvroGenomicDataset[Variant, VariantProduct, VariantDataset] with VCFSupportingGenomicDataset[Variant, VariantProduct, VariantDataset] {

  protected val productFn = VariantProduct.fromAvro(_)
  protected val unproductFn = (v: VariantProduct) => v.toAvro

  @transient val uTag: TypeTag[VariantProduct] = typeTag[VariantProduct]

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

  def union(rdds: VariantDataset*): VariantDataset = {
    val iterableRdds = rdds.toSeq
    VariantDataset(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      (headerLines ++ iterableRdds.flatMap(_.headerLines)).distinct)
  }

  override def transformDataset(
    tFn: Dataset[VariantProduct] => Dataset[VariantProduct]): VariantDataset = {
    DatasetBoundVariantDataset(tFn(dataset), sequences, headerLines)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[VariantProduct], Dataset[VariantProduct]]): VariantDataset = {
    DatasetBoundVariantDataset(tFn.call(dataset), sequences, headerLines)
  }

  /**
   * @return Returns this VariantDataset as a VariantContextDataset.
   */
  def toVariantContexts(): VariantContextDataset = {
    new RDDBoundVariantContextDataset(rdd.map(VariantContext(_)),
      sequences,
      Seq.empty[Sample],
      headerLines,
      optPartitionMap = optPartitionMap)
  }

  /**
   * Filter this VariantDataset to filters passed (VCF column 7 "FILTER" value PASS).
   *
   * @return VariantDataset filtered to filters passed.
   */
  def filterToFiltersPassed(): VariantDataset = {
    transform(rdd => rdd.filter(_.getFiltersPassed))
  }

  /**
   * Filter this VariantDataset by quality (VCF column 6 "QUAL").  Variants split
   * for multi-allelic sites will also be filtered out.
   *
   * @param minimumQuality Minimum quality to filter by, inclusive.
   * @return VariantDataset filtered by quality.
   */
  def filterByQuality(minimumQuality: Double): VariantDataset = {
    transform(rdd => rdd.filter(v => !(Option(v.getSplitFromMultiAllelic).exists(_ == true)) && Option(v.getQuality).exists(_ >= minimumQuality)))
  }

  /**
   * Filter this VariantDataset by total read depth (VCF INFO reserved key AD, Number=R,
   * split for multi-allelic sites into single integer values for the reference allele
   * (<code>filterByReferenceReadDepth</code>) and the alternate allele (this method)).
   *
   * @param minimumReadDepth Minimum total read depth to filter by, inclusive.
   * @return VariantDataset filtered by total read depth.
   */
  def filterByReadDepth(minimumReadDepth: Int): VariantDataset = {
    transform(rdd => rdd.filter(v => Option(v.getAnnotation().getReadDepth).exists(_ >= minimumReadDepth)))
  }

  /**
   * Filter this VariantDataset by reference total read depth (VCF INFO reserved key AD, Number=R,
   * split for multi-allelic sites into single integer values for the alternate allele
   * (<code>filterByReadDepth</code>) and the reference allele (this method)).
   *
   * @param minimumReferenceReadDepth Minimum reference total read depth to filter by, inclusive.
   * @return VariantDataset filtered by reference total read depth.
   */
  def filterByReferenceReadDepth(minimumReferenceReadDepth: Int): VariantDataset = {
    transform(rdd => rdd.filter(v => Option(v.getAnnotation().getReferenceReadDepth).exists(_ >= minimumReferenceReadDepth)))
  }

  /**
   * Filter single nucleotide variants (SNPs) from this VariantDataset.
   *
   * @return VariantDataset filtered to remove single nucleotide variants (SNPs).
   */
  def filterSingleNucleotideVariants() = {
    transform(rdd => rdd.filter(v => !RichVariant(v).isSingleNucleotideVariant))
  }

  /**
   * Filter multiple nucleotide variants (MNPs) from this VariantDataset.
   *
   * @return VariantDataset filtered to remove multiple nucleotide variants (MNPs).
   */
  def filterMultipleNucleotideVariants() = {
    transform(rdd => rdd.filter(v => !RichVariant(v).isMultipleNucleotideVariant))
  }

  /**
   * Filter insertions and deletions (indels) from this VariantDataset.
   *
   * @return VariantDataset filtered to remove insertions and deletions (indels).
   */
  def filterIndels() = {
    transform(rdd => rdd.filter(v => {
      val rv = RichVariant(v)
      !rv.isInsertion && !rv.isDeletion
    }))
  }

  /**
   * Filter this VariantDataset to include only single nucleotide variants (SNPs).
   *
   * @return VariantDataset filtered to include only single nucleotide variants (SNPs).
   */
  def filterToSingleNucleotideVariants() = {
    transform(rdd => rdd.filter(v => RichVariant(v).isSingleNucleotideVariant))
  }

  /**
   * Filter this VariantDataset to include only multiple nucleotide variants (MNPs).
   *
   * @return VariantDataset filtered to include only multiple nucleotide variants (MNPs).
   */
  def filterToMultipleNucleotideVariants() = {
    transform(rdd => rdd.filter(v => RichVariant(v).isMultipleNucleotideVariant))
  }

  /**
   * Filter this VariantDataset to include only insertions and deletions (indels).
   *
   * @return VariantDataset filtered to include only insertions and deletions (indels).
   */
  def filterToIndels() = {
    transform(rdd => rdd.filter(v => {
      val rv = RichVariant(v)
      rv.isInsertion || rv.isDeletion
    }))
  }

  /**
   * @param newRdd An RDD to replace the underlying RDD with.
   * @return Returns a new VariantDataset with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Variant],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): VariantDataset = {
    RDDBoundVariantDataset(newRdd, sequences, headerLines, newPartitionMap)
  }

  /**
   * @param elem The variant to get a reference region for.
   * @return Returns the singular region this variant covers.
   */
  protected def getReferenceRegions(elem: Variant): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
