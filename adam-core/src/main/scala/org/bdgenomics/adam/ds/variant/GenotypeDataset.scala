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
package org.bdgenomics.adam.ds.variant

import htsjdk.variant.vcf.{ VCFHeader, VCFHeaderLine }
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.{
  DatasetBoundGenomicDataset,
  MultisampleAvroGenomicDataset,
  VCFHeaderUtils,
  VCFSupportingGenomicDataset
}
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{
  Genotype => GenotypeProduct,
  Variant => VariantProduct,
  VariantAnnotation => VariantAnnotationProduct
}
import org.bdgenomics.utils.interval.array.{ IntervalArray, IntervalArraySerializer }
import org.bdgenomics.formats.avro.{
  Genotype,
  GenotypeAllele,
  Reference,
  Sample,
  Variant,
  VariantAnnotation
}
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

object GenotypeDataset extends Serializable {

  /**
   * Builds a GenotypeDataset from an RDD.
   *
   * @param rdd The underlying Genotype RDD.
   * @param references The references for the genomic dataset.
   * @param samples The samples for the genomic dataset.
   * @param headerLines The header lines for the genomic dataset.
   * @return A new GenotypeDataset.
   */
  def apply(rdd: RDD[Genotype],
            references: Iterable[Reference],
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine]): GenotypeDataset = {

    RDDBoundGenotypeDataset(rdd, SequenceDictionary.fromAvro(references.toSeq), samples.toSeq, headerLines, None)
  }

  /**
   * Builds a GenotypeDataset from an RDD.
   *
   * @param rdd The underlying Genotype RDD.
   * @param references The references for the genomic dataset.
   * @param samples The samples for the genomic dataset.
   * @param headerLines The header lines for the genomic dataset.
   * @return A new GenotypeDataset.
   */
  def apply(rdd: RDD[Genotype],
            sequences: SequenceDictionary,
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines): GenotypeDataset = {

    RDDBoundGenotypeDataset(rdd, sequences, samples.toSeq, headerLines, None)
  }

  /**
   * Builds a GenotypeDataset from a Dataset.
   *
   * @param ds The underlying Genotype Dataset.
   * @return A new GenotypeDataset.
   */
  def apply(ds: Dataset[GenotypeProduct]): GenotypeDataset = {
    DatasetBoundGenotypeDataset(ds, SequenceDictionary.empty, Seq.empty, DefaultHeaderLines.allHeaderLines)
  }

  /**
   * Builds a GenotypeDataset from a Dataset.
   *
   * @param ds The underlying Genotype Dataset.
   * @param references The references for the genomic dataset.
   * @param samples The samples for the genomic dataset.
   * @param headerLines The header lines for the genomic dataset.
   * @return A new GenotypeDataset.
   */

  def apply(ds: Dataset[GenotypeProduct],
            references: Iterable[Reference],
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine]): GenotypeDataset = {

    DatasetBoundGenotypeDataset(ds, SequenceDictionary.fromAvro(references.toSeq), samples.toSeq, headerLines)
  }

  /**
   * Builds a GenotypeDataset from a Dataset.
   *
   * @param ds The underlying Genotype Dataset.
   * @param sequences The sequence dictionary for the genomic dataset.
   * @param samples The samples for the genomic dataset.
   * @param headerLines The header lines for the genomic dataset.
   * @return A new GenotypeDataset.
   */

  def apply(ds: Dataset[GenotypeProduct],
            sequences: SequenceDictionary,
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine]): GenotypeDataset = {

    DatasetBoundGenotypeDataset(ds, sequences, samples.toSeq, headerLines)
  }
}

case class ParquetUnboundGenotypeDataset private[ds] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    references: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine]) extends GenotypeDataset {

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val rdd: RDD[Genotype] = {
    sc.loadParquet(parquetFilename)
  }

  lazy val dataset = {
    import spark.implicits._
    spark.read.parquet(parquetFilename).as[GenotypeProduct]
  }

  def replaceReferences(
    newReferences: SequenceDictionary): GenotypeDataset = {
    copy(references = newReferences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): GenotypeDataset = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): GenotypeDataset = {
    copy(samples = newSamples.toSeq)
  }
}

case class DatasetBoundGenotypeDataset private[ds] (
  dataset: Dataset[GenotypeProduct],
  references: SequenceDictionary,
  @transient samples: Seq[Sample],
  @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends GenotypeDataset
    with DatasetBoundGenomicDataset[Genotype, GenotypeProduct, GenotypeDataset] {

  protected lazy val optPartitionMap = None

  lazy val rdd = dataset.rdd.map(_.toAvro)

  override def saveAsParquet(filePath: String,
                             blockSize: Int = 128 * 1024 * 1024,
                             pageSize: Int = 1 * 1024 * 1024,
                             compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                             disableDictionaryEncoding: Boolean = false) {
    info("Saving directly as Parquet from SQL. Options other than compression codec are ignored.")
    dataset.toDF()
      .write
      .format("parquet")
      .option("spark.sql.parquet.compression.codec", compressionCodec.toString.toLowerCase())
      .save(filePath)
    saveMetadata(filePath)
  }

  override def transformDataset(
    tFn: Dataset[GenotypeProduct] => Dataset[GenotypeProduct]): GenotypeDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[GenotypeProduct], Dataset[GenotypeProduct]]): GenotypeDataset = {
    copy(dataset = tFn.call(dataset))
  }

  def replaceReferences(
    newReferences: SequenceDictionary): GenotypeDataset = {
    copy(references = newReferences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): GenotypeDataset = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): GenotypeDataset = {
    copy(samples = newSamples.toSeq)
  }

  override def copyVariantEndToAttribute(): GenotypeDataset = {
    def copyEnd(g: GenotypeProduct): GenotypeProduct = {
      val variant = g.variant.getOrElse(VariantProduct())
      val annotation = variant.annotation.getOrElse(VariantAnnotationProduct())
      val attributes = annotation.attributes + ("END" -> g.end.toString)
      val annotationCopy = annotation.copy(attributes = attributes)
      val variantCopy = variant.copy(annotation = Some(annotationCopy))
      g.copy(variant = Some(variantCopy))
    }
    import spark.implicits._
    transformDataset(dataset => dataset.map(copyEnd))
  }

  override def filterToFiltersPassed(): GenotypeDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("variantCallingAnnotations.filtersPassed")))
  }

  override def filterByQuality(minimumQuality: Double): GenotypeDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("genotypeQuality") >= minimumQuality))
  }

  override def filterByReadDepth(minimumReadDepth: Int): GenotypeDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("readDepth") >= minimumReadDepth))
  }

  override def filterByAlternateReadDepth(minimumAlternateReadDepth: Int): GenotypeDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("alternateReadDepth") >= minimumAlternateReadDepth))
  }

  override def filterByReferenceReadDepth(minimumReferenceReadDepth: Int): GenotypeDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("referenceReadDepth") >= minimumReferenceReadDepth))
  }

  override def filterToReferenceName(referenceName: String): GenotypeDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("referenceName").eqNullSafe(referenceName)))
  }

  override def filterToSample(sampleId: String) = {
    transformDataset(dataset => dataset.filter(dataset.col("sampleId") === sampleId))
  }

  override def filterToSamples(sampleIds: Seq[String]) = {
    transformDataset(dataset => dataset.filter(dataset.col("sampleId") isin (sampleIds: _*)))
  }

  override def filterNoCalls() = {
    transformDataset(dataset => dataset.filter("!array_contains(alleles, 'NO_CALL')"))
  }
}

case class RDDBoundGenotypeDataset private[ds] (
    rdd: RDD[Genotype],
    references: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None) extends GenotypeDataset {

  /**
   * A SQL Dataset of genotypes.
   */
  lazy val dataset: Dataset[GenotypeProduct] = {
    import spark.implicits._
    spark.createDataset(rdd.map(GenotypeProduct.fromAvro))
  }

  def replaceReferences(
    newReferences: SequenceDictionary): GenotypeDataset = {
    copy(references = newReferences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): GenotypeDataset = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): GenotypeDataset = {
    copy(samples = newSamples.toSeq)
  }
}

sealed abstract class GenotypeDataset extends MultisampleAvroGenomicDataset[Genotype, GenotypeProduct, GenotypeDataset] with VCFSupportingGenomicDataset[Genotype, GenotypeProduct, GenotypeDataset] {

  protected val productFn = GenotypeProduct.fromAvro(_)
  protected val unproductFn = (g: GenotypeProduct) => g.toAvro

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
    saveReferences(filePath)
    saveSamples(filePath)
    saveVcfHeaders(filePath)
  }

  def union(datasets: GenotypeDataset*): GenotypeDataset = {
    val iterableDatasets = datasets.toSeq
    GenotypeDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.references).fold(references)(_ ++ _),
      (samples ++ iterableDatasets.flatMap(_.samples)).distinct,
      (headerLines ++ iterableDatasets.flatMap(_.headerLines)).distinct)
  }

  protected def buildTree(rdd: RDD[(ReferenceRegion, Genotype)])(
    implicit tTag: ClassTag[Genotype]): IntervalArray[ReferenceRegion, Genotype] = {
    IntervalArray(rdd, GenotypeArray.apply(_, _))
  }

  override def transformDataset(
    tFn: Dataset[GenotypeProduct] => Dataset[GenotypeProduct]): GenotypeDataset = {
    DatasetBoundGenotypeDataset(tFn(dataset), references, samples, headerLines)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[GenotypeProduct], Dataset[GenotypeProduct]]): GenotypeDataset = {
    DatasetBoundGenotypeDataset(tFn.call(dataset), references, samples, headerLines)
  }

  /**
   * @return Returns this GenotypeDataset squared off as a VariantContextDataset.
   */
  def toVariantContexts(): VariantContextDataset = {
    val vcIntRdd: RDD[(RichVariant, Genotype)] = rdd.keyBy(g => {
      RichVariant.genotypeToRichVariant(g)
    })
    val vcRdd = vcIntRdd.groupByKey
      .map {
        case (v: RichVariant, g) => {
          new VariantContext(ReferencePosition(v.variant), v, g)
        }
      }

    VariantContextDataset(vcRdd, references, samples, headerLines)
  }

  /**
   * Extracts the variants contained in this genomic dataset of genotypes.
   *
   * Does not perform any filtering looking at whether the variant was called or
   * not. Does not dedupe the variants.
   *
   * @return Returns the variants described by this GenotypeDataset.
   */
  def toVariants(): VariantDataset = {
    toVariants(dedupe = false)
  }

  /**
   * Extracts the variants contained in this genomic dataset of genotypes.
   *
   * Does not perform any filtering looking at whether the variant was called or
   * not.
   *
   * @param dedupe If true, drops variants described in more than one genotype
   *   record.
   * @return Returns the variants described by this GenotypeDataset.
   */
  def toVariants(dedupe: java.lang.Boolean): VariantDataset = {
    import spark.implicits._

    val notDedupedVariants = dataset.select($"variant.*")
      .as[VariantProduct]

    val maybeDedupedVariants = if (dedupe) {
      // we can't call dropDuplicates without specifying fields,
      // because you can't call a set operation on a schema that includes
      // map/array types
      notDedupedVariants.dropDuplicates("referenceName",
        "start",
        "end",
        "referenceAllele",
        "alternateAllele")
    } else {
      notDedupedVariants
    }

    VariantDataset(maybeDedupedVariants, references, headerLines)
  }

  /**
   * Copy variant end to a variant attribute (VCF INFO field "END").
   *
   * @return GenotypeDataset with variant end copied to a variant attribute.
   */
  def copyVariantEndToAttribute(): GenotypeDataset = {
    def copyEnd(g: Genotype): Genotype = {
      val variant = Option(g.getVariant).getOrElse(new Variant())
      val annotation = Option(variant.getAnnotation).getOrElse(new VariantAnnotation())
      val attributes = new java.util.HashMap[String, String]()
      Option(annotation.getAttributes).map(attributes.putAll(_))
      attributes.put("END", g.getEnd.toString)
      val annotationCopy = VariantAnnotation.newBuilder(annotation).setAttributes(attributes).build()
      val variantCopy = Variant.newBuilder(variant).setAnnotation(annotationCopy).build()
      Genotype.newBuilder(g).setVariant(variantCopy).build()
    }
    transform((rdd: RDD[Genotype]) => rdd.map(copyEnd))
  }

  /**
   * Filter this GenotypeDataset to genotype filters passed (VCF FORMAT field "FT" value PASS).
   *
   * @return GenotypeDataset filtered to genotype filters passed.
   */
  def filterToFiltersPassed(): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => Option(g.getVariantCallingAnnotations).exists(_.getFiltersPassed)))
  }

  /**
   * Filter this GenotypeDataset by quality (VCF FORMAT field "GQ").
   *
   * @param minimumQuality Minimum quality to filter by, inclusive.
   * @return GenotypeDataset filtered by quality.
   */
  def filterByQuality(minimumQuality: Double): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => Option(g.getGenotypeQuality).exists(_ >= minimumQuality)))
  }

  /**
   * Filter this GenotypeDataset by read depth (VCF FORMAT field "DP").
   *
   * @param minimumReadDepth Minimum read depth to filter by, inclusive.
   * @return GenotypeDataset filtered by read depth.
   */
  def filterByReadDepth(minimumReadDepth: Int): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => Option(g.getReadDepth).exists(_ >= minimumReadDepth)))
  }

  /**
   * Filter this GenotypeDataset by alternate read depth (VCF FORMAT field "AD").
   *
   * @param minimumAlternateReadDepth Minimum alternate read depth to filter by, inclusive.
   * @return GenotypeDataset filtered by alternate read depth.
   */
  def filterByAlternateReadDepth(minimumAlternateReadDepth: Int): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => Option(g.getAlternateReadDepth).exists(_ >= minimumAlternateReadDepth)))
  }

  /**
   * Filter this GenotypeDataset by reference read depth (VCF FORMAT field "AD").
   *
   * @param minimumReferenceReadDepth Minimum reference read depth to filter by, inclusive.
   * @return GenotypeDataset filtered by reference read depth.
   */
  def filterByReferenceReadDepth(minimumReferenceReadDepth: Int): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => Option(g.getReferenceReadDepth).exists(_ >= minimumReferenceReadDepth)))
  }

  /**
   * Filter this GenotypeDataset by reference name to those that match the specified reference name.
   *
   * @param referenceName Reference name to filter by.
   * @return GenotypeDataset filtered by the specified reference name.
   */
  def filterToReferenceName(referenceName: String): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => Option(g.getReferenceName).exists(_.equals(referenceName))))
  }

  /**
   * Filter this GenotypeDataset by sample to those that match the specified sample.
   *
   * @param sampleId Sample to filter by.
   * return GenotypeDataset filtered by sample.
   */
  def filterToSample(sampleId: String): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => Option(g.getSampleId).contains(sampleId)))
  }

  /**
   * (Java-specific) Filter this GenotypeDataset by sample to those that match the specified samples.
   *
   * @param sampleIds List of samples to filter by.
   * return GenotypeDataset filtered by one or more samples.
   */
  def filterToSamples(sampleIds: java.util.List[String]): GenotypeDataset = {
    filterToSamples(asScalaBuffer(sampleIds))
  }

  /**
   * (Scala-specific) Filter this GenotypeDataset by sample to those that match the specified samples.
   *
   * @param sampleIds Sequence of samples to filter by.
   * return GenotypeDataset filtered by one or more samples.
   */
  def filterToSamples(sampleIds: Seq[String]): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => Option(g.getSampleId).exists(sampleIds.contains(_))))
  }

  /**
   * Filter genotypes containing NO_CALL alleles from this GenotypeDataset.
   *
   * @return GenotypeDataset filtered to remove genotypes containing NO_CALL alleles.
   */
  def filterNoCalls(): GenotypeDataset = {
    transform((rdd: RDD[Genotype]) => rdd.filter(g => !g.getAlleles.contains(GenotypeAllele.NO_CALL)))
  }

  /**
   * @param newRdd An RDD to replace the underlying RDD with.
   * @return Returns a new GenotypeDataset with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Genotype],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): GenotypeDataset = {
    RDDBoundGenotypeDataset(newRdd, references, samples, headerLines, newPartitionMap)
  }

  /**
   * @param elem The genotype to get a reference region for.
   * @return Returns the singular region this genotype covers.
   */
  protected def getReferenceRegions(elem: Genotype): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
