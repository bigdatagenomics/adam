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

import grizzled.slf4j.Logging
import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.util.BlockCompressedOutputStream
import htsjdk.variant.vcf.{
  VCFFormatHeaderLine,
  VCFHeader,
  VCFHeaderLine
}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.specific.{ SpecificDatumWriter, SpecificRecordBase }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.converters.{
  DefaultHeaderLines,
  VariantContextConverter
}
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary,
  VariantContext,
  VariantContextSerializer
}
import org.bdgenomics.adam.ds.{
  ADAMSaveAnyArgs,
  DatasetBoundGenomicDataset,
  GenomicDataset,
  MultisampleGenomicDataset,
  VCFHeaderUtils,
  VCFSupportingGenomicDataset
}
import org.bdgenomics.adam.sql.{ VariantContext => VariantContextProduct }
import org.bdgenomics.adam.util.{ FileMerger, FileExtensions }
import org.bdgenomics.formats.avro.{ Reference, Sample }
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.seqdoop.hadoop_bam._
import org.seqdoop.hadoop_bam.util.{
  BGZFCodec,
  BGZFEnhancedGzipCodec
}
import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class VariantContextArray(
    array: Array[(ReferenceRegion, VariantContext)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, VariantContext] {

  def duplicate(): IntervalArray[ReferenceRegion, VariantContext] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, VariantContext)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, VariantContext] = {
    VariantContextArray(arr, maxWidth)
  }
}

private[adam] class VariantContextArraySerializer extends IntervalArraySerializer[ReferenceRegion, VariantContext, VariantContextArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new VariantContextSerializer

  protected def builder(arr: Array[(ReferenceRegion, VariantContext)],
                        maxIntervalWidth: Long): VariantContextArray = {
    VariantContextArray(arr, maxIntervalWidth)
  }
}

object VariantContextDataset extends Serializable {

  /**
   * Builds a VariantContextDataset from an RDD.
   *
   * @param rdd The underlying VariantContext RDD.
   * @param references The references for the genomic dataset.
   * @param samples The samples for the genomic dataset.
   * @param headerLines The header lines for the genomic dataset.
   * @return A new VariantContextDataset.
   */
  def apply(rdd: RDD[VariantContext],
            references: Iterable[Reference],
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine]): VariantContextDataset = {

    RDDBoundVariantContextDataset(rdd, SequenceDictionary.fromAvro(references.toSeq), samples.toSeq, headerLines)
  }

  /**
   * Builds a VariantContextDataset from an RDD.
   *
   * @param rdd The underlying VariantContext RDD.
   * @param sequences The sequence dictionary for the genomic dataset.
   * @param samples The samples for the genomic dataset.
   * @param headerLines The header lines for the genomic dataset.
   * @return A new VariantContextDataset.
   */
  def apply(rdd: RDD[VariantContext],
            sequences: SequenceDictionary,
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines): VariantContextDataset = {

    RDDBoundVariantContextDataset(rdd, sequences, samples.toSeq, headerLines, None)
  }

  /**
   * Builds a VariantContextDataset from a Dataset.
   *
   * @param ds The underlying VariantContext dataset.
   * @return A new VariantContextDataset.
   */
  def apply(ds: Dataset[VariantContextProduct]): VariantContextDataset = {
    DatasetBoundVariantContextDataset(ds, SequenceDictionary.empty, Seq.empty, DefaultHeaderLines.allHeaderLines)
  }

  /**
   * Builds a VariantContextDataset from a Dataset.
   *
   * @param ds The underlying VariantContext dataset.
   * @param references The references for the genomic dataset.
   * @param samples The samples for the genomic dataset.
   * @param headerLines The header lines for the genomic dataset.
   * @return A new VariantContextDataset.
   */
  def apply(ds: Dataset[VariantContextProduct],
            references: Iterable[Reference],
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine]): VariantContextDataset = {

    DatasetBoundVariantContextDataset(ds, SequenceDictionary.fromAvro(references.toSeq), samples.toSeq, headerLines)
  }

  /**
   * Builds a VariantContextDataset from a Dataset.
   *
   * @param ds The underlying VariantContext dataset.
   * @param sequences The sequence dictionary for the genomic dataset.
   * @param samples The samples for the genomic dataset.
   * @param headerLines The header lines for the genomic dataset.
   * @return A new VariantContextDataset.
   */
  def apply(ds: Dataset[VariantContextProduct],
            sequences: SequenceDictionary,
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine]): VariantContextDataset = {

    DatasetBoundVariantContextDataset(ds, sequences, samples.toSeq, headerLines)
  }
}

case class DatasetBoundVariantContextDataset private[ds] (
  dataset: Dataset[VariantContextProduct],
  references: SequenceDictionary,
  @transient samples: Seq[Sample],
  @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends VariantContextDataset
    with DatasetBoundGenomicDataset[VariantContext, VariantContextProduct, VariantContextDataset] {

  protected lazy val optPartitionMap = None

  lazy val rdd = dataset.rdd.map(_.toModel)

  override def transformDataset(
    tFn: Dataset[VariantContextProduct] => Dataset[VariantContextProduct]): VariantContextDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[VariantContextProduct], Dataset[VariantContextProduct]]): VariantContextDataset = {
    copy(dataset = tFn.call(dataset))
  }

  def replaceReferences(
    newReferences: SequenceDictionary): VariantContextDataset = {
    copy(references = newReferences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantContextDataset = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): VariantContextDataset = {
    copy(samples = newSamples.toSeq)
  }
}

case class RDDBoundVariantContextDataset private[ds] (
    rdd: RDD[VariantContext],
    references: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None) extends VariantContextDataset {

  /**
   * A SQL Dataset of variant contexts.
   */
  lazy val dataset: Dataset[VariantContextProduct] = {
    import spark.implicits._
    spark.createDataset(rdd.map(VariantContextProduct.fromModel))
  }

  def replaceReferences(
    newReferences: SequenceDictionary): VariantContextDataset = {
    copy(references = newReferences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantContextDataset = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): VariantContextDataset = {
    copy(samples = newSamples.toSeq)
  }
}

/**
 * An genomic dataset containing VariantContexts attached to a reference and samples.
 */
sealed abstract class VariantContextDataset extends MultisampleGenomicDataset[VariantContext, VariantContextProduct, VariantContextDataset] with GenomicDataset[VariantContext, VariantContextProduct, VariantContextDataset] with Logging with VCFSupportingGenomicDataset[VariantContext, VariantContextProduct, VariantContextDataset] {

  protected val productFn = VariantContextProduct.fromModel(_)
  protected val unproductFn = (vc: VariantContextProduct) => vc.toModel

  @transient val uTag: TypeTag[VariantContextProduct] = typeTag[VariantContextProduct]

  val headerLines: Seq[VCFHeaderLine]

  def saveVcfHeaders(filePath: String): Unit = {
    // write vcf headers to file
    VCFHeaderUtils.write(new VCFHeader(headerLines.toSet),
      new Path("%s/_header".format(filePath)),
      rdd.context.hadoopConfiguration,
      false,
      false)
  }

  override protected def saveMetadata(filePath: String): Unit = {
    saveReferences(filePath)
    saveSamples(filePath)
    saveVcfHeaders(filePath)
  }

  def saveAsParquet(pathName: String,
                    blockSize: Int = 128 * 1024 * 1024,
                    pageSize: Int = 1 * 1024 * 1024,
                    compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                    disableDictionaryEncoding: Boolean = false) {
    info("Saving directly as Parquet from SQL. Options other than compression codec are ignored.")
    dataset.toDF()
      .write
      .format("parquet")
      .option("spark.sql.parquet.compression.codec", compressionCodec.toString.toLowerCase())
      .save(pathName)
    saveMetadata(pathName)
  }

  override def transformDataset(
    tFn: Dataset[VariantContextProduct] => Dataset[VariantContextProduct]): VariantContextDataset = {
    DatasetBoundVariantContextDataset(tFn(dataset), references, samples, headerLines)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[VariantContextProduct], Dataset[VariantContextProduct]]): VariantContextDataset = {
    DatasetBoundVariantContextDataset(tFn.call(dataset), references, samples, headerLines)
  }

  /**
   * Replaces the header lines attached to this genomic dataset.
   *
   * @param newHeaderLines The new header lines to attach to this genomic dataset.
   * @return A new genomic dataset with the header lines replaced.
   */
  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantContextDataset

  protected def buildTree(rdd: RDD[(ReferenceRegion, VariantContext)])(
    implicit tTag: ClassTag[VariantContext]): IntervalArray[ReferenceRegion, VariantContext] = {
    IntervalArray(rdd, VariantContextArray.apply(_, _))
  }

  def union(datasets: VariantContextDataset*): VariantContextDataset = {
    val iterableDatasets = datasets.toSeq
    VariantContextDataset(
      rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.references).fold(references)(_ ++ _),
      (samples ++ iterableDatasets.flatMap(_.samples)).distinct,
      (headerLines ++ iterableDatasets.flatMap(_.headerLines)).distinct)
  }

  /**
   * @return Returns a GenotypeDataset containing the Genotypes in this genomic dataset.
   */
  def toGenotypes(): GenotypeDataset = {
    new RDDBoundGenotypeDataset(rdd.flatMap(_.genotypes),
      references,
      samples,
      headerLines,
      optPartitionMap = optPartitionMap)
  }

  /**
   * @return Returns the Variants in this genomic dataset.
   */
  def toVariants(): VariantDataset = {
    new RDDBoundVariantDataset(rdd.map(_.variant.variant),
      references,
      headerLines.filter(hl => hl match {
        case fl: VCFFormatHeaderLine => false
        case _                       => true
      }),
      optPartitionMap = optPartitionMap)
  }

  /**
   * Converts this genomic dataset of ADAM VariantContexts to HTSJDK VariantContexts
   * and saves to disk as VCF.
   *
   * File paths that end in .gz or .bgz will be saved as block GZIP compressed VCFs.
   *
   * @param args Arguments defining where to save the file.
   * @param stringency The validation stringency to use when writing the VCF.
   *   Defaults to LENIENT.
   */
  def saveAsVcf(args: ADAMSaveAnyArgs,
                stringency: ValidationStringency = ValidationStringency.LENIENT): Unit = {
    saveAsVcf(
      args.outputPath,
      asSingleFile = args.asSingleFile,
      deferMerging = args.deferMerging,
      disableFastConcat = args.disableFastConcat,
      stringency = stringency)
  }

  /**
   * Converts this genomic dataset of ADAM VariantContexts to HTSJDK VariantContexts
   * and saves as a single file to disk as VCF. Uses lenient validation
   * stringency.
   *
   * File paths that end in .gz or .bgz will be saved as block GZIP compressed VCFs.
   *
   * @param filePath The file path to save to.
   */
  def saveAsVcf(filePath: String): Unit = {
    saveAsVcf(
      filePath,
      asSingleFile = true,
      deferMerging = false,
      disableFastConcat = false,
      stringency = ValidationStringency.LENIENT)
  }

  /**
   * Converts this genomic dataset of ADAM VariantContexts to HTSJDK VariantContexts.
   *
   * @param stringency Validation stringency to use.
   * @return Return a tuple of VCFHeader and an RDD of HTSJDK VariantContexts.
   */
  def convertToVcf(
    stringency: ValidationStringency): (VCFHeader, RDD[htsjdk.variant.variantcontext.VariantContext]) = {

    val header = new VCFHeader(
      headerLines.toSet,
      samples.map(_.getId))
    header.setSequenceDictionary(references.toSAMSequenceDictionary)

    val converter = VariantContextConverter(headerLines,
      stringency,
      rdd.context.hadoopConfiguration)

    val htsjdkVcs = rdd
      .map(vc => converter.convert(vc))
      .filter(_.isDefined)
      .map(_.get)

    (header, htsjdkVcs)
  }

  /**
   * Converts this genomic dataset of ADAM VariantContexts to HTSJDK VariantContexts
   * and saves to disk as VCF.
   *
   * File paths that end in .gz or .bgz will be saved as block GZIP compressed VCFs.
   *
   * @param filePath The file path to save to.
   * @param asSingleFile If true, saves the output as a single file by merging
   *   the sharded output after completing the write to HDFS. If false, the
   *   output of this call will be written as shards, where each shard has a
   *   valid VCF header.
   * @param deferMerging If true and asSingleFile is true, we will save the
   *   output shards as a headerless file, but we will not merge the shards.
   * @param disableFastConcat If asSingleFile is true and deferMerging is false,
   *   disables the use of the parallel file merging engine.
   * @param stringency The validation stringency to use when writing the VCF.
   */
  def saveAsVcf(filePath: String,
                asSingleFile: Boolean,
                deferMerging: Boolean,
                disableFastConcat: Boolean,
                stringency: ValidationStringency): Unit = {

    // TODO: Add BCF support
    val vcfFormat = VCFFormat.VCF
    val isBgzip = FileExtensions.isGzip(filePath)
    if (!FileExtensions.isVcfExt(filePath)) {
      throw new IllegalArgumentException(
        "Saw non-VCF extension for file %s. Extensions supported are .vcf<.gz, .bgz>"
          .format(filePath))
    }

    info(s"Writing $vcfFormat file to $filePath")

    // convert the variants to htsjdk VCs
    val converter = VariantContextConverter(headerLines,
      stringency,
      rdd.context.hadoopConfiguration)
    val writableVCs: RDD[(LongWritable, VariantContextWritable)] = rdd.flatMap(vc => {
      converter.convert(vc)
        .map(htsjdkVc => {
          val vcw = new VariantContextWritable
          vcw.set(htsjdkVc)
          (new LongWritable(vc.position.pos), vcw)
        })
    })

    // make header
    val header = new VCFHeader(
      headerLines.toSet,
      samples.map(_.getId))
    header.setSequenceDictionary(references.toSAMSequenceDictionary)

    // write header
    val headPath = new Path("%s_head".format(filePath))

    // configure things for saving to disk
    val conf = rdd.context.hadoopConfiguration
    val fs = headPath.getFileSystem(conf)

    // write vcf header
    VCFHeaderUtils.write(header,
      headPath,
      fs,
      isBgzip,
      asSingleFile)

    // set path to header file and the vcf format
    conf.set("org.bdgenomics.adam.rdd.variant.vcf_header_path", headPath.toString)
    conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, vcfFormat.toString)
    if (isBgzip) {
      conf.setStrings("io.compression.codecs",
        classOf[BGZFCodec].getCanonicalName,
        classOf[BGZFEnhancedGzipCodec].getCanonicalName)
      conf.setBoolean(FileOutputFormat.COMPRESS, true)
      conf.setClass(FileOutputFormat.COMPRESS_CODEC,
        classOf[BGZFCodec], classOf[CompressionCodec])
    }

    if (asSingleFile) {

      // disable writing header
      conf.setBoolean(KeyIgnoringVCFOutputFormat.WRITE_HEADER_PROPERTY,
        false)

      // write shards to disk
      val tailPath = "%s_tail".format(filePath)
      writableVCs.saveAsNewAPIHadoopFile(
        tailPath,
        classOf[LongWritable],
        classOf[VariantContextWritable],
        classOf[ADAMVCFOutputFormat[LongWritable]],
        conf
      )

      // optionally merge
      if (!deferMerging) {

        FileMerger.mergeFiles(rdd.context,
          fs,
          new Path(filePath),
          new Path(tailPath),
          Some(headPath),
          disableFastConcat = disableFastConcat,
          writeEmptyGzipBlock = isBgzip)
      }
    } else {

      // write shards
      writableVCs.saveAsNewAPIHadoopFile(
        filePath,
        classOf[LongWritable],
        classOf[VariantContextWritable],
        classOf[ADAMVCFOutputFormat[LongWritable]],
        conf
      )

      // remove header file
      fs.delete(headPath, true)
    }
  }

  /**
   * @param newRdd The RDD of VariantContexts to replace the underlying RDD.
   * @return Returns a new VariantContextDataset where the underlying RDD has
   *   been replaced.
   */
  protected def replaceRdd(newRdd: RDD[VariantContext],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): VariantContextDataset = {
    RDDBoundVariantContextDataset(newRdd,
      references,
      samples,
      headerLines,
      newPartitionMap)
  }

  /**
   * @param elem The variant context to get a reference region for.
   * @return Returns a seq containing the position key from the variant context.
   */
  protected def getReferenceRegions(elem: VariantContext): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem.position))
  }
}
