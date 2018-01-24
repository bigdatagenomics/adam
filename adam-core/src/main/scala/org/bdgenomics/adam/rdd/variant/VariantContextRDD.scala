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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.converters.{
  DefaultHeaderLines,
  VariantContextConverter
}
import org.bdgenomics.adam.instrumentation.Timers.SaveAsVcf
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary,
  VariantContext,
  VariantContextSerializer
}
import org.bdgenomics.adam.rdd.{
  ADAMSaveAnyArgs,
  GenomicDataset,
  MultisampleGenomicRDD,
  VCFHeaderUtils,
  VCFSupportingGenomicRDD
}
import org.bdgenomics.adam.sql.{ VariantContext => VariantContextProduct }
import org.bdgenomics.adam.util.{ FileMerger, FileExtensions }
import org.bdgenomics.formats.avro.{ Contig, Sample }
import org.bdgenomics.utils.misc.Logging
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

object VariantContextRDD extends Serializable {

  /**
   * Builds a VariantContextRDD without a partition map.
   *
   * @param rdd The underlying VariantContext RDD.
   * @param sequences The sequence dictionary for the RDD.
   * @param samples The samples for the RDD.
   * @param headerLines The header lines for the RDD.
   * @return A new VariantContextRDD.
   */
  def apply(rdd: RDD[VariantContext],
            sequences: SequenceDictionary,
            samples: Iterable[Sample],
            headerLines: Seq[VCFHeaderLine]): VariantContextRDD = {
    RDDBoundVariantContextRDD(rdd, sequences, samples.toSeq, headerLines, None)
  }

  def apply(rdd: RDD[VariantContext],
            sequences: SequenceDictionary,
            samples: Iterable[Sample]): VariantContextRDD = {
    RDDBoundVariantContextRDD(rdd, sequences, samples.toSeq, null)
  }
}

case class DatasetBoundVariantContextRDD private[rdd] (
    dataset: Dataset[VariantContextProduct],
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines) extends VariantContextRDD {

  protected lazy val optPartitionMap = None

  lazy val rdd = dataset.rdd.map(_.toModel)

  override def transformDataset(
    tFn: Dataset[VariantContextProduct] => Dataset[VariantContextProduct]): VariantContextRDD = {
    copy(dataset = tFn(dataset))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): VariantContextRDD = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantContextRDD = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): VariantContextRDD = {
    copy(samples = newSamples.toSeq)
  }
}

case class RDDBoundVariantContextRDD private[rdd] (
    rdd: RDD[VariantContext],
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample],
    @transient headerLines: Seq[VCFHeaderLine] = DefaultHeaderLines.allHeaderLines,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None) extends VariantContextRDD {

  /**
   * A SQL Dataset of variant contexts.
   */
  lazy val dataset: Dataset[VariantContextProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(VariantContextProduct.fromModel))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): VariantContextRDD = {
    copy(sequences = newSequences)
  }

  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantContextRDD = {
    copy(headerLines = newHeaderLines)
  }

  def replaceSamples(newSamples: Iterable[Sample]): VariantContextRDD = {
    copy(samples = newSamples.toSeq)
  }
}

/**
 * An RDD containing VariantContexts attached to a reference and samples.
 */
sealed abstract class VariantContextRDD extends MultisampleGenomicRDD[VariantContext, VariantContextRDD] with GenomicDataset[VariantContext, VariantContextProduct, VariantContextRDD] with Logging with VCFSupportingGenomicRDD[VariantContext, VariantContextRDD] {

  @transient val uTag: TypeTag[VariantContextProduct] = typeTag[VariantContextProduct]

  val headerLines: Seq[VCFHeaderLine]

  protected def saveAvro[U <: SpecificRecordBase](pathName: String,
                                                  sc: SparkContext,
                                                  schema: Schema,
                                                  avro: Seq[U])(implicit tUag: ClassTag[U]) {

    // get our current file system
    val path = new Path(pathName)
    val fs = path.getFileSystem(sc.hadoopConfiguration)

    // get an output stream
    val os = fs.create(path)

    // set up avro for writing
    val dw = new SpecificDatumWriter[U](schema)
    val fw = new DataFileWriter[U](dw)
    fw.create(schema, os)

    // write all our records
    avro.foreach(r => fw.append(r))

    // close the file
    fw.close()
    os.close()
  }

  protected def saveSequences(filePath: String): Unit = {
    // convert sequence dictionary to avro form and save
    val contigs = sequences.toAvro

    saveAvro("%s/_seqdict.avro".format(filePath),
      rdd.context,
      Contig.SCHEMA$,
      contigs)
  }

  protected def saveSamples(filePath: String): Unit = {
    // get file to write to
    saveAvro("%s/_samples.avro".format(filePath),
      rdd.context,
      Sample.SCHEMA$,
      samples)
  }

  def saveVcfHeaders(filePath: String): Unit = {
    // write vcf headers to file
    VCFHeaderUtils.write(new VCFHeader(headerLines.toSet),
      new Path("%s/_header".format(filePath)),
      rdd.context.hadoopConfiguration,
      false,
      false)
  }

  protected def saveMetadata(filePath: String): Unit = {
    saveSequences(filePath)
    saveSamples(filePath)
    saveVcfHeaders(filePath)
  }

  def saveAsParquet(filePath: String,
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

  def transformDataset(
    tFn: Dataset[VariantContextProduct] => Dataset[VariantContextProduct]): VariantContextRDD = {
    DatasetBoundVariantContextRDD(tFn(dataset), sequences, samples, headerLines)
  }

  /**
   * Replaces the header lines attached to this RDD.
   *
   * @param newHeaderLines The new header lines to attach to this RDD.
   * @return A new RDD with the header lines replaced.
   */
  def replaceHeaderLines(newHeaderLines: Seq[VCFHeaderLine]): VariantContextRDD

  protected def buildTree(rdd: RDD[(ReferenceRegion, VariantContext)])(
    implicit tTag: ClassTag[VariantContext]): IntervalArray[ReferenceRegion, VariantContext] = {
    IntervalArray(rdd, VariantContextArray.apply(_, _))
  }

  def union(rdds: VariantContextRDD*): VariantContextRDD = {
    val iterableRdds = rdds.toSeq
    VariantContextRDD(
      rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      (samples ++ iterableRdds.flatMap(_.samples)).distinct,
      (headerLines ++ iterableRdds.flatMap(_.headerLines)).distinct)
  }

  /**
   * @return Returns a GenotypeRDD containing the Genotypes in this RDD.
   */
  def toGenotypes(): GenotypeRDD = {
    new RDDBoundGenotypeRDD(rdd.flatMap(_.genotypes),
      sequences,
      samples,
      headerLines,
      optPartitionMap = optPartitionMap)
  }

  /**
   * @return Returns the Variants in this RDD.
   */
  def toVariants(): VariantRDD = {
    new RDDBoundVariantRDD(rdd.map(_.variant.variant),
      sequences,
      headerLines.filter(hl => hl match {
        case fl: VCFFormatHeaderLine => false
        case _                       => true
      }),
      optPartitionMap = optPartitionMap)
  }

  /**
   * Converts an RDD of ADAM VariantContexts to HTSJDK VariantContexts
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
   * Converts an RDD of ADAM VariantContexts to HTSJDK VariantContexts
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
   * Converts an RDD of ADAM VariantContexts to HTSJDK VariantContexts
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
                stringency: ValidationStringency): Unit = SaveAsVcf.time {

    // TODO: Add BCF support
    val vcfFormat = VCFFormat.VCF
    val isBgzip = FileExtensions.isGzip(filePath)
    if (!FileExtensions.isVcfExt(filePath)) {
      throw new IllegalArgumentException(
        "Saw non-VCF extension for file %s. Extensions supported are .vcf<.gz, .bgz>"
          .format(filePath))
    }

    log.info(s"Writing $vcfFormat file to $filePath")

    // map samples to sample ids
    val sampleIds = samples.map(_.getSampleId)

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
      samples.map(_.getSampleId))
    header.setSequenceDictionary(sequences.toSAMSequenceDictionary)

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
   * @return Returns a new VariantContextRDD where the underlying RDD has
   *   been replaced.
   */
  protected def replaceRdd(newRdd: RDD[VariantContext],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): VariantContextRDD = {
    RDDBoundVariantContextRDD(newRdd,
      sequences,
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
