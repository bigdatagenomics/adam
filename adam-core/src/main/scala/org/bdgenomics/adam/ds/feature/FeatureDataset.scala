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
package org.bdgenomics.adam.ds.feature

import com.google.common.collect.ComparisonChain
import java.util.{ Collections, Comparator }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds._
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Feature => FeatureProduct }
import org.bdgenomics.adam.util.FileMerger
import org.bdgenomics.formats.avro.{ Sample, Feature, Strand }
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import scala.collection.JavaConversions._
import scala.math.max
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class FeatureArray(
    array: Array[(ReferenceRegion, Feature)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Feature] {

  def duplicate(): IntervalArray[ReferenceRegion, Feature] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Feature)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Feature] = {
    FeatureArray(arr, maxWidth)
  }
}

private[adam] class FeatureArraySerializer extends IntervalArraySerializer[ReferenceRegion, Feature, FeatureArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Feature]

  protected def builder(arr: Array[(ReferenceRegion, Feature)],
                        maxIntervalWidth: Long): FeatureArray = {
    FeatureArray(arr, maxIntervalWidth)
  }
}

private trait FeatureOrdering[T <: Feature] extends Ordering[T] {
  def allowNull(s: java.lang.String): java.lang.Integer = {
    if (s == null) {
      return null
    }
    java.lang.Integer.parseInt(s)
  }

  def compare(x: Feature, y: Feature) = {
    val doubleNullsLast: Comparator[java.lang.Double] = com.google.common.collect.Ordering.natural().nullsLast()
    val intNullsLast: Comparator[java.lang.Integer] = com.google.common.collect.Ordering.natural().nullsLast()
    val strandNullsLast: Comparator[Strand] = com.google.common.collect.Ordering.natural().nullsLast()
    val stringNullsLast: Comparator[java.lang.String] = com.google.common.collect.Ordering.natural().nullsLast()
    // use ComparisonChain to safely handle nulls, as Feature is a java object
    ComparisonChain.start()
      // consider reference region first
      .compare(x.getReferenceName, y.getReferenceName)
      .compare(x.getStart, y.getStart)
      .compare(x.getEnd, y.getEnd)
      .compare(x.getStrand, y.getStrand, strandNullsLast)
      // then feature fields
      .compare(x.getFeatureId, y.getFeatureId, stringNullsLast)
      .compare(x.getFeatureType, y.getFeatureType, stringNullsLast)
      .compare(x.getName, y.getName, stringNullsLast)
      .compare(x.getSource, y.getSource, stringNullsLast)
      .compare(x.getPhase, y.getPhase, intNullsLast)
      .compare(x.getFrame, y.getFrame, intNullsLast)
      .compare(x.getScore, y.getScore, doubleNullsLast)
      // finally gene structure
      .compare(x.getGeneId, y.getGeneId, stringNullsLast)
      .compare(x.getTranscriptId, y.getTranscriptId, stringNullsLast)
      .compare(x.getExonId, y.getExonId, stringNullsLast)
      .compare(allowNull(x.getAttributes.get("exon_number")), allowNull(y.getAttributes.get("exon_number")), intNullsLast)
      .compare(allowNull(x.getAttributes.get("intron_number")), allowNull(y.getAttributes.get("intron_number")), intNullsLast)
      .compare(allowNull(x.getAttributes.get("rank")), allowNull(y.getAttributes.get("rank")), intNullsLast)
      .result()
  }
}
private object FeatureOrdering extends FeatureOrdering[Feature] {}

object FeatureDataset {

  /**
   * A GenomicDataset that wraps a Dataset of Feature data with an empty sequence dictionary.
   *
   * @param ds A Dataset of genomic Features.
   */
  def apply(ds: Dataset[FeatureProduct]): FeatureDataset = {
    new DatasetBoundFeatureDataset(ds, SequenceDictionary.empty, Seq.empty[Sample])
  }

  /**
   * A GenomicDataset that wraps a Dataset of Feature data given a sequence dictionary.
   *
   * @param ds A Dataset of genomic Features.
   * @param sd The reference genome these data are aligned to.
   */
  def apply(ds: Dataset[FeatureProduct],
            sequences: SequenceDictionary,
            samples: Iterable[Sample]): FeatureDataset = {
    new DatasetBoundFeatureDataset(ds, sequences, samples.toSeq)
  }

  /**
   * Builds a FeatureDataset that wraps an RDD of Feature data with an empty sequence dictionary.
   *
   * @param rdd The underlying Feature RDD to build from.
   * @return Returns a new FeatureDataset.
   */
  def apply(rdd: RDD[Feature]): FeatureDataset = {
    FeatureDataset(rdd, SequenceDictionary.empty, Iterable.empty[Sample])
  }

  /**
   * Builds a FeatureDataset that wraps an RDD of Feature data given a sequence dictionary.
   *
   * @param rdd The underlying Feature RDD to build from.
   * @param sd The sequence dictionary for this FeatureDataset.
   * @param samples The samples in this FeatureDataset.
   * @return Returns a new FeatureDataset.
   */
  def apply(rdd: RDD[Feature],
            sd: SequenceDictionary,
            samples: Iterable[Sample]): FeatureDataset = {
    new RDDBoundFeatureDataset(rdd, sd, samples.toSeq, None)
  }

  /**
   * @param feature Feature to convert to GTF format.
   * @return Returns this feature as a GTF line.
   */
  private[feature] def toGtf(feature: Feature): String = {
    def escape(entry: (Any, Any)): String = {
      entry._1 + " \"" + entry._2 + "\""
    }

    val seqname = feature.getReferenceName
    val source = Option(feature.getSource).getOrElse(".")
    val featureType = Option(feature.getFeatureType).getOrElse(".")
    val start = feature.getStart + 1 // GTF/GFF ranges are 1-based
    val end = feature.getEnd // GTF/GFF ranges are closed
    val score = Option(feature.getScore).getOrElse(".")
    val strand = Features.asString(feature.getStrand)
    val frame = Option(feature.getFrame).getOrElse(".")
    val attributes = Features.gatherAttributes(feature).map(escape).mkString("; ")
    List(seqname, source, featureType, start, end, score, strand, frame, attributes).mkString("\t")
  }

  /**
   * @param feature Feature to write in IntervalList format.
   * @return Feature as a one line interval list string.
   */
  private[ds] def toInterval(feature: Feature): String = {
    val sequenceName = feature.getReferenceName
    val start = feature.getStart + 1 // IntervalList ranges are 1-based
    val end = feature.getEnd // IntervalList ranges are closed
    val strand = Features.asString(feature.getStrand, emptyUnknown = false)
    val name = Features.nameOf(feature)
    List(sequenceName, start, end, strand, name).mkString("\t")
  }

  /**
   * @param feature Feature to write in the narrow peak format.
   * @return Returns this feature as a single narrow peak line.
   */
  private[ds] def toNarrowPeak(feature: Feature): String = {
    val chrom = feature.getReferenceName
    val start = feature.getStart
    val end = feature.getEnd
    val name = Features.nameOf(feature)
    val score = Option(feature.getScore).map(_.toInt).getOrElse(".")
    val strand = Features.asString(feature.getStrand)
    val signalValue = feature.getAttributes.getOrElse("signalValue", "0")
    val pValue = feature.getAttributes.getOrElse("pValue", "-1")
    val qValue = feature.getAttributes.getOrElse("qValue", "-1")
    val peak = feature.getAttributes.getOrElse("peak", "-1")
    List(chrom, start, end, name, score, strand, signalValue, pValue, qValue, peak).mkString("\t")
  }

  /**
   * @param feature Feature to write in BED format.
   * @return Returns the feature as a single line BED string.
   */
  private[ds] def toBed(feature: Feature): String = {
    toBed(feature, None, None, None)
  }

  /**
   * @param feature Feature to write in BED format.
   * @return Returns the feature as a single line BED string.
   */
  private[ds] def toBed(feature: Feature,
                        minimumScore: Option[Double],
                        maximumScore: Option[Double],
                        missingValue: Option[Int]): String = {

    val chrom = feature.getReferenceName
    val start = feature.getStart
    val end = feature.getEnd
    val name = Features.nameOf(feature)
    val strand = Features.asString(feature.getStrand)

    val score = if (minimumScore.isDefined && maximumScore.isDefined && missingValue.isDefined) {
      Features.interpolateScore(feature.getScore, minimumScore.get, maximumScore.get, missingValue.get)
    } else {
      Features.formatScore(feature.getScore)
    }

    if (!feature.getAttributes.containsKey("thickStart") &&
      !feature.getAttributes.containsKey("itemRgb") &&
      !feature.getAttributes.containsKey("blockCount")) {
      // write BED6 format
      List(chrom, start, end, name, score, strand).mkString("\t")
    } else {
      // write BED12 format
      val thickStart = feature.getAttributes.getOrElse("thickStart", ".")
      val thickEnd = feature.getAttributes.getOrElse("thickEnd", ".")
      val itemRgb = feature.getAttributes.getOrElse("itemRgb", ".")
      val blockCount = feature.getAttributes.getOrElse("blockCount", ".")
      val blockSizes = feature.getAttributes.getOrElse("blockSizes", ".")
      val blockStarts = feature.getAttributes.getOrElse("blockStarts", ".")
      List(chrom, start, end, name, score, strand, thickStart, thickEnd, itemRgb, blockCount, blockSizes, blockStarts).mkString("\t")
    }
  }

  /**
   * @param feature Feature to write in GFF3 format.
   * @return Returns this feature as a single line GFF3 string.
   */
  private[ds] def toGff3(feature: Feature): String = {
    def escape(entry: (Any, Any)): String = {
      entry._1 + "=" + entry._2
    }

    val seqid = feature.getReferenceName
    val source = Option(feature.getSource).getOrElse(".")
    val featureType = Option(feature.getFeatureType).getOrElse(".")
    val start = feature.getStart + 1 // GFF3 coordinate system is 1-based
    val end = feature.getEnd // GFF3 ranges are closed
    val score = Option(feature.getScore).getOrElse(".")
    val strand = Features.asString(feature.getStrand)
    val phase = Option(feature.getPhase).getOrElse(".")
    val attributes = Features.gatherAttributes(feature).map(escape).mkString(";")
    List(seqid, source, featureType, start, end, score, strand, phase, attributes).mkString("\t")
  }
}

case class ParquetUnboundFeatureDataset private[ds] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    references: SequenceDictionary,
    @transient samples: Seq[Sample]) extends FeatureDataset {

  lazy val rdd: RDD[Feature] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    import spark.implicits._
    spark.read.parquet(parquetFilename).as[FeatureProduct]
  }

  override def replaceReferences(newReferences: SequenceDictionary): FeatureDataset = {
    copy(references = newReferences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): FeatureDataset = {
    copy(samples = newSamples.toSeq)
  }

  def toCoverage(): CoverageDataset = {
    ParquetUnboundCoverageDataset(sc, parquetFilename, references, samples)
  }
}

case class DatasetBoundFeatureDataset private[ds] (
  dataset: Dataset[FeatureProduct],
  references: SequenceDictionary,
  @transient samples: Seq[Sample],
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends FeatureDataset
    with DatasetBoundGenomicDataset[Feature, FeatureProduct, FeatureDataset] {

  lazy val rdd = dataset.rdd.map(_.toAvro)
  protected lazy val optPartitionMap = None

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
    tFn: Dataset[FeatureProduct] => Dataset[FeatureProduct]): FeatureDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[FeatureProduct], Dataset[FeatureProduct]]): FeatureDataset = {
    copy(dataset = tFn.call(dataset))
  }

  override def replaceReferences(newReferences: SequenceDictionary): FeatureDataset = {
    copy(references = newReferences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): FeatureDataset = {
    copy(samples = newSamples.toSeq)
  }

  def toCoverage(): CoverageDataset = {
    import spark.implicits._
    DatasetBoundCoverageDataset(dataset.toDF
      .select("referenceName", "start", "end", "score", "sampleId")
      .withColumnRenamed("score", "count")
      .withColumnRenamed("sampleId", "optSampleId")
      .as[Coverage], references, samples)
  }

  override def filterToFeatureType(featureType: String): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("featureType").eqNullSafe(featureType)))
  }

  override def filterToFeatureTypes(featureTypes: Seq[String]): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("featureType") isin (featureTypes: _*)))
  }

  override def filterToGene(geneId: String): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("geneId").eqNullSafe(geneId)))
  }

  override def filterToGenes(geneIds: Seq[String]): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("geneId") isin (geneIds: _*)))
  }

  override def filterToTranscript(transcriptId: String): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("transcriptId").eqNullSafe(transcriptId)))
  }

  override def filterToTranscripts(transcriptIds: Seq[String]): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("transcriptId") isin (transcriptIds: _*)))
  }

  override def filterToExon(exonId: String): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("exonId").eqNullSafe(exonId)))
  }

  override def filterToExons(exonIds: Seq[String]): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("exonId") isin (exonIds: _*)))
  }

  override def filterByScore(minimumScore: Double): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("score").geq(minimumScore)))
  }

  override def filterToParent(parentId: String): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("parentIds").contains(parentId)))
  }

  override def filterToParents(parentIds: Seq[String]): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("parentIds") isin (parentIds: _*)))
  }

  override def filterToReferenceName(referenceName: String): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("referenceName").eqNullSafe(referenceName)))
  }

  override def filterByAttribute(key: String, value: String): FeatureDataset = {
    transformDataset(dataset => dataset.filter(dataset.col("attributes").getItem(key).eqNullSafe(value)))
  }
}

case class RDDBoundFeatureDataset private[ds] (
    rdd: RDD[Feature],
    references: SequenceDictionary,
    @transient samples: Seq[Sample],
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends FeatureDataset {

  /**
   * A SQL Dataset of reads.
   */
  lazy val dataset: Dataset[FeatureProduct] = {
    import spark.implicits._
    spark.createDataset(rdd.map(FeatureProduct.fromAvro))
  }

  override def replaceReferences(newReferences: SequenceDictionary): FeatureDataset = {
    copy(references = newReferences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): FeatureDataset = {
    copy(samples = newSamples.toSeq)
  }

  def toCoverage(): CoverageDataset = {
    val coverageRdd = rdd.map(f => Coverage(f))
    RDDBoundCoverageDataset(coverageRdd, references, samples, optPartitionMap)
  }
}

sealed abstract class FeatureDataset extends AvroGenomicDataset[Feature, FeatureProduct, FeatureDataset]
    with MultisampleGenomicDataset[Feature, FeatureProduct, FeatureDataset] {

  protected val productFn = FeatureProduct.fromAvro(_)
  protected val unproductFn = (f: FeatureProduct) => f.toAvro

  @transient val uTag: TypeTag[FeatureProduct] = typeTag[FeatureProduct]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Feature)])(
    implicit tTag: ClassTag[Feature]): IntervalArray[ReferenceRegion, Feature] = {
    IntervalArray(rdd, FeatureArray.apply(_, _))
  }

  /**
   * Saves metadata for a FeatureDataset, including partition map, sequences, and samples.
   *
   * @param pathName The path name to save meta data for this FeatureDataset.
   */
  override protected def saveMetadata(pathName: String): Unit = {
    savePartitionMap(pathName)
    saveReferences(pathName)
    saveSamples(pathName)
  }

  def union(datasets: FeatureDataset*): FeatureDataset = {
    val iterableDatasets = datasets.toSeq
    FeatureDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.references).fold(references)(_ ++ _),
      iterableDatasets.map(_.samples).fold(samples)(_ ++ _))
  }

  override def transformDataset(
    tFn: Dataset[FeatureProduct] => Dataset[FeatureProduct]): FeatureDataset = {
    DatasetBoundFeatureDataset(tFn(dataset), references, samples)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[FeatureProduct], Dataset[FeatureProduct]]): FeatureDataset = {
    DatasetBoundFeatureDataset(tFn.call(dataset), references, samples)
  }

  /**
   * Java friendly save function. Automatically detects the output format.
   *
   * Writes files ending in .bed as BED6/12, .gff3 as GFF3, .gtf/.gff as
   * GTF/GFF2, .narrow[pP]eak as NarrowPeak, and .interval_list as
   * IntervalList. If none of these match, we fall back to Parquet.
   * These files are written as sharded text files.
   *
   * @param filePath The location to write the output.
   * @param asSingleFile If false, writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   fast file concatenation engine.
   */
  def save(filePath: java.lang.String,
           asSingleFile: java.lang.Boolean,
           disableFastConcat: java.lang.Boolean) {
    if (filePath.endsWith(".bed")) {
      saveAsBed(filePath,
        asSingleFile = asSingleFile,
        disableFastConcat = disableFastConcat)
    } else if (filePath.endsWith(".gtf") ||
      filePath.endsWith(".gff")) {
      saveAsGtf(filePath,
        asSingleFile = asSingleFile,
        disableFastConcat = disableFastConcat)
    } else if (filePath.endsWith(".gff3")) {
      saveAsGff3(filePath,
        asSingleFile = asSingleFile,
        disableFastConcat = disableFastConcat)
    } else if (filePath.endsWith(".narrowPeak") ||
      filePath.endsWith(".narrowpeak")) {
      saveAsNarrowPeak(filePath,
        asSingleFile = asSingleFile,
        disableFastConcat = disableFastConcat)
    } else if (filePath.endsWith(".interval_list")) {
      saveAsIntervalList(filePath,
        asSingleFile = asSingleFile,
        disableFastConcat = disableFastConcat)
    } else {
      if (asSingleFile) {
        warn("asSingleFile = true ignored when saving as Parquet.")
      }
      saveAsParquet(new JavaSaveArgs(filePath))
    }
  }

  /**
   * Converts the FeatureDataset to a CoverageDataset.
   *
   * @return Genomic dataset containing Coverage records.
   */
  def toCoverage(): CoverageDataset

  /**
   * Filter this FeatureDataset by feature type to those that match the specified feature type.
   *
   * @param featureType Feature type to filter by.
   * @return FeatureDataset filtered by the specified feature type.
   */
  def filterToFeatureType(featureType: String): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getFeatureType).exists(_.equals(featureType))))
  }

  /**
   * (Java-specific) Filter this FeatureDataset by feature type to those that match the specified feature types.
   *
   * @param featureType List of feature types to filter by.
   * @return FeatureDataset filtered by the specified feature types.
   */
  def filterToFeatureTypes(featureTypes: java.util.List[String]): FeatureDataset = {
    filterToFeatureTypes(asScalaBuffer(featureTypes))
  }

  /**
   * (Scala-specific) Filter this FeatureDataset by feature type to those that match the specified feature types.
   *
   * @param featureType Sequence of feature types to filter by.
   * @return FeatureDataset filtered by the specified feature types.
   */
  def filterToFeatureTypes(featureTypes: Seq[String]): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getFeatureType).exists(featureTypes.contains(_))))
  }

  /**
   * Filter this FeatureDataset by gene to those that match the specified gene.
   *
   * @param geneId Gene to filter by.
   * @return FeatureDataset filtered by the specified gene.
   */
  def filterToGene(geneId: String): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getGeneId).exists(_.equals(geneId))))
  }

  /**
   * (Java-specific) Filter this FeatureDataset by gene to those that match the specified genes.
   *
   * @param geneIds List of genes to filter by.
   * @return FeatureDataset filtered by the specified genes.
   */
  def filterToGenes(geneIds: java.util.List[String]): FeatureDataset = {
    filterToGenes(asScalaBuffer(geneIds))
  }

  /**
   * (Scala-specific) Filter this FeatureDataset by gene to those that match the specified genes.
   *
   * @param geneIds Sequence of genes to filter by.
   * @return FeatureDataset filtered by the specified genes.
   */
  def filterToGenes(geneIds: Seq[String]): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getGeneId).exists(geneIds.contains(_))))
  }

  /**
   * Filter this FeatureDataset by transcript to those that match the specified transcript.
   *
   * @param transcriptId Transcript to filter by.
   * @return FeatureDataset filtered by the specified transcript.
   */
  def filterToTranscript(transcriptId: String): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getTranscriptId).exists(_.equals(transcriptId))))
  }

  /**
   * (Java-specific) Filter this FeatureDataset by transcript to those that match the specified transcripts.
   *
   * @param transcriptIds List of transcripts to filter by.
   * @return FeatureDataset filtered by the specified transcripts.
   */
  def filterToTranscripts(transcriptIds: java.util.List[String]): FeatureDataset = {
    filterToTranscripts(asScalaBuffer(transcriptIds))
  }

  /**
   * (Scala-specific) Filter this FeatureDataset by transcript to those that match the specified transcripts.
   *
   * @param transcriptIds Sequence of transcripts to filter by.
   * @return FeatureDataset filtered by the specified transcripts.
   */
  def filterToTranscripts(transcriptIds: Seq[String]): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getTranscriptId).exists(transcriptIds.contains(_))))
  }

  /**
   * Filter this FeatureDataset by exon to those that match the specified exon.
   *
   * @param exonId Exon to filter by.
   * @return FeatureDataset filtered by the specified exon.
   */
  def filterToExon(exonId: String): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getExonId).exists(_.equals(exonId))))
  }

  /**
   * (Java-specific) Filter this FeatureDataset by exon to those that match the specified exons.
   *
   * @param exonIds List of exons to filter by.
   * @return FeatureDataset filtered by the specified exons.
   */
  def filterToExons(exonIds: java.util.List[String]): FeatureDataset = {
    filterToExons(asScalaBuffer(exonIds))
  }

  /**
   * (Scala-specific) Filter this FeatureDataset by exon to those that match the specified exons.
   *
   * @param exonIds Sequence of exons to filter by.
   * @return FeatureDataset filtered by the specified exons.
   */
  def filterToExons(exonIds: Seq[String]): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getExonId).exists(exonIds.contains(_))))
  }

  /**
   * Filter this FeatureDataset by score.
   *
   * @param minimumScore Minimum score to filter by, inclusive.
   * @return FeatureDataset filtered by the specified minimum score.
   */
  def filterByScore(minimumScore: Double): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getScore).exists(_ >= minimumScore)))
  }

  /**
   * Filter this FeatureDataset by parent to those that match the specified parent.
   *
   * @param parentId Parent to filter by.
   * @return FeatureDataset filtered by the specified parent.
   */
  def filterToParent(parentId: String): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getParentIds).exists(_.contains(parentId))))
  }

  /**
   * (Java-specific) Filter this FeatureDataset by parent to those that match the specified parents.
   *
   * @param parentIds List of parents to filter by.
   * @return FeatureDataset filtered by the specified parents.
   */
  def filterToParents(parentIds: java.util.List[String]): FeatureDataset = {
    filterToParents(asScalaBuffer(parentIds))
  }

  /**
   * (Scala-specific) Filter this FeatureDataset by parent to those that match the specified parents.
   *
   * @param parentIds Sequence of parents to filter by.
   * @return FeatureDataset filtered by the specified parents.
   */
  def filterToParents(parentIds: Seq[String]): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getParentIds).exists(!Collections.disjoint(_, parentIds))))
  }

  /**
   * Filter this FeatureDataset by reference name to those that match the specified reference name.
   *
   * @param referenceName Reference name to filter by.
   * @return FeatureDataset filtered by the specified reference name.
   */
  def filterToReferenceName(referenceName: String): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getReferenceName).exists(_.equals(referenceName))))
  }

  /**
   * Filter this FeatureDataset by attribute to those that match the specified attribute key and value.
   *
   * @param key Attribute key to filter by.
   * @param value Attribute value to filter by.
   * @return FeatureDataset filtered by the specified attribute.
   */
  def filterByAttribute(key: String, value: String): FeatureDataset = {
    transform((rdd: RDD[Feature]) => rdd.filter(f => Option(f.getAttributes.get(key)).exists(_.equals(value))))
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new FeatureDataset with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Feature],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): FeatureDataset = {
    new RDDBoundFeatureDataset(newRdd, references, samples, newPartitionMap)
  }

  /**
   * @param elem The Feature to get an underlying region for.
   * @return Since a feature maps directly to a single genomic region, this
   *   method will always return a Seq of exactly one ReferenceRegion.
   */
  protected def getReferenceRegions(elem: Feature): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion.unstranded(elem))
  }

  /**
   * Save this FeatureDataset in GTF format.
   *
   * @param fileName The path to save GTF formatted text file(s) to.
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   */
  def saveAsGtf(fileName: String,
                asSingleFile: Boolean = false,
                disableFastConcat: Boolean = false) = {
    writeTextRdd(rdd.map(FeatureDataset.toGtf),
      fileName,
      asSingleFile,
      disableFastConcat)
  }

  /**
   * Save this FeatureDataset in GFF3 format.
   *
   * @param fileName The path to save GFF3 formatted text file(s) to.
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   */
  def saveAsGff3(fileName: String,
                 asSingleFile: Boolean = false,
                 disableFastConcat: Boolean = false) = {
    val optHeaderPath = if (asSingleFile) {
      val headerPath = "%s_head".format(fileName)
      GFF3HeaderWriter(headerPath, rdd.context)
      Some(headerPath)
    } else {
      None
    }
    writeTextRdd(rdd.map(FeatureDataset.toGff3),
      fileName,
      asSingleFile,
      disableFastConcat,
      optHeaderPath = optHeaderPath)
  }

  /**
   * Save this FeatureDataset in UCSC BED format, where score is formatted as
   * integer values between 0 and 1000, with missing value as specified.
   *
   * @param fileName The path to save BED formatted text file(s) to.
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param minimumScore Minimum score, interpolated to 0.
   * @param maximumScore Maximum score, interpolated to 1000.
   * @param missingValue Value to use if score is not specified. Defaults to 0.
   */
  def saveAsUcscBed(fileName: String,
                    asSingleFile: Boolean = false,
                    disableFastConcat: Boolean = false,
                    minimumScore: Double,
                    maximumScore: Double,
                    missingValue: Int = 0) = {

    writeTextRdd(rdd.map(FeatureDataset.toBed(_, Some(minimumScore), Some(maximumScore), Some(missingValue))),
      fileName,
      asSingleFile,
      disableFastConcat)
  }

  /**
   * Save this FeatureDataset in bedtools2 BED format, where score is formatted
   * as double floating point values with missing values.
   *
   * @param fileName The path to save BED formatted text file(s) to.
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   */
  def saveAsBed(fileName: String,
                asSingleFile: Boolean = false,
                disableFastConcat: Boolean = false) = {
    writeTextRdd(rdd.map(FeatureDataset.toBed),
      fileName,
      asSingleFile,
      disableFastConcat)
  }

  /**
   * Save this FeatureDataset in interval list format.
   *
   * @param fileName The path to save interval list formatted text file(s) to.
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   */
  def saveAsIntervalList(fileName: String,
                         asSingleFile: Boolean = false,
                         disableFastConcat: Boolean = false) = {
    val intervalEntities = rdd.map(FeatureDataset.toInterval)

    if (asSingleFile) {

      // get fs
      val fs = FileSystem.get(rdd.context.hadoopConfiguration)

      // write sam file header
      val headPath = new Path("%s_head".format(fileName))
      SAMHeaderWriter.writeHeader(fs,
        headPath,
        references)

      // write tail entries
      val tailPath = new Path("%s_tail".format(fileName))
      intervalEntities.saveAsTextFile(tailPath.toString)

      // merge
      FileMerger.mergeFiles(rdd.context,
        fs,
        new Path(fileName),
        tailPath,
        optHeaderPath = Some(headPath),
        disableFastConcat = disableFastConcat)
    } else {
      intervalEntities.saveAsTextFile(fileName)
    }
  }

  /**
   * Save this FeatureDataset in NarrowPeak format.
   *
   * @param fileName The path to save NarrowPeak formatted text file(s) to.
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   */
  def saveAsNarrowPeak(fileName: String,
                       asSingleFile: Boolean = false,
                       disableFastConcat: Boolean = false) {
    writeTextRdd(rdd.map(FeatureDataset.toNarrowPeak),
      fileName,
      asSingleFile,
      disableFastConcat)
  }

  /**
   * Sorts the RDD by the reference ordering.
   *
   * @param ascending Whether to sort in ascending order or not.
   * @param numPartitions The number of partitions to have after sorting.
   *   Defaults to the partition count of the underlying RDD.
   */
  def sortByReference(ascending: Boolean = true, numPartitions: Int = rdd.partitions.length): FeatureDataset = {
    implicit def ord = FeatureOrdering

    replaceRdd(rdd.sortBy(f => f, ascending, numPartitions))
  }
}
