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
package org.bdgenomics.adam.rdd.feature

import com.google.common.collect.ComparisonChain
import java.util.Comparator
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.{
  AvroGenomicRDD,
  FileMerger,
  JavaSaveArgs,
  SAMHeaderWriter
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro.{ Feature, Strand }
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._
import scala.math.max
import scala.reflect.ClassTag

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
      .compare(x.getContigName, y.getContigName)
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

object FeatureRDD {

  /**
   * Builds a FeatureRDD without SequenceDictionary information by running an
   * aggregate to rebuild the SequenceDictionary.
   *
   * @param rdd The underlying Feature RDD to build from.
   * @param optStorageLevel Optional storage level to use for cache before
   *                        building the SequenceDictionary.
   * @return Returns a new FeatureRDD.
   */
  def apply(
    rdd: RDD[Feature],
    optStorageLevel: Option[StorageLevel]): FeatureRDD = BuildSequenceDictionary.time {

    // optionally cache the rdd, since we're making multiple passes
    optStorageLevel.foreach(rdd.persist(_))

    // create sequence records with length max(start, end) + 1L
    val sequenceRecords = rdd
      .keyBy(_.getContigName)
      .map(kv => (kv._1, max(kv._2.getStart, kv._2.getEnd) + 1L))
      .reduceByKey(max(_, _))
      .map(kv => SequenceRecord(kv._1, kv._2))

    val sd = new SequenceDictionary(sequenceRecords.collect.toVector)

    FeatureRDD(rdd, sd)
  }

  /**
   * Builds a FeatureRDD without a partitionMap.
   *
   * @param rdd The underlying Feature RDD.
   * @param sd The Sequence Dictionary for the Feature RDD.
   * @return Returns a new FeatureRDD.
   */
  def apply(rdd: RDD[Feature], sd: SequenceDictionary): FeatureRDD = {
    FeatureRDD(rdd, sd, None)
  }
  /**
   * @param feature Feature to convert to GTF format.
   * @return Returns this feature as a GTF line.
   */
  private[feature] def toGtf(feature: Feature): String = {
    def escape(entry: (Any, Any)): String = {
      entry._1 + " \"" + entry._2 + "\""
    }

    val seqname = feature.getContigName
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
  private[rdd] def toInterval(feature: Feature): String = {
    val sequenceName = feature.getContigName
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
  private[rdd] def toNarrowPeak(feature: Feature): String = {
    val chrom = feature.getContigName
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
  private[rdd] def toBed(feature: Feature): String = {
    val chrom = feature.getContigName
    val start = feature.getStart
    val end = feature.getEnd
    val name = Features.nameOf(feature)
    val score = Option(feature.getScore).getOrElse(".")
    val strand = Features.asString(feature.getStrand)

    if (!feature.getAttributes.containsKey("thickStart")) {
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
  private[rdd] def toGff3(feature: Feature): String = {
    def escape(entry: (Any, Any)): String = {
      entry._1 + "=" + entry._2
    }

    val seqid = feature.getContigName
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

/**
 * A GenomicRDD that wraps Feature data.
 *
 * @param rdd An RDD of genomic Features.
 * @param sequences The reference genome this data is aligned to.
 */
case class FeatureRDD(rdd: RDD[Feature],
                      sequences: SequenceDictionary,
                      optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends AvroGenomicRDD[Feature, FeatureRDD] with Logging {

  protected def buildTree(rdd: RDD[(ReferenceRegion, Feature)])(
    implicit tTag: ClassTag[Feature]): IntervalArray[ReferenceRegion, Feature] = {
    IntervalArray(rdd, FeatureArray.apply(_, _))
  }

  def union(rdds: FeatureRDD*): FeatureRDD = {
    val iterableRdds = rdds.toSeq
    FeatureRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _))
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
        log.warn("asSingleFile = true ignored when saving as Parquet.")
      }
      saveAsParquet(new JavaSaveArgs(filePath))
    }
  }

  /**
   * Converts the FeatureRDD to a CoverageRDD.
   *
   * @return CoverageRDD containing RDD of Coverage.
   */
  def toCoverage: CoverageRDD = {
    val coverageRdd = rdd.map(f => Coverage(f))
    CoverageRDD(coverageRdd, sequences)
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new FeatureRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Feature],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): FeatureRDD = {
    copy(rdd = newRdd, optPartitionMap = newPartitionMap)
  }

  /**
   * @param elem The Feature to get an underlying region for.
   * @return Since a feature maps directly to a single genomic region, this
   *   method will always return a Seq of exactly one ReferenceRegion.
   */
  protected def getReferenceRegions(elem: Feature): Seq[ReferenceRegion] = {
    Seq({
      try {
        ReferenceRegion.stranded(elem)
      } catch {
        case e: IllegalArgumentException => ReferenceRegion.unstranded(elem)
      }
    })
  }

  /**
   * Writes an RDD to disk as text and optionally merges.
   *
   * @param rdd RDD to save.
   * @param outputPath Output path to save text files to.
   * @param asSingleFile If true, combines all partition shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param optHeaderPath If provided, the header file to include.
   */
  private def writeTextRdd[T](rdd: RDD[T],
                              outputPath: String,
                              asSingleFile: Boolean,
                              disableFastConcat: Boolean,
                              optHeaderPath: Option[String] = None) {
    if (asSingleFile) {

      // write rdd to disk
      val tailPath = "%s_tail".format(outputPath)
      rdd.saveAsTextFile(tailPath)

      // get the filesystem impl
      val fs = FileSystem.get(rdd.context.hadoopConfiguration)

      // and then merge
      FileMerger.mergeFiles(rdd.context,
        fs,
        new Path(outputPath),
        new Path(tailPath),
        disableFastConcat = disableFastConcat,
        optHeaderPath = optHeaderPath.map(p => new Path(p)))
    } else {
      assert(optHeaderPath.isEmpty)
      rdd.saveAsTextFile(outputPath)
    }
  }

  /**
   * Save this FeatureRDD in GTF format.
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
    writeTextRdd(rdd.map(FeatureRDD.toGtf),
      fileName,
      asSingleFile,
      disableFastConcat)
  }

  /**
   * Save this FeatureRDD in GFF3 format.
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
    writeTextRdd(rdd.map(FeatureRDD.toGff3),
      fileName,
      asSingleFile,
      disableFastConcat,
      optHeaderPath = optHeaderPath)
  }

  /**
   * Save this FeatureRDD in BED format.
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
    writeTextRdd(rdd.map(FeatureRDD.toBed),
      fileName,
      asSingleFile,
      disableFastConcat)
  }

  /**
   * Save this FeatureRDD in interval list format.
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
    val intervalEntities = rdd.map(FeatureRDD.toInterval)

    if (asSingleFile) {

      // get fs
      val fs = FileSystem.get(rdd.context.hadoopConfiguration)

      // write sam file header
      val headPath = new Path("%s_head".format(fileName))
      SAMHeaderWriter.writeHeader(fs,
        headPath,
        sequences)

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
   * Save this FeatureRDD in NarrowPeak format.
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
    writeTextRdd(rdd.map(FeatureRDD.toNarrowPeak),
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
  def sortByReference(ascending: Boolean = true, numPartitions: Int = rdd.partitions.length): FeatureRDD = {
    implicit def ord = FeatureOrdering

    replaceRdd(rdd.sortBy(f => f, ascending, numPartitions))
  }
}
