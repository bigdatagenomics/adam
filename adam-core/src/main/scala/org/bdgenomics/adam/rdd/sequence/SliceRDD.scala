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
package org.bdgenomics.adam.rdd.sequence

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.read.{
AlignmentRecordRDD,
ReadRDD
}
import org.bdgenomics.adam.rdd.{
  AvroGenomicRDD,
  JavaSaveArgs
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Slice => SliceProduct }
import org.bdgenomics.formats.avro.{
AlignmentRecord,
  QualityScoreVariant,
  Read,
  Sequence,
  Slice
}
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import scala.collection.JavaConversions._
import scala.math._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class SliceArray(
    array: Array[(ReferenceRegion, Slice)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Slice] {

  def duplicate(): IntervalArray[ReferenceRegion, Slice] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Slice)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Slice] = {
    SliceArray(arr, maxWidth)
  }
}

private[adam] class SliceArraySerializer extends IntervalArraySerializer[ReferenceRegion, Slice, SliceArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Slice]

  protected def builder(arr: Array[(ReferenceRegion, Slice)],
                        maxIntervalWidth: Long): SliceArray = {
    SliceArray(arr, maxIntervalWidth)
  }
}

object SliceRDD {

  /**
   * A GenomicRDD that wraps a dataset of Slice data.
   *
   * @param ds A Dataset of slices.
   * @param sequences The reference genome these data are aligned to.
   */
  def apply(ds: Dataset[SliceProduct],
            sequences: SequenceDictionary): SliceRDD = {
    new DatasetBoundSliceRDD(ds, sequences)
  }

  /**
   * Builds a SliceRDD with an empty sequence dictionary.
   *
   * @param rdd The underlying Slice RDD to build from.
   * @return Returns a new SliceRDD.
   */
  def apply(rdd: RDD[Slice]): SliceRDD = {
    SliceRDD(rdd, SequenceDictionary.empty)
  }

  /**
   * Builds a SliceRDD given a sequence dictionary.
   *
   * @param rdd The underlying Slice RDD to build from.
   * @param sd The sequence dictionary for this SliceRDD.
   * @return Returns a new SliceRDD.
   */
  def apply(rdd: RDD[Slice], sd: SequenceDictionary): SliceRDD = {
    new RDDBoundSliceRDD(rdd, sd, None)
  }
}

case class ParquetUnboundSliceRDD private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary) extends SliceRDD {

  lazy val rdd: RDD[Slice] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).as[SliceProduct]
  }

  def replaceSequences(newSequences: SequenceDictionary): SliceRDD = {
    copy(sequences = newSequences)
  }
}

case class DatasetBoundSliceRDD private[rdd] (
    dataset: Dataset[SliceProduct],
    sequences: SequenceDictionary) extends SliceRDD {

  lazy val rdd = dataset.rdd.map(_.toAvro)
  protected lazy val optPartitionMap = None

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
    tFn: Dataset[SliceProduct] => Dataset[SliceProduct]): SliceRDD = {
    copy(dataset = tFn(dataset))
  }

  def replaceSequences(newSequences: SequenceDictionary): SliceRDD = {
    copy(sequences = newSequences)
  }
}

case class RDDBoundSliceRDD private[rdd] (
    rdd: RDD[Slice],
    sequences: SequenceDictionary,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends SliceRDD {

  /**
   * A SQL Dataset of slices.
   */
  lazy val dataset: Dataset[SliceProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(SliceProduct.fromAvro))
  }

  def replaceSequences(newSequences: SequenceDictionary): SliceRDD = {
    copy(sequences = newSequences)
  }
}

sealed abstract class SliceRDD extends AvroGenomicRDD[Slice, SliceProduct, SliceRDD] {

  @transient val uTag: TypeTag[SliceProduct] = typeTag[SliceProduct]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Slice)])(
    implicit tTag: ClassTag[Slice]): IntervalArray[ReferenceRegion, Slice] = {
    IntervalArray(rdd, SliceArray.apply(_, _))
  }

  def union(rdds: SliceRDD*): SliceRDD = {
    val iterableRdds = rdds.toSeq
    SliceRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _))
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
    tFn: Dataset[SliceProduct] => Dataset[SliceProduct]): SliceRDD = {
    DatasetBoundSliceRDD(tFn(dataset), sequences)
  }

  /**
   * Merge slices into sequences.
   *
   * @return Returns a SequenceRDD containing merged slices.
   */
  def merge(): SequenceRDD = {
    def toSequence(slice: Slice): Sequence = {
      Sequence.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setAttributes(slice.getAttributes)
        .build
    }

    def mergeSequences(first: Sequence, second: Sequence): Sequence = {
      Sequence.newBuilder(first)
        .setLength(first.getLength + second.getLength)
        .setSequence(first.getSequence + second.getSequence)
        .setAttributes(first.getAttributes ++ second.getAttributes)
        .build
    }

    val merged: RDD[Sequence] = rdd
      .sortBy(slice => (slice.getName, slice.getStart))
      .map(slice => (slice.getName, toSequence(slice)))
      .reduceByKey(mergeSequences)
      .values

    SequenceRDD(merged)
  }

  /**
   * Convert this RDD of slices into reads.
   *
   * @return Returns a new ReadRDD converted from this RDD of slices.
   */
  def toReads: ReadRDD = {
    def toRead(slice: Slice): Read = {
      Read.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setQualityScoreVariant(QualityScoreVariant.FASTQ_SANGER)
        .setQualityScores("B" * (if (slice.getLength == null) 0 else slice.getLength.toInt))
        .setAttributes(slice.getAttributes)
        .build()
    }
    ReadRDD(rdd.map(toRead), sequences)
  }

  /**
   * Convert this RDD of slices into sequences.
   *
   * @return Returns a new SequenceRDD converted from this RDD of slices.
   */
  def toSequences: SequenceRDD = {
    def toSequence(slice: Slice): Sequence = {
      Sequence.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setAttributes(slice.getAttributes)
        .build()
    }
    SequenceRDD(rdd.map(toSequence), sequences)
  }

  /**
    * Convert this RDD of slices into alignments.
    *
    * @return Returns a new AlignmentRecordRDD converted from this RDD of slices.
    */
  def toAlignments: AlignmentRecordRDD = {
    def toAlignments(slice: Slice): AlignmentRecord = {
      AlignmentRecord.newBuilder()
        .setContigName(slice.getName)
        .setStart(slice.getStart)
        .setEnd(slice.getEnd)
        .build()
    }
    AlignmentRecordRDD(rdd.map(toAlignments), sequences, RecordGroupDictionary.empty, Seq.empty)
  }

  /**
   * Save slices as Parquet or FASTA.
   *
   * If filename ends in .fa or .fasta, saves as FASTA. If not, saves slices
   * to Parquet. Defaults to 60 character line length, if saving to FASTA.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   */
  def save(filePath: java.lang.String, asSingleFile: java.lang.Boolean) {
    if (filePath.endsWith(".fa") || filePath.endsWith(".fasta")) {
      saveAsFasta(filePath, asSingleFile = asSingleFile)
    } else {
      if (asSingleFile) {
        log.warn("asSingleFile = true ignored when saving as Parquet.")
      }
      saveAsParquet(new JavaSaveArgs(filePath))
    }
  }

  /**
   * Save slices in FASTA format.
   *
   * The coordinate fields for this slice are appended to the description field
   * for the FASTA description line:
   * <pre>
   * &gt;description start-slice:strand
   * </pre>
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   * @param lineWidth Hard wrap FASTA formatted slice at line width, default 60.
   */
  def saveAsFasta(filePath: String,
                  asSingleFile: Boolean = false,
                  disableFastConcat: Boolean = false,
                  lineWidth: Int = 60) {

    def toFasta(slice: Slice): String = {
      val sb = new StringBuilder()
      sb.append(">")
      sb.append(slice.getName)
      Option(slice.getDescription).foreach(n => sb.append(" ").append(n))
      sb.append(s" slice.getStart-slice.getEnd:slice.getStrand")
      slice.getSequence.grouped(lineWidth).foreach(line => {
        sb.append("\n")
        sb.append(line)
      })
      sb.toString
    }

    writeTextRdd(rdd.map(toFasta),
      filePath,
      asSingleFile = asSingleFile,
      disableFastConcat = disableFastConcat)
  }

  /**
    * Extract the specified region from this RDD of slices as a string, merging
    * slices if necessary.
    *
    * @param region Region to extract.
    * @return Return the specified region from this RDD of slices as a string, merging
    *         slices if necessary.
    */
  def extract(region: ReferenceRegion): String = {
    def getString(slice: (ReferenceRegion, Slice)): (ReferenceRegion, String) = {
      val trimStart = max(0, region.start - slice._1.start).toInt
      val trimEnd = max(0, slice._1.end - region.end).toInt

      val fragmentSequence: String = slice._2.getSequence

      val str = fragmentSequence.drop(trimStart)
        .dropRight(trimEnd)
      val reg = new ReferenceRegion(
        slice._1.referenceName,
        slice._1.start + trimStart,
        slice._1.end - trimEnd
      )
      (reg, str)
    }

    def reducePairs(
                     kv1: (ReferenceRegion, String),
                     kv2: (ReferenceRegion, String)): (ReferenceRegion, String) = {
      assert(kv1._1.isAdjacent(kv2._1), "Regions being joined must be adjacent. For: " +
        kv1 + ", " + kv2)

      (kv1._1.merge(kv2._1), if (kv1._1.compareTo(kv2._1) <= 0) {
        kv1._2 + kv2._2
      } else {
        kv2._2 + kv1._2
      })
    }

    try {
      val refPairRDD: RDD[(ReferenceRegion, String)] = rdd.keyBy(ReferenceRegion(_))
        .filter(kv => kv._1.isDefined)
        .map(kv => (kv._1.get, kv._2))
        .filter(kv => kv._1.overlaps(region))
        .sortByKey()
        .map(kv => getString(kv))

      val pair: (ReferenceRegion, String) = refPairRDD.collect.reduceLeft(reducePairs)
      assert(
        pair._1.compareTo(region) == 0,
        "Merging slices returned a different region than requested."
      )

      pair._2
    } catch {
      case (uoe: UnsupportedOperationException) =>
        throw new UnsupportedOperationException("Could not find " + region + "in reference RDD.")
    }
  }

  /**
    * Extract the specified regions from this RDD of slices as an RDD of (ReferenceRegion,
    * String) tuples, merging slices if necessary.
    *
    * @param regions Zero or more regions to extract.
    * @return Return the specified regions from this RDD of slices as an RDD of (ReferenceRegion,
    *         String) tuples, merging slices if necessary.
    */
  def extractRegions(regions: Iterable[ReferenceRegion]): RDD[(ReferenceRegion, String)] = {
    def extractSequence(sliceRegion: ReferenceRegion, slice: Slice, region: ReferenceRegion): (ReferenceRegion, String) = {
      val merged = sliceRegion.intersection(region)
      val start = (merged.start - sliceRegion.start).toInt
      val end = (merged.end - sliceRegion.start).toInt
      val fragmentSequence: String = slice.getSequence
      (merged, fragmentSequence.substring(start, end))
    }

    def reduceRegionSequences(
                               kv1: (ReferenceRegion, String),
                               kv2: (ReferenceRegion, String)): (ReferenceRegion, String) = {
      (kv1._1.merge(kv2._1), if (kv1._1.compareTo(kv2._1) <= 0) {
        kv1._2 + kv2._2
      } else {
        kv2._2 + kv1._2
      })
    }

    val places = flattenRddByRegions()
      .flatMap {
        case (sliceRegion, slice) =>
          regions.collect {
            case region if sliceRegion.overlaps(region) =>
              (region, extractSequence(sliceRegion, slice, region))
          }
      }.sortByKey()

    places.reduceByKey(reduceRegionSequences).values
  }

  /**
    * For all adjacent slices in this RDD, we extend the slices so that the adjacent
    * slices now overlap by _n_ bases, where _n_ is the flank length.
    *
    * Java friendly variant.
    *
    * @param flankLength The length to extend adjacent slices by.
    * @return Returns this RDD, with all adjacent slices extended with flanking sequence.
    */
  def flankAdjacent(flankLength: java.lang.Integer): SliceRDD = {
    val flank: Int = flankLength
    flankAdjacent(flank)
  }

  /**
    * For all adjacent slices in this RDD, we extend the slices so that the adjacent
    * slices now overlap by _n_ bases, where _n_ is the flank length.
    *
    * @param flankLength The length to extend adjacent slices by.
    * @return Returns this RDD, with all adjacent slices extended with flanking sequence.
    */
  def flankAdjacent(flankLength: Int): SliceRDD = {
    replaceRdd(FlankSlices(rdd,
      sequences,
      flankLength))
  }

  /**
    * Counts the k-mers contained in this RDD of slices.
    *
    * @param kmerLength The length of k-mers to count.
    * @return Returns an RDD containing k-mer/count pairs.
    */
  def countKmers(kmerLength: Int): RDD[(String, Long)] = {
    flankAdjacent(kmerLength).rdd.flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getSequence
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }

  /**
    * Counts the k-mers contained in this RDD of slices.
    *
    * Java friendly variant.
    *
    * @param kmerLength The length of k-mers to count.
    * @return Returns an RDD containing k-mer/count pairs.
    */
  def countKmers(
                  kmerLength: java.lang.Integer): JavaRDD[(String, java.lang.Long)] = {
    val k: Int = kmerLength
    countKmers(k).map(p => {
      (p._1, p._2: java.lang.Long)
    }).toJavaRDD()
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new ReadRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Slice],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): SliceRDD = {
    new RDDBoundSliceRDD(newRdd, sequences, newPartitionMap)
  }

  /**
   * @param slice Slice to extract a region from.
   * @return Returns a reference region that covers the entirety of the slice.
   */
  protected def getReferenceRegions(slice: Slice): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(slice.getName, slice.getStart, slice.getEnd, slice.getStrand))
  }
}
