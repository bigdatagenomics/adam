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
package org.bdgenomics.adam.rdd.contig

import com.google.common.base.Splitter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.converters.FragmentConverter
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceRecord,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{
  DatasetBoundGenomicDataset,
  AvroGenomicDataset,
  JavaSaveArgs
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ NucleotideContigFragment => NucleotideContigFragmentProduct }
import org.bdgenomics.formats.avro.{ AlignmentRecord, NucleotideContigFragment }
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.math.max
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class NucleotideContigFragmentArray(
    array: Array[(ReferenceRegion, NucleotideContigFragment)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, NucleotideContigFragment] {

  def duplicate(): IntervalArray[ReferenceRegion, NucleotideContigFragment] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, NucleotideContigFragment)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, NucleotideContigFragment] = {
    NucleotideContigFragmentArray(arr, maxWidth)
  }
}

private[adam] class NucleotideContigFragmentArraySerializer extends IntervalArraySerializer[ReferenceRegion, NucleotideContigFragment, NucleotideContigFragmentArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[NucleotideContigFragment]

  protected def builder(arr: Array[(ReferenceRegion, NucleotideContigFragment)],
                        maxIntervalWidth: Long): NucleotideContigFragmentArray = {
    NucleotideContigFragmentArray(arr, maxIntervalWidth)
  }
}

object NucleotideContigFragmentDataset extends Serializable {

  /**
   * Builds a NucleotideContigFragmentDataset when no sequence dictionary is given.
   *
   * @param rdd Underlying RDD. We recompute the sequence dictionary from
   *   this RDD.
   * @return Returns a new NucleotideContigFragmentDataset.
   */
  private[rdd] def apply(rdd: RDD[NucleotideContigFragment]): NucleotideContigFragmentDataset = {

    // get sequence dictionary
    val sd = new SequenceDictionary(rdd.flatMap(ncf => {
      if (ncf.getContigName != null) {
        Some(SequenceRecord.fromADAMContigFragment(ncf))
      } else {
        None
      }
    }).distinct
      .collect
      .toVector)

    NucleotideContigFragmentDataset(rdd, sd)
  }

  /**
   * Builds a NucleotideContigFragmentDataset without a partition map.
   *
   * @param rdd The underlying NucleotideContigFragment RDD.
   * @param sequences The sequence dictionary for the RDD.
   * @return A new NucleotideContigFragmentDataset.
   */
  def apply(rdd: RDD[NucleotideContigFragment],
            sequences: SequenceDictionary): NucleotideContigFragmentDataset = {

    RDDBoundNucleotideContigFragmentDataset(rdd, sequences, None)
  }
}

case class ParquetUnboundNucleotideContigFragmentDataset private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary) extends NucleotideContigFragmentDataset {

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val rdd: RDD[NucleotideContigFragment] = {
    sc.loadParquet(parquetFilename)
  }

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename).withColumnRenamed("referenceName", "contigName").as[NucleotideContigFragmentProduct]
  }

  def replaceSequences(
    newSequences: SequenceDictionary): NucleotideContigFragmentDataset = {
    copy(sequences = newSequences)
  }
}

case class DatasetBoundNucleotideContigFragmentDataset private[rdd] (
  dataset: Dataset[NucleotideContigFragmentProduct],
  sequences: SequenceDictionary,
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends NucleotideContigFragmentDataset
    with DatasetBoundGenomicDataset[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  lazy val rdd: RDD[NucleotideContigFragment] = dataset.rdd.map(_.toAvro)

  protected lazy val optPartitionMap = None

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

  def replaceSequences(
    newSequences: SequenceDictionary): NucleotideContigFragmentDataset = {
    copy(sequences = newSequences)
  }
}

/**
 * A wrapper class for RDD[NucleotideContigFragment].
 *
 * @param rdd Underlying RDD
 * @param sequences Sequence dictionary computed from rdd
 */
case class RDDBoundNucleotideContigFragmentDataset private[rdd] (
    rdd: RDD[NucleotideContigFragment],
    sequences: SequenceDictionary,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends NucleotideContigFragmentDataset {

  /**
   * A SQL Dataset of contig fragments.
   */
  lazy val dataset: Dataset[NucleotideContigFragmentProduct] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd.map(NucleotideContigFragmentProduct.fromAvro))
  }

  def replaceSequences(
    newSequences: SequenceDictionary): NucleotideContigFragmentDataset = {
    copy(sequences = newSequences)
  }
}

sealed abstract class NucleotideContigFragmentDataset extends AvroGenomicDataset[NucleotideContigFragment, NucleotideContigFragmentProduct, NucleotideContigFragmentDataset] {

  protected val productFn = NucleotideContigFragmentProduct.fromAvro(_)
  protected val unproductFn = (c: NucleotideContigFragmentProduct) => c.toAvro

  @transient val uTag: TypeTag[NucleotideContigFragmentProduct] = typeTag[NucleotideContigFragmentProduct]

  protected def buildTree(rdd: RDD[(ReferenceRegion, NucleotideContigFragment)])(
    implicit tTag: ClassTag[NucleotideContigFragment]): IntervalArray[ReferenceRegion, NucleotideContigFragment] = {
    IntervalArray(rdd, NucleotideContigFragmentArray.apply(_, _))
  }

  /**
   * Converts an RDD of nucleotide contig fragments into reads. Adjacent contig fragments are
   * combined.
   *
   * @return Returns an RDD of reads.
   */
  def toReads: RDD[AlignmentRecord] = {
    FragmentConverter.convertRdd(rdd)
  }

  def union(datasets: NucleotideContigFragmentDataset*): NucleotideContigFragmentDataset = {
    val iterableDatasets = datasets.toSeq
    NucleotideContigFragmentDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.sequences).fold(sequences)(_ ++ _))
  }

  /**
   * Replaces the underlying RDD with a new RDD.
   *
   * @param newRdd The RDD to use for the new NucleotideContigFragmentDataset.
   * @return Returns a new NucleotideContigFragmentDataset where the underlying RDD
   *   has been replaced.
   */
  protected def replaceRdd(newRdd: RDD[NucleotideContigFragment],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): NucleotideContigFragmentDataset = {
    new RDDBoundNucleotideContigFragmentDataset(newRdd, sequences, newPartitionMap)
  }

  /**
   * @param elem Fragment to extract a region from.
   * @return If a fragment is aligned to a reference location, returns a single
   *   reference region. If the fragment start position and name is not defined,
   *   returns no regions.
   */
  protected def getReferenceRegions(elem: NucleotideContigFragment): Seq[ReferenceRegion] = {
    ReferenceRegion(elem).toSeq
  }

  override def transformDataset(
    tFn: Dataset[NucleotideContigFragmentProduct] => Dataset[NucleotideContigFragmentProduct]): NucleotideContigFragmentDataset = {
    DatasetBoundNucleotideContigFragmentDataset(tFn(dataset), sequences)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[NucleotideContigFragmentProduct], Dataset[NucleotideContigFragmentProduct]]): NucleotideContigFragmentDataset = {
    DatasetBoundNucleotideContigFragmentDataset(tFn.call(dataset), sequences)
  }

  override def saveAsPartitionedParquet(pathName: String,
                                        compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
                                        partitionSize: Int = 1000000) {
    log.info("Saving directly as Hive-partitioned Parquet from SQL. " +
      "Options other than compression codec are ignored.")
    val df = toDF()
      .withColumnRenamed("contigName", "referenceName")
    df.withColumn("positionBin", floor(df("start") / partitionSize))
      .write
      .partitionBy("referenceName", "positionBin")
      .format("parquet")
      .option("spark.sql.parquet.compression.codec", compressCodec.toString.toLowerCase())
      .save(pathName)
    writePartitionedParquetFlag(pathName, partitionSize)
    saveMetadata(pathName)
  }

  /**
   * Save nucleotide contig fragments as Parquet or FASTA.
   *
   * If filename ends in .fa or .fasta, saves as Fasta. If not, saves fragments
   * to Parquet. Defaults to 60 character line length, if saving to FASTA.
   *
   * @param fileName file name
   * @param asSingleFile If false, writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   */
  def save(fileName: java.lang.String,
           asSingleFile: java.lang.Boolean) {
    if (fileName.endsWith(".fa") || fileName.endsWith(".fasta")) {
      saveAsFasta(fileName, asSingleFile = asSingleFile)
    } else {
      saveAsParquet(new JavaSaveArgs(fileName))
    }
  }

  /**
   * Save nucleotide contig fragments in FASTA format.
   *
   * @param fileName file name
   * @param lineWidth hard wrap FASTA formatted sequence at line width, default 60
   * @param asSingleFile By default (false), writes file to disk as shards with
   *   one shard per partition. If true, we save the file to disk as a single
   *   file by merging the shards.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   */
  def saveAsFasta(fileName: String,
                  lineWidth: Int = 60,
                  asSingleFile: Boolean = false,
                  disableFastConcat: Boolean = false) {

    def isFragment(record: NucleotideContigFragment): Boolean = {
      Option(record.getIndex).isDefined && Option(record.getFragments).fold(false)(_ > 1)
    }

    def toFasta(record: NucleotideContigFragment): String = {
      val sb = new StringBuilder()
      sb.append(">")
      sb.append(record.getContigName)
      Option(record.getDescription).foreach(n => sb.append(" ").append(n))
      if (isFragment(record)) {
        sb.append(s" fragment ${record.getIndex + 1} of ${record.getFragments}")
      }
      for (line <- Splitter.fixedLength(lineWidth).split(record.getSequence)) {
        sb.append("\n")
        sb.append(line)
      }
      sb.toString
    }

    val asFasta = rdd.map(toFasta)

    writeTextRdd(asFasta,
      fileName,
      asSingleFile,
      disableFastConcat)
  }

  /**
   * Merge fragments by contig name.
   *
   * @return Returns a NucleotideContigFragmentDataset containing a single fragment
   *   per contig.
   */
  def mergeFragments(): NucleotideContigFragmentDataset = {

    def merge(first: NucleotideContigFragment, second: NucleotideContigFragment): NucleotideContigFragment = {
      val merged = NucleotideContigFragment.newBuilder(first)
        .setIndex(null)
        .setStart(null)
        .setFragments(null)
        .setSequence(first.getSequence + second.getSequence)
        .build

      merged
    }

    replaceRdd(rdd.sortBy(fragment => (fragment.getContigName,
      Option(fragment.getIndex).map(_.toInt)
      .getOrElse(-1)))
      .map(fragment => (fragment.getContigName, fragment))
      .reduceByKey(merge)
      .values)
  }

  /**
   * From a set of contigs, returns the base sequence that corresponds to a region of the reference.
   *
   * @throws UnsupportedOperationException Throws exception if query region is not found.
   * @param region Reference region over which to get sequence.
   * @return String of bases corresponding to reference sequence.
   */
  def extract(region: ReferenceRegion): String = {
    def getString(fragment: (ReferenceRegion, NucleotideContigFragment)): (ReferenceRegion, String) = {
      val trimStart = max(0, region.start - fragment._1.start).toInt
      val trimEnd = max(0, fragment._1.end - region.end).toInt

      val fragmentSequence: String = fragment._2.getSequence

      val str = fragmentSequence.drop(trimStart)
        .dropRight(trimEnd)
      val reg = new ReferenceRegion(
        fragment._1.referenceName,
        fragment._1.start + trimStart,
        fragment._1.end - trimEnd
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
        "Merging fragments returned a different region than requested."
      )

      pair._2
    } catch {
      case (uoe: UnsupportedOperationException) =>
        throw new UnsupportedOperationException("Could not find " + region + "in reference RDD.")
    }
  }

  /**
   * (Java-specific) From a set of contigs, returns a list of sequences based on reference regions provided.
   *
   * @param regions List of Reference regions over which to get sequences.
   * @return JavaRDD[(ReferenceRegion, String)] of region -> sequence pairs.
   */
  def extractRegions(regions: java.util.List[ReferenceRegion]): JavaRDD[(ReferenceRegion, String)] = {
    extractRegions(asScalaBuffer(regions)).toJavaRDD()
  }

  /**
   * (Scala-specific) From a set of contigs, returns a list of sequences based on reference regions provided.
   *
   * @param regions Reference regions over which to get sequences.
   * @return RDD[(ReferenceRegion, String)] of region -> sequence pairs.
   */
  def extractRegions(regions: Iterable[ReferenceRegion]): RDD[(ReferenceRegion, String)] = {

    def extractSequence(fragmentRegion: ReferenceRegion, fragment: NucleotideContigFragment, region: ReferenceRegion): (ReferenceRegion, String) = {
      val merged = fragmentRegion.intersection(region)
      val start = (merged.start - fragmentRegion.start).toInt
      val end = (merged.end - fragmentRegion.start).toInt
      val fragmentSequence: String = fragment.getSequence
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
        case (fragmentRegion, fragment) =>
          regions.collect {
            case region if fragmentRegion.overlaps(region) =>
              (region, extractSequence(fragmentRegion, fragment, region))
          }
      }.sortByKey()

    places.reduceByKey(reduceRegionSequences).values
  }

  /**
   * (Java-specific) For all adjacent records in the genomic dataset, we extend the records so that the adjacent
   * records now overlap by _n_ bases, where _n_ is the flank length.
   *
   * @param flankLength The length to extend adjacent records by.
   * @return Returns the genomic dataset, with all adjacent fragments extended with flanking sequence.
   */
  def flankAdjacentFragments(
    flankLength: java.lang.Integer): NucleotideContigFragmentDataset = {
    val flank: Int = flankLength
    flankAdjacentFragments(flank)
  }

  /**
   * (Scala-specific) For all adjacent records in the genomic dataset, we extend the records so that the adjacent
   * records now overlap by _n_ bases, where _n_ is the flank length.
   *
   * @param flankLength The length to extend adjacent records by.
   * @return Returns the genomic dataset, with all adjacent fragments extended with flanking sequence.
   */
  def flankAdjacentFragments(
    flankLength: Int): NucleotideContigFragmentDataset = {
    replaceRdd(FlankReferenceFragments(rdd,
      sequences,
      flankLength))
  }

  /**
   * (Scala-specific) Counts the k-mers contained in a FASTA contig.
   *
   * @param kmerLength The length of k-mers to count.
   * @return Returns an RDD containing k-mer/count pairs.
   */
  def countKmers(kmerLength: Int): RDD[(String, Long)] = {
    flankAdjacentFragments(kmerLength).rdd.flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getSequence
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }

  /**
   * (Java-specific) Counts the k-mers contained in a FASTA contig.
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
}
