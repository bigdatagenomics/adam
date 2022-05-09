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
package org.bdgenomics.adam.ds.sequence

import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.ds.read.ReadDataset
import org.bdgenomics.adam.ds.{
  AvroGenomicDataset,
  DatasetBoundGenomicDataset,
  JavaSaveArgs,
  MultisampleGenomicDataset
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.adam.sql.{ Slice => SliceProduct }
import org.bdgenomics.formats.avro.{
  Read,
  Sample,
  Sequence,
  Slice
}
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import scala.collection.JavaConverters.{ asScalaBuffer, mapAsJavaMap, mapAsScalaMap }
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

object SliceDataset {

  /**
   * A genomic dataset that wraps a dataset of Slice data.
   *
   * @param ds A Dataset of slices.
   */
  def apply(ds: Dataset[SliceProduct]): SliceDataset = {
    DatasetBoundSliceDataset(ds, SequenceDictionary.empty, Seq.empty[Sample])
  }

  /**
   * A genomic dataset that wraps a dataset of Slice data.
   *
   * @param ds A Dataset of slices.
   * @param references The reference genome these data are aligned to.
   * @param samples Samples for these slices.
   */
  def apply(ds: Dataset[SliceProduct],
            references: SequenceDictionary,
            samples: Iterable[Sample]): SliceDataset = {
    DatasetBoundSliceDataset(ds, references, samples.toSeq)
  }

  /**
   * A genomic dataset that wraps an RDD of Sequence data.
   *
   * @param rdd The underlying Slice RDD to build from.
   * @return Returns a new SliceDataset.
   */
  def apply(rdd: RDD[Slice]): SliceDataset = {
    SliceDataset(rdd, SequenceDictionary.empty, Iterable.empty[Sample])
  }

  /**
   * Builds a SliceDataset given a sequence dictionary.
   *
   * @param rdd The underlying Slice RDD to build from.
   * @param references The reference genome these data are aligned to.
   * @param samples Samples for these sequences.
   * @return Returns a new SliceDataset.
   */
  def apply(rdd: RDD[Slice],
            references: SequenceDictionary,
            samples: Iterable[Sample]): SliceDataset = {
    RDDBoundSliceDataset(rdd, references, samples.toSeq, None)
  }
}

case class ParquetUnboundSliceDataset private[ds] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    references: SequenceDictionary,
    @transient samples: Seq[Sample]) extends SliceDataset {

  lazy val rdd: RDD[Slice] = {
    sc.loadParquet(parquetFilename)
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset: Dataset[SliceProduct] = {
    import spark.implicits._
    spark.read.parquet(parquetFilename).as[SliceProduct]
  }

  def replaceReferences(newReferences: SequenceDictionary): SliceDataset = {
    copy(references = newReferences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): SliceDataset = {
    copy(samples = newSamples.toSeq)
  }
}

case class DatasetBoundSliceDataset private[ds] (
  dataset: Dataset[SliceProduct],
  references: SequenceDictionary,
  @transient samples: Seq[Sample],
  override val isPartitioned: Boolean = true,
  override val optPartitionBinSize: Option[Int] = Some(1000000),
  override val optLookbackPartitions: Option[Int] = Some(1)) extends SliceDataset
    with DatasetBoundGenomicDataset[Slice, SliceProduct, SliceDataset] {

  lazy val rdd: RDD[Slice] = dataset.rdd.map(_.toAvro)
  protected lazy val optPartitionMap = None

  override def saveAsParquet(filePath: String,
                             blockSize: Int = 128 * 1024 * 1024,
                             pageSize: Int = 1 * 1024 * 1024,
                             compressionCodec: CompressionCodecName = CompressionCodecName.GZIP,
                             disableDictionaryEncoding: Boolean = false) {
    warn("Saving directly as Parquet from SQL. Options other than compression codec are ignored.")
    dataset.toDF()
      .write
      .format("parquet")
      .option("spark.sql.parquet.compression.codec", compressionCodec.toString.toLowerCase())
      .save(filePath)
    saveMetadata(filePath)
  }

  override def transformDataset(
    tFn: Dataset[SliceProduct] => Dataset[SliceProduct]): SliceDataset = {
    copy(dataset = tFn(dataset))
  }

  override def transformDataset(
    tFn: JFunction[Dataset[SliceProduct], Dataset[SliceProduct]]): SliceDataset = {
    copy(dataset = tFn.call(dataset))
  }

  def replaceReferences(newReferences: SequenceDictionary): SliceDataset = {
    copy(references = newReferences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): SliceDataset = {
    copy(samples = newSamples.toSeq)
  }
}

case class RDDBoundSliceDataset private[ds] (
    rdd: RDD[Slice],
    references: SequenceDictionary,
    @transient samples: Seq[Sample],
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends SliceDataset {

  /**
   * A SQL Dataset of slices.
   */
  lazy val dataset: Dataset[SliceProduct] = {
    import spark.implicits._
    spark.createDataset(rdd.map(SliceProduct.fromAvro))
  }

  def replaceReferences(newReferences: SequenceDictionary): SliceDataset = {
    copy(references = newReferences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): SliceDataset = {
    copy(samples = newSamples.toSeq)
  }
}

sealed abstract class SliceDataset extends AvroGenomicDataset[Slice, SliceProduct, SliceDataset]
    with MultisampleGenomicDataset[Slice, SliceProduct, SliceDataset] {

  protected val productFn = SliceProduct.fromAvro(_)
  protected val unproductFn = (s: SliceProduct) => s.toAvro

  @transient val uTag: TypeTag[SliceProduct] = typeTag[SliceProduct]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Slice)])(
    implicit tTag: ClassTag[Slice]): IntervalArray[ReferenceRegion, Slice] = {
    IntervalArray(rdd, SliceArray.apply(_, _))
  }

  override protected def saveMetadata(pathName: String): Unit = {
    savePartitionMap(pathName)
    saveReferences(pathName)
    saveSamples(pathName)
  }

  def union(datasets: SliceDataset*): SliceDataset = {
    val iterableDatasets = datasets.toSeq
    SliceDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
      iterableDatasets.map(_.references).fold(references)(_ ++ _),
      iterableDatasets.map(_.samples).fold(samples)(_ ++ _))
  }

  override def transformDataset(
    tFn: Dataset[SliceProduct] => Dataset[SliceProduct]): SliceDataset = {
    DatasetBoundSliceDataset(tFn(dataset), references, samples)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[SliceProduct], Dataset[SliceProduct]]): SliceDataset = {
    DatasetBoundSliceDataset(tFn.call(dataset), references, samples)
  }

  /**
   * Filter this SliceDataset by sample to those that match the specified sample.
   *
   * @param sampleId Sample to filter by.
   * return SliceDataset filtered by sample.
   */
  def filterToSample(sampleId: String): SliceDataset = {
    transform((rdd: RDD[Slice]) => rdd.filter(s => Option(s.getSampleId).contains(sampleId)))
  }

  /**
   * (Java-specific) Filter this SliceDataset by sample to those that match the specified samples.
   *
   * @param sampleIds List of samples to filter by.
   * return SliceDataset filtered by one or more samples.
   */
  def filterToSamples(sampleIds: java.util.List[String]): SliceDataset = {
    filterToSamples(asScalaBuffer(sampleIds))
  }

  /**
   * (Scala-specific) Filter this SliceDataset by sample to those that match the specified samples.
   *
   * @param sampleIds Sequence of samples to filter by.
   * return SliceDataset filtered by one or more samples.
   */
  def filterToSamples(sampleIds: Seq[String]): SliceDataset = {
    transform((rdd: RDD[Slice]) => rdd.filter(s => Option(s.getSampleId).exists(sampleIds.contains(_))))
  }

  /**
   * Merge slices into sequences.
   *
   * @return Returns a SequenceDataset containing merged slices.
   */
  def merge(): SequenceDataset = {
    def toSequence(slice: Slice): Sequence = {
      val sb = Sequence.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setSampleId(slice.getSampleId)
        .setAttributes(slice.getAttributes)

      Option(slice.getSampleId).foreach(sampleId => sb.setSampleId(sampleId))
      sb.build
    }

    def mergeSequences(first: Sequence, second: Sequence): Sequence = {
      val sb = Sequence.newBuilder(first)
        .setLength(first.getLength + second.getLength)
        .setSequence(first.getSequence + second.getSequence)
        .setAttributes(mapAsJavaMap(mapAsScalaMap(first.getAttributes) ++ mapAsScalaMap(second.getAttributes)))

      Option(first.getSampleId).foreach(sampleId => sb.setSampleId(sampleId))
      sb.build
    }

    val merged: RDD[Sequence] = rdd
      .sortBy(slice => (slice.getName, slice.getStart))
      .map(slice => (slice.getName, toSequence(slice)))
      .reduceByKey(mergeSequences)
      .values

    SequenceDataset(merged, references, samples)
  }

  /**
   * Convert this genomic dataset of slices into reads.
   *
   * @return Returns a new ReadDataset converted from this genomic dataset of slices.
   */
  def toReads: ReadDataset = {
    def toRead(slice: Slice): Read = {
      val rb = Read.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setQualityScores("B" * (if (slice.getLength == null) 0 else slice.getLength.toInt))
        .setAttributes(slice.getAttributes)

      Option(slice.getSampleId).foreach(sampleId => rb.setSampleId(sampleId))
      rb.build
    }
    ReadDataset(rdd.map(toRead), references, samples)
  }

  /**
   * Convert this genomic dataset of slices into sequences.
   *
   * @return Returns a new SequenceDataset converted from this genomic dataset of slices.
   */
  def toSequences: SequenceDataset = {
    def toSequence(slice: Slice): Sequence = {
      val sb = Sequence.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setAttributes(slice.getAttributes)

      Option(slice.getSampleId).foreach(sampleId => sb.setSampleId(sampleId))
      sb.build
    }
    SequenceDataset(rdd.map(toSequence), references, samples)
  }

  /**
   * Replace the references for this SliceDataset with those
   * created from the slices in this SliceDataset.
   *
   * @return Returns a new SliceDataset with the references replaced.
   */
  def createReferences(): SliceDataset = {
    val references = new SequenceDictionary(rdd.flatMap(slice => {
      if (slice.getName != null) {
        Some(SequenceRecord.fromSlice(slice))
      } else {
        None
      }
    }).distinct
      .collect
      .toVector)

    replaceReferences(references)
  }

  /**
   * Save slices as Parquet or FASTA.
   *
   * If filename ends in .fa or .fasta, saves as FASTA. If not, saves slices
   * to Parquet. Defaults to 60 character line length, if saving to FASTA.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   * @param disableFastConcat If asSingleFile is true, disables the use of the
   *   parallel file merging engine.
   */
  def save(
    filePath: java.lang.String,
    asSingleFile: java.lang.Boolean,
    disableFastConcat: java.lang.Boolean) {
    if (filePath.endsWith(".fa") || filePath.endsWith(".fasta")) {
      saveAsFasta(filePath, asSingleFile = asSingleFile, disableFastConcat = disableFastConcat)
    } else {
      if (asSingleFile) {
        warn("asSingleFile = true ignored when saving as Parquet.")
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
   * Extract the specified region from this genomic dataset of slices as a string, merging
   * slices if necessary.
   *
   * @param region Region to extract.
   * @return Return the specified region from this genomic dataset of slices as a string, merging
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
   * Extract the specified regions from this genomic dataset of slices as an RDD of (ReferenceRegion,
   * String) tuples, merging slices if necessary.
   *
   * @param regions Zero or more regions to extract.
   * @return Return the specified regions from this genomic dataset of slices as an RDD of (ReferenceRegion,
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
   * (Java-specific) For all adjacent slices in this genomic dataset, we extend the slices so that the adjacent
   * slices now overlap by _n_ bases, where _n_ is the flank length.
   *
   * @param flankLength The length to extend adjacent slices by.
   * @return Returns this genomic dataset, with all adjacent slices extended with flanking sequence.
   */
  def flankAdjacent(flankLength: java.lang.Integer): SliceDataset = {
    val flank: Int = flankLength
    flankAdjacent(flank)
  }

  /**
   * (Scala-specific) For all adjacent slices in this genomic dataset, we extend the slices so that the adjacent
   * slices now overlap by _n_ bases, where _n_ is the flank length.
   *
   * @param flankLength The length to extend adjacent slices by.
   * @return Returns this genomic dataset, with all adjacent slices extended with flanking sequence.
   */
  def flankAdjacent(flankLength: Int): SliceDataset = {
    replaceRdd(FlankSlices(rdd,
      references,
      flankLength))
  }

  /**
   * (Scala-specific) Cuts slices after flanking into _k_-mers, and then counts the
   * number of occurrences of each _k_-mer.
   *
   * @param kmerLength The value of _k_ to use for cutting _k_-mers.
   * @return Returns an RDD containing k-mer/count pairs.
   */
  def countKmers(kmerLength: Int): RDD[(String, Long)] = {
    flankAdjacent(kmerLength).rdd.flatMap(r => {
      // first slice has no left flank
      if (r.getStart == 0) {
        r.getSequence
          .sliding(kmerLength)
          .map(k => (k, 1L))
      } else {
        // account for duplicate kmers on left flank
        r.getSequence
          .substring(kmerLength + 1)
          .sliding(kmerLength)
          .map(k => (k, 1L))
      }
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }

  /**
   * (Java-specific) Cuts slices after flanking into _k_-mers, and then counts the
   * number of occurrences of each _k_-mer.
   *
   * @param kmerLength The value of _k_ to use for cutting _k_-mers.
   * @return Returns a JavaRDD containing k-mer/count pairs.
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
   * @param newPartitionMap New partition map, if any.
   * @return Returns a new SliceDataset with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Slice],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): SliceDataset = {
    RDDBoundSliceDataset(newRdd, references, samples, newPartitionMap)
  }

  /**
   * @param slice Slice to extract a region from.
   * @return Returns a reference region that covers the entirety of the slice.
   */
  protected def getReferenceRegions(slice: Slice): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(slice.getName, slice.getStart, slice.getEnd, slice.getStrand))
  }
}
