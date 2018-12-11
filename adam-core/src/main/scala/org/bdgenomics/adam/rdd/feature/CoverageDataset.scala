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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.FieldSerializer
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models.{
  Coverage,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.{
  MultisampleGenomicDataset,
  DatasetBoundGenomicDataset,
  GenomicDataset
}
import org.bdgenomics.formats.avro.Sample
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[adam] case class CoverageArray(
    array: Array[(ReferenceRegion, Coverage)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Coverage] {

  def duplicate(): IntervalArray[ReferenceRegion, Coverage] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Coverage)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Coverage] = {
    CoverageArray(arr, maxWidth)
  }
}

private[adam] class CoverageArraySerializer(kryo: Kryo) extends IntervalArraySerializer[ReferenceRegion, Coverage, CoverageArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new FieldSerializer[Coverage](kryo, classOf[Coverage])

  protected def builder(arr: Array[(ReferenceRegion, Coverage)],
                        maxIntervalWidth: Long): CoverageArray = {
    CoverageArray(arr, maxIntervalWidth)
  }
}

object CoverageDataset {

  /**
   * A GenomicDataset that wraps a dataset of Coverage data with an empty sequence dictionary.
   *
   * @param ds A Dataset of genomic Coverage features.
   */
  def apply(ds: Dataset[Coverage]): CoverageDataset = {
    new DatasetBoundCoverageDataset(ds, SequenceDictionary.empty, Seq.empty[Sample])
  }

  /**
   * A GenomicDataset that wraps a dataset of Coverage data given a sequence dictionary.
   *
   * @param ds A Dataset of genomic Coverage features.
   * @param sequences The reference genome these data are aligned to.
   * @param samples The samples in this Dataset.
   */
  def apply(ds: Dataset[Coverage],
            sequences: SequenceDictionary,
            samples: Seq[Sample]): CoverageDataset = {
    new DatasetBoundCoverageDataset(ds, sequences, samples)
  }

  /**
   * A CoverageDataset that wraps an RDD of Coverage data with an empty sequence dictionary.
   *
   * @param rdd The underlying Coverage RDD to build from.
   * @return Returns a new CoverageDataset.
   */
  def apply(rdd: RDD[Coverage]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, SequenceDictionary.empty, Seq.empty[Sample], None)
  }

  /**
   * A CoverageDataset that wraps an RDD of Coverage data given a sequence dictionary.
   *
   * @param rdd The underlying Coverage RDD to build from.
   * @param sd The sequence dictionary for this CoverageDataset.
   * @param samples The samples in this CoverageDataset.
   * @return Returns a new CoverageDataset.
   */
  def apply(rdd: RDD[Coverage],
            sd: SequenceDictionary,
            samples: Seq[Sample]): CoverageDataset = {
    new RDDBoundCoverageDataset(rdd, sd, samples, None)
  }
}

case class ParquetUnboundCoverageDataset private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample]) extends CoverageDataset {

  lazy val rdd: RDD[Coverage] = {
    sc.loadParquetCoverage(parquetFilename,
      forceRdd = true).rdd
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename)
      .select("referenceName", "start", "end", "score", "sampleId")
      .withColumnRenamed("sampleId", "optSampleId")
      .withColumnRenamed("score", "count")
      .as[Coverage]
  }

  override def toFeatures(): FeatureDataset = {
    ParquetUnboundFeatureDataset(sc, parquetFilename, sequences, samples)
  }

  override def replaceSequences(newSequences: SequenceDictionary): CoverageDataset = {
    copy(sequences = newSequences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): CoverageDataset = {
    copy(samples = newSamples.toSeq)
  }
}

case class DatasetBoundCoverageDataset private[rdd] (
  dataset: Dataset[Coverage],
  sequences: SequenceDictionary,
  @transient samples: Seq[Sample],
  override val isPartitioned: Boolean = false,
  override val optPartitionBinSize: Option[Int] = None,
  override val optLookbackPartitions: Option[Int] = None) extends CoverageDataset
    with DatasetBoundGenomicDataset[Coverage, Coverage, CoverageDataset] {

  protected lazy val optPartitionMap = None

  lazy val rdd: RDD[Coverage] = {
    dataset.rdd
  }

  override def toFeatures(): FeatureDataset = {
    import dataset.sqlContext.implicits._
    DatasetBoundFeatureDataset(dataset.map(_.toSqlFeature), sequences, samples)
  }

  override def replaceSequences(newSequences: SequenceDictionary): CoverageDataset = {
    copy(sequences = newSequences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): CoverageDataset = {
    copy(samples = newSamples.toSeq)
  }
}

case class RDDBoundCoverageDataset private[rdd] (
    rdd: RDD[Coverage],
    sequences: SequenceDictionary,
    @transient samples: Seq[Sample],
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends CoverageDataset {

  lazy val dataset: Dataset[Coverage] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd)
  }

  override def toFeatures(): FeatureDataset = {
    val features = rdd.map(_.toFeature)
    new RDDBoundFeatureDataset(features, sequences, samples, optPartitionMap = optPartitionMap)
  }

  override def replaceSequences(newSequences: SequenceDictionary): CoverageDataset = {
    copy(sequences = newSequences)
  }

  override def replaceSamples(newSamples: Iterable[Sample]): CoverageDataset = {
    copy(samples = newSamples.toSeq)
  }
}

abstract class CoverageDataset
    extends MultisampleGenomicDataset[Coverage, Coverage, CoverageDataset]
    with GenomicDataset[Coverage, Coverage, CoverageDataset] {

  protected val productFn = (c: Coverage) => c
  protected val unproductFn = (c: Coverage) => c

  @transient val uTag: TypeTag[Coverage] = typeTag[Coverage]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Coverage)])(
    implicit tTag: ClassTag[Coverage]): IntervalArray[ReferenceRegion, Coverage] = {
    IntervalArray(rdd, CoverageArray.apply(_, _))
  }

  def union(datasets: CoverageDataset*): CoverageDataset = {
    val iterableDatasets = datasets.toSeq
    val mergedSequences = iterableDatasets.map(_.sequences).fold(sequences)(_ ++ _)
    val mergedSamples = (samples ++ iterableDatasets.flatMap(_.samples)).distinct.toSeq

    if (iterableDatasets.forall(dataset => dataset match {
      case DatasetBoundCoverageDataset(_, _, _, _, _, _) => true
      case _ => false
    })) {
      DatasetBoundCoverageDataset(iterableDatasets.map(_.dataset)
        .fold(dataset)(_.union(_)), mergedSequences, mergedSamples)
    } else {
      RDDBoundCoverageDataset(rdd.context.union(rdd, iterableDatasets.map(_.rdd): _*),
        mergedSequences,
        mergedSamples,
        None)
    }
  }

  def saveAsParquet(filePath: String,
                    blockSize: Int = 128 * 1024 * 1024,
                    pageSize: Int = 1 * 1024 * 1024,
                    compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
                    disableDictionaryEncoding: Boolean = false) {

    toFeatures().saveAsParquet(filePath,
      blockSize,
      pageSize,
      compressCodec,
      disableDictionaryEncoding)
  }

  override def transformDataset(
    tFn: Dataset[Coverage] => Dataset[Coverage]): CoverageDataset = {
    DatasetBoundCoverageDataset(tFn(dataset), sequences, samples)
  }

  override def transformDataset(
    tFn: JFunction[Dataset[Coverage], Dataset[Coverage]]): CoverageDataset = {
    DatasetBoundCoverageDataset(tFn.call(dataset), sequences, samples)
  }

  /**
   * Saves coverage as feature file.
   *
   * @see FeatureDataset.save
   *
   * Coverage is saved as a feature where coverage is stored in the score column
   * and sampleId is stored in attributes, if available.
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
           disableFastConcat: java.lang.Boolean) = {
    toFeatures.save(filePath,
      asSingleFile,
      disableFastConcat)
  }

  /**
   * Merges adjacent ReferenceRegions with the same coverage value.
   * This reduces the loss of coverage information while reducing the number of records in the RDD.
   * For example, adjacent records Coverage("chr1", 1, 10, 3.0) and Coverage("chr1", 10, 20, 3.0)
   * would be merged into one record Coverage("chr1", 1, 20, 3.0).
   *
   * @note Data must be sorted before collapse is called.
   *
   * @return merged tuples of adjacent ReferenceRegions and coverage.
   */
  def collapse(): CoverageDataset = {
    val newRDD: RDD[Coverage] = rdd
      .mapPartitions(iter => {
        // must sort values to iteratively collapse coverage
        val sortedIter = iter.toList
          .sortBy(r => (r.referenceName, r.start))
          .toIterator
        if (sortedIter.hasNext) {
          val first = sortedIter.next
          collapse(sortedIter, first, List.empty)
        } else sortedIter
      })

    transform(rdd => newRDD)
  }

  /**
   * Tail recursion for merging adjacent ReferenceRegions with the same value.
   *
   * @param iter partition iterator of ReferenceRegion and coverage values.
   * @param lastCoverage the last coverage from a sorted Iterator that has been considered to merge.
   * @param condensed Condensed iterator of iter with adjacent regions with the same value merged.
   * @return merged tuples of adjacent ReferenceRegions and coverage.
   */
  @tailrec private def collapse(iter: Iterator[Coverage],
                                lastCoverage: Coverage,
                                condensed: List[Coverage]): Iterator[Coverage] = {
    if (!iter.hasNext) {
      // if lastCoverage has not yet been added, add to condensed
      val nextCondensed =
        if (condensed.map(r => ReferenceRegion(r)).filter(_.overlaps(ReferenceRegion(lastCoverage))).isEmpty) {
          lastCoverage :: condensed
        } else {
          condensed
        }
      nextCondensed.toIterator
    } else {
      val cov = iter.next
      val rr = ReferenceRegion(cov)
      val lastRegion = ReferenceRegion(lastCoverage)
      val (nextCoverage, nextCondensed) =
        if (rr.isAdjacent(lastRegion) && lastCoverage.count == cov.count) {
          (Coverage(rr.merge(lastRegion), lastCoverage.count, lastCoverage.optSampleId), condensed)
        } else {
          (cov, lastCoverage :: condensed)
        }
      collapse(iter, nextCoverage, nextCondensed)
    }
  }

  /**
   * Converts CoverageDataset to FeatureDataset.
   *
   * @return Returns a FeatureDataset from CoverageDataset.
   */
  def toFeatures(): FeatureDataset

  /**
   * (Java-specific) Gets coverage overlapping specified ReferenceRegion.
   *
   * For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified
   * to bin together ReferenceRegions of equal size. The coverage of each bin is
   * coverage of the first base pair in that bin.
   *
   * @param bpPerBin base pairs per bin, number of bases to combine to one bin.
   * @return Genomic dataset of Coverage Records.
   */
  def coverage(bpPerBin: java.lang.Integer): CoverageDataset = {
    val bp: Int = bpPerBin
    coverage(bpPerBin = bp)
  }

  /**
   * (Scala-specific) Gets coverage overlapping specified ReferenceRegion.
   *
   * For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified
   * to bin together ReferenceRegions of equal size. The coverage of each bin is
   * coverage of the first base pair in that bin.
   *
   * @param bpPerBin base pairs per bin, number of bases to combine to one bin.
   * @return Genomic dataset of Coverage Records.
   */
  def coverage(bpPerBin: Int = 1): CoverageDataset = {

    val flattened = flatten()

    if (bpPerBin == 1) {
      flattened // no binning, return raw results
    } else {
      // subtract region.start to shift mod to start of ReferenceRegion
      val newRDD = flattened.rdd.filter(r => r.start % bpPerBin == 0)
      flattened.transform(rdd => newRDD)
    }
  }

  /**
   * (Java-specific) Gets coverage overlapping specified ReferenceRegion.
   *
   * For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified
   * to bin together ReferenceRegions of equal size. The coverage of each bin is
   * the mean coverage over all base pairs in that bin.
   *
   * @param bpPerBin base pairs per bin, number of bases to combine to one bin.
   * @return Genomic dataset of Coverage Records.
   */
  def aggregatedCoverage(bpPerBin: java.lang.Integer): CoverageDataset = {
    val bp: Int = bpPerBin
    aggregatedCoverage(bpPerBin = bp)
  }

  /**
   * (Scala-specific) Gets coverage overlapping specified ReferenceRegion.
   *
   * For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified
   * to bin together ReferenceRegions of equal size. The coverage of each bin is
   * the mean coverage over all base pairs in that bin.
   *
   * @param bpPerBin base pairs per bin, number of bases to combine to one bin.
   * @return Genomic dataset of Coverage Records.
   */
  def aggregatedCoverage(bpPerBin: Int = 1): CoverageDataset = {

    val flattened = flatten()

    def reduceFn(a: (Double, Int), b: (Double, Int)): (Double, Int) = {
      (a._1 + b._1, a._2 + b._2)
    }

    if (bpPerBin == 1) {
      flattened // no binning, return raw results
    } else {
      val newRDD = flattened.rdd
        .keyBy(r => {
          // key coverage by binning start site mod bpPerbin
          // subtract region.start to shift mod to start of ReferenceRegion
          val start = r.start - (r.start % bpPerBin)
          (r.optSampleId, ReferenceRegion(r.referenceName, start, start + bpPerBin))
        }).mapValues(r => (r.count, 1))
        .reduceByKey(reduceFn)
        .map(r => {
          // r is a key of (id: string, region: ReferenceRegion), and value of (count: double, int)
          // compute average coverage in bin
          Coverage(r._1._2.referenceName, r._1._2.start, r._1._2.end, r._2._1 / r._2._2, r._1._1)
        })
      flattened.transform(rdd => newRDD)
    }
  }

  /**
   * Gets sequence of ReferenceRegions from Coverage element.
   * Since coverage maps directly to a single genomic region, this method will always
   * return a Seq of exactly one ReferenceRegion.
   *
   * @param elem The Coverage to get an underlying region for.
   * @return Sequence of ReferenceRegions extracted from Coverage.
   */
  protected def getReferenceRegions(elem: Coverage): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem.referenceName, elem.start, elem.end))
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new CoverageDataset with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Coverage],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): CoverageDataset = {
    RDDBoundCoverageDataset(newRdd, sequences, samples, newPartitionMap)
  }

  /**
   * Gets flattened genomic dataset of coverage, with coverage mapped to a ReferenceRegion at each base pair.
   *
   * @return CoverageDataset of flattened Coverage records.
   */
  def flatten(): CoverageDataset = {
    transform(rdd => flatMapCoverage(rdd))
  }

  /**
   * Flat maps coverage into ReferenceRegion and counts for each base pair.
   *
   * @param rdd RDD of Coverage.
   * @return Genomic dataset of flattened Coverage.
   */
  private def flatMapCoverage(rdd: RDD[Coverage]): RDD[Coverage] = {
    rdd.flatMap(r => {
      val positions = r.start until r.end
      positions.map(n => Coverage(r.referenceName, n, n + 1, r.count, r.optSampleId))
    })
  }
}

