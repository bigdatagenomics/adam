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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models.{
  Coverage,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.GenomicDataset
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

case class ParquetUnboundCoverageRDD private[rdd] (
    @transient private val sc: SparkContext,
    private val parquetFilename: String,
    sequences: SequenceDictionary) extends CoverageRDD {

  lazy val rdd: RDD[Coverage] = {
    sc.loadParquetCoverage(parquetFilename,
      forceRdd = true).rdd
  }

  protected lazy val optPartitionMap = sc.extractPartitionMap(parquetFilename)

  lazy val dataset = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    sqlContext.read.parquet(parquetFilename)
      .select("contigName", "start", "end", "score")
      .withColumnRenamed("score", "count")
      .as[Coverage]
  }

  def toFeatures(): FeatureRDD = {
    ParquetUnboundFeatureRDD(sc, parquetFilename, sequences)
  }

  def replaceSequences(
    newSequences: SequenceDictionary): CoverageRDD = {
    copy(sequences = newSequences)
  }
}

/**
 * A Dataset containing Coverage data.
 *
 * @param dataset A SQL Dataset containing data describing how many reads cover
 *   a genomic locus/region.
 * @param sequences A dictionary describing the reference genome.
 */
case class DatasetBoundCoverageRDD private[rdd] (
    dataset: Dataset[Coverage],
    sequences: SequenceDictionary) extends CoverageRDD {

  protected lazy val optPartitionMap = None

  lazy val rdd: RDD[Coverage] = {
    dataset.rdd
  }

  def toFeatures(): FeatureRDD = {
    import dataset.sqlContext.implicits._
    DatasetBoundFeatureRDD(dataset.map(_.toSqlFeature), sequences)
  }

  def replaceSequences(
    newSequences: SequenceDictionary): CoverageRDD = {
    copy(sequences = newSequences)
  }
}

/**
 * An RDD containing Coverage data.
 *
 * @param rdd An RDD containing data describing how many reads cover a genomic
 *   locus/region.
 * @param sequences A dictionary describing the reference genome.
 */
case class RDDBoundCoverageRDD private[rdd] (
    rdd: RDD[Coverage],
    sequences: SequenceDictionary,
    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends CoverageRDD {

  lazy val dataset: Dataset[Coverage] = {
    val sqlContext = SQLContext.getOrCreate(rdd.context)
    import sqlContext.implicits._
    sqlContext.createDataset(rdd)
  }

  def toFeatures(): FeatureRDD = {
    val featureRdd = rdd.map(_.toFeature)
    new RDDBoundFeatureRDD(featureRdd, sequences, optPartitionMap = optPartitionMap)
  }

  def replaceSequences(
    newSequences: SequenceDictionary): CoverageRDD = {
    copy(sequences = newSequences)
  }
}

abstract class CoverageRDD extends GenomicDataset[Coverage, Coverage, CoverageRDD] {

  @transient val uTag: TypeTag[Coverage] = typeTag[Coverage]

  protected def buildTree(rdd: RDD[(ReferenceRegion, Coverage)])(
    implicit tTag: ClassTag[Coverage]): IntervalArray[ReferenceRegion, Coverage] = {
    IntervalArray(rdd, CoverageArray.apply(_, _))
  }

  def union(rdds: CoverageRDD*): CoverageRDD = {
    val iterableRdds = rdds.toSeq

    val mergedSequences = iterableRdds.map(_.sequences).fold(sequences)(_ ++ _)

    if (iterableRdds.forall(rdd => rdd match {
      case DatasetBoundCoverageRDD(_, _) => true
      case _                             => false
    })) {
      DatasetBoundCoverageRDD(iterableRdds.map(_.dataset)
        .fold(dataset)(_.union(_)), mergedSequences)
    } else {
      RDDBoundCoverageRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
        mergedSequences,
        None)
    }
  }

  def transformDataset(
    tFn: Dataset[Coverage] => Dataset[Coverage]): CoverageRDD = {
    DatasetBoundCoverageRDD(tFn(dataset), sequences)
  }

  /**
   * Saves coverage as feature file.
   *
   * @see FeatureRDD.save
   *
   * Supported file formats include bed, narrowPeak and parquet. Coverage is saved
   * as a feature where coverage is stored in score attribute.
   *   val chrom    = feature.getContigName
   *   val start    = feature.getStart
   *   val end      = feature.getEnd
   *   val name     = Features.nameOf(feature)
   *   val coverage = feature.getScore
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
  def collapse(): CoverageRDD = {
    val newRDD: RDD[Coverage] = rdd
      .mapPartitions(iter => {
        if (iter.hasNext) {
          val first = iter.next
          collapse(iter, first, List.empty)
        } else iter
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
          (Coverage(rr.merge(lastRegion), lastCoverage.count), condensed)
        } else {
          (cov, lastCoverage :: condensed)
        }
      collapse(iter, nextCoverage, nextCondensed)
    }
  }

  /**
   * Converts CoverageRDD to FeatureRDD.
   *
   * @return Returns a FeatureRDD from CoverageRDD.
   */
  def toFeatures(): FeatureRDD

  /**
   * Gets coverage overlapping specified ReferenceRegion.
   *
   * For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified
   * to bin together ReferenceRegions of equal size. The coverage of each bin is
   * coverage of the first base pair in that bin. Java friendly variant.
   *
   * @param bpPerBin base pairs per bin, number of bases to combine to one bin.
   * @return RDD of Coverage Records.
   */
  def coverage(bpPerBin: java.lang.Integer): CoverageRDD = {
    val bp: Int = bpPerBin
    coverage(bpPerBin = bp)
  }

  /**
   * Gets coverage overlapping specified ReferenceRegion.
   * For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified
   * to bin together ReferenceRegions of equal size. The coverage of each bin is
   * coverage of the first base pair in that bin.
   *
   * @param bpPerBin base pairs per bin, number of bases to combine to one bin.
   * @return RDD of Coverage Records.
   */
  def coverage(bpPerBin: Int = 1): CoverageRDD = {

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
   * Gets coverage overlapping specified ReferenceRegion.
   *
   * For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified
   * to bin together ReferenceRegions of equal size. The coverage of each bin is
   * the mean coverage over all base pairs in that bin. Java friendly variant.
   *
   * @param bpPerBin base pairs per bin, number of bases to combine to one bin.
   * @return RDD of Coverage Records.
   */
  def aggregatedCoverage(bpPerBin: java.lang.Integer): CoverageRDD = {
    val bp: Int = bpPerBin
    aggregatedCoverage(bpPerBin = bp)
  }

  /**
   * Gets coverage overlapping specified ReferenceRegion.
   *
   * For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified
   * to bin together ReferenceRegions of equal size. The coverage of each bin is
   * the mean coverage over all base pairs in that bin.
   *
   * @param bpPerBin base pairs per bin, number of bases to combine to one bin.
   * @return RDD of Coverage Records.
   */
  def aggregatedCoverage(bpPerBin: Int = 1): CoverageRDD = {

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
          ReferenceRegion(r.contigName, start, start + bpPerBin)
        }).mapValues(r => (r.count, 1))
        .reduceByKey(reduceFn)
        .map(r => {
          // compute average coverage in bin
          Coverage(r._1.referenceName, r._1.start, r._1.end, r._2._1 / r._2._2)
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
    Seq(ReferenceRegion(elem.contigName, elem.start, elem.end))
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new CoverageRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Coverage],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): CoverageRDD = {
    RDDBoundCoverageRDD(newRdd, sequences, newPartitionMap)
  }

  /**
   * Gets flattened RDD of coverage, with coverage mapped to a ReferenceRegion at each base pair.
   *
   * @return CoverageRDD of flattened Coverage records.
   */
  def flatten(): CoverageRDD = {
    transform(rdd => flatMapCoverage(rdd))
  }

  /**
   * Flat maps coverage into ReferenceRegion and counts for each base pair.
   *
   * @param rdd RDD of Coverage.
   * @return RDD of flattened Coverage.
   */
  private def flatMapCoverage(rdd: RDD[Coverage]): RDD[Coverage] = {
    rdd.flatMap(r => {
      val positions = r.start until r.end
      positions.map(n => Coverage(r.contigName, n, n + 1, r.count))
    })
  }
}

