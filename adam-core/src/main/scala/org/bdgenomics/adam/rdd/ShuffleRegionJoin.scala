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

package org.bdgenomics.adam.rdd

import org.apache.spark.{ Logging, Partitioner, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegion, ReferenceMapping }

import scala.collection.mutable.ListBuffer
import scala.math._
import scala.reflect.ClassTag

object ShuffleRegionJoin {

  implicit val manualRegionOrdering = new Ordering[(ReferenceRegion, Int)] {
    override def compare(x: (ReferenceRegion, Int), y: (ReferenceRegion, Int)) = {
      if (x._1 != y._1) {
        x._1.compare(y._1)
      } else {
        x._2.compare(y._2)
      }
    }
  }

  /**
   * Performs a region join between two RDDs (shuffle join).
   *
   * This implementation is shuffle-based, so does not require collecting one side into memory
   * like BroadcastRegionJoin.  It basically performs a global sort of each RDD by genome position
   * and then does a sort-merge join, similar to the chromsweep implementatio in bedtools.  More
   * specifically, it first defines a set of bins across the genome, then assigns each object in the
   * RDDs to each bin that they overlap (replicating if necessary), performs the shuffle, and sorts
   * the object in each bin.  Finally, each bin independently performs a chromsweep sort-merge join.
   *
   * @param sc A SparkContext for the cluster that will perform the join
   * @param leftRDD The 'left' side of the join, a set of values which correspond (through an implicit
   *                ReferenceMapping) to regions on the genome.
   * @param rightRDD The 'right' side of the join, a set of values which correspond (through an implicit
   *                 ReferenceMapping) to regions on the genome
   * @param seqDict A SequenceDictionary -- every region corresponding to either the leftRDD or rightRDD
   *                values must be mapped to a chromosome with an entry in this dictionary.
   * @param partitionSize The size of the genome bin in nucleotides.  Controls the parallelism of the join.
   * @param tMapping implicit reference mapping for leftRDD regions
   * @param uMapping implicit reference mapping for rightRDD regions
   * @param tManifest implicit type of leftRDD
   * @param uManifest implicit type of rightRDD
   * @tparam T type of leftRDD
   * @tparam U type of rightRDD
   * @return An RDD of pairs (x, y), where x is from leftRDD, y is from rightRDD, and the region
   *         corresponding to x overlaps the region corresponding to y.
   */
  def partitionAndJoin[T, U](sc: SparkContext,
                             leftRDD: RDD[T],
                             rightRDD: RDD[U],
                             seqDict: SequenceDictionary,
                             partitionSize: Long)(implicit tMapping: ReferenceMapping[T],
                                                  uMapping: ReferenceMapping[U],
                                                  tManifest: ClassTag[T],
                                                  uManifest: ClassTag[U]): RDD[(T, U)] = {
    // Create the set of bins across the genome for parallel processing
    val seqLengths = Map(seqDict.records.toSeq.map(rec => (rec.name.toString, rec.length)): _*)
    val bins = sc.broadcast(GenomeBins(partitionSize, seqLengths))

    // Key each RDD element to its corresponding bin
    // Elements may be replicated if they overlap multiple bins
    val keyedLeft: RDD[((ReferenceRegion, Int), T)] =
      leftRDD.flatMap(x => {
        val region = tMapping.getReferenceRegion(x)
        val lo = bins.value.getStartBin(region)
        val hi = bins.value.getEndBin(region)
        (lo to hi).map(i => ((region, i), x))
      })
    val keyedRight: RDD[((ReferenceRegion, Int), U)] =
      rightRDD.flatMap(y => {
        val region = uMapping.getReferenceRegion(y)
        val lo = bins.value.getStartBin(region)
        val hi = bins.value.getEndBin(region)
        (lo to hi).map(i => ((region, i), y))
      })

    // Sort each RDD by shuffling the data into the corresponding genome bin
    // and then sorting within each bin by the key, which sorts by ReferenceRegion.
    // This should be the most expensive operation. At the end, each genome bin
    // corresponds to a Spark partition.  The ManualRegionPartitioner pulls out the
    // bin number for each elt.
    val sortedLeft: RDD[((ReferenceRegion, Int), T)] =
      keyedLeft.repartitionAndSortWithinPartitions(ManualRegionPartitioner(bins.value.numBins))
    val sortedRight: RDD[((ReferenceRegion, Int), U)] =
      keyedRight.repartitionAndSortWithinPartitions(ManualRegionPartitioner(bins.value.numBins))

    // this function carries out the sort-merge join inside each Spark partition.
    // It assumes the iterators are sorted.
    def sweep(leftIter: Iterator[((ReferenceRegion, Int), T)], rightIter: Iterator[((ReferenceRegion, Int), U)]) = {
      if (leftIter.isEmpty || rightIter.isEmpty) {
        Seq.empty[(T, U)].toIterator
      } else {
        val bufferedLeft = leftIter.buffered
        val currentBin = bufferedLeft.head._1._2
        val region = bins.value.invert(currentBin)
        // return an Iterator[(T, U)]
        SortedIntervalPartitionJoin(region, bufferedLeft, rightIter)
      }
    }

    // Execute the sort-merge join on each partition
    // Note that we do NOT preserve the partitioning, as the ManualRegionPartitioner
    // has no meaning for the return type of RDD[(T, U)].  In fact, how
    // do you order a pair of ReferenceRegions?
    sortedLeft.zipPartitions(sortedRight, preservesPartitioning = false)(sweep)
  }
}

/**
 * Partition a genome into a set of bins.
 *
 * Note that this class will not tolerate invalid input, so filter in advance if you use it.
 *
 * @param binSize The size of each bin in nucleotides
 * @param seqLengths A map containing the length of each contig
 */
case class GenomeBins(binSize: Long, seqLengths: Map[String, Long]) extends Serializable {
  private val names: Seq[String] = seqLengths.keys.toSeq.sortWith(_ < _)
  private val lengths: Seq[Long] = names.map(seqLengths(_))
  private val parts: Seq[Int] = lengths.map(v => round(ceil(v.toDouble / binSize)).toInt)
  private val cumulParts: Seq[Int] = parts.scan(0)(_ + _)
  private val contig2cumulParts: Map[String, Int] = Map(names.zip(cumulParts): _*)

  /**
   * The total number of bins induced by this partition.
   */
  def numBins: Int = parts.sum

  /**
   * Get the bin number corresponding to a query ReferenceRegion.
   *
   * @param region the query ReferenceRegion
   * @param useStart whether to use the start or end point of region to pick the bin
   */
  def get(region: ReferenceRegion, useStart: Boolean = true): Int = {
    val pos = if (useStart) region.start else (region.end - 1)
    (contig2cumulParts(region.referenceName) + pos / binSize).toInt
  }

  /**
   * Get the bin number corresponding to the start of the query ReferenceRegion.
   */
  def getStartBin(region: ReferenceRegion): Int = get(region, useStart = true)

  /**
   * Get the bin number corresponding to the end of the query ReferenceRegion.
   */
  def getEndBin(region: ReferenceRegion): Int = get(region, useStart = false)

  /**
   * Given a bin number, return its corresponding ReferenceRegion.
   */
  def invert(bin: Int): ReferenceRegion = {
    val idx = cumulParts.indexWhere(_ > bin) - 1
    val name = names(idx)
    val relPartition = bin - contig2cumulParts(name)
    val start = relPartition * binSize
    val end = min((relPartition + 1) * binSize, seqLengths(name))
    ReferenceRegion(name, start, end)
  }
}

/**
 * A Partitioner that simply passes through the precomputed partition number for the RegionJoin.
 *
 * This is a "hack" partitioner enables the replication of objects into different genome bins.
 * The key should correspond to a pair (region: ReferenceRegion, bin: Int).
 * The Spark partition number corresponds to the genome bin number, and was precomputed
 * with a flatmap to allow for replication into multiple bins.
 *
 * @param partitions should correspond to the number of bins in the corresponding GenomeBins
 */
private case class ManualRegionPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = key match {
    case (r: ReferenceRegion, p: Int) => p
    case _                            => throw new AssertionError("Unexpected key in ManualRegionPartitioner")
  }
}

/**
 * Implementation of a "chromosome sweep" sort-merge join.
 *
 * This implementation is based on the implementation used in bedtools:
 * https://github.com/arq5x/bedtools2
 *
 * Given two iterators of ReferenceRegions in sorted order, sweep across the relevant
 * region of the genome to emit pairs of overlapping regions.  Compared with the bedtools impl,
 * and to ensure no duplication of joined records, a joined pair is emitted only if at least
 * one of the two regions starts in the corresponding genome bin.  For this reason, we must
 * take in the partition's genome coords
 *
 * @param region The ReferenceRegion corresponding to this partition, to ensure no duplicate
 *               join results across the whole RDD
 * @param leftIter The left iterator
 * @param rightIter The right iterator
 * @tparam T type of leftIter
 * @tparam U type of rightIter
 */
private case class SortedIntervalPartitionJoin[T, U](region: ReferenceRegion, leftIter: Iterator[((ReferenceRegion, Int), T)], rightIter: Iterator[((ReferenceRegion, Int), U)])
    extends Iterator[(T, U)] with Serializable {
  // inspired by bedtools2 chromsweep
  private val left: BufferedIterator[((ReferenceRegion, Int), T)] = leftIter.buffered
  private val right: BufferedIterator[((ReferenceRegion, Int), U)] = rightIter.buffered
  private val binRegion: ReferenceRegion = region
  // stores the current set of joined pairs
  private var hits: List[(T, U)] = List.empty
  // stores the rightIter values that might overlap the current value from the leftIter
  private val cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty
  private var prevLeftRegion: ReferenceRegion = _

  private def advanceCache(until: Long): Unit = {
    while (right.hasNext && right.head._1._1.start < until) {
      val x = right.next()
      cache += x._1._1 -> x._2
    }
  }

  private def pruneCache(to: Long): Unit = {
    cache.dropWhile(_._1.end <= to)
  }

  private def getHits(): Unit = {
    // if there is nothing more in left, then I'm done
    while (left.hasNext && hits.isEmpty) {
      // there is more in left...
      val ((nextLeftRegion, _), nextLeft) = left.next
      // ...so check whether I need to advance the cache
      // (where nextLeftRegion's end is further than prevLeftRegion's end)...
      // (the null checks are for the first iteration)
      if (prevLeftRegion == null || nextLeftRegion.end > prevLeftRegion.end) {
        advanceCache(nextLeftRegion.end)
      }
      // ...and whether I need to prune the cache
      if (prevLeftRegion == null || nextLeftRegion.start > prevLeftRegion.start) {
        pruneCache(nextLeftRegion.start)
      }
      // at this point, we effectively do a cross-product and filter; this could probably
      // be improved by making cache a fancier data structure than just a list
      // we filter for things that overlap, where at least one side of the join has a start position
      // in this partition
      hits = cache
        .filter(y => {
          y._1.overlaps(nextLeftRegion) &&
            (y._1.start >= binRegion.start || nextLeftRegion.start >= binRegion.start)
        })
        .map(y => (nextLeft, y._2))
        .toList
      prevLeftRegion = nextLeftRegion
    }
  }

  def hasNext: Boolean = {
    // if the list of current hits is empty, try to refill it by moving forward
    if (hits.isEmpty) {
      getHits()
    }
    // if hits is still empty, I must really be at the end
    !hits.isEmpty
  }

  def next: (T, U) = {
    val popped = hits.head
    hits = hits.tail
    popped
  }
}
