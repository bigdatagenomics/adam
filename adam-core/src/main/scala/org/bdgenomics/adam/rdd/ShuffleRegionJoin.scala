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

import org.apache.spark.{ Partitioner, SparkContext }
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion._
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegion }
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * A trait describing join implementations that are based on a sort-merge join.
 *
 * @tparam T The type of the left RDD.
 * @tparam U The type of the right RDD.
 * @tparam RT The type of data yielded by the left RDD at the output of the
 *   join. This may not match T if the join is an outer join, etc.
 * @tparam RU The type of data yielded by the right RDD at the output of the
 *   join.
 */
sealed trait ShuffleRegionJoin[T, U, RT, RU] extends RegionJoin[T, U, RT, RU] {

  protected val sd: SequenceDictionary
  protected val partitionSize: Long
  protected val sc: SparkContext

  // Create the set of bins across the genome for parallel processing
  //   partitionSize (in nucleotides) may range from 10000 to 10000000+ 
  //   depending on cluster and dataset size
  protected val seqLengths = Map(sd.records.toSeq.map(rec => (rec.name, rec.length)): _*)
  protected val bins = sc.broadcast(GenomeBins(partitionSize, seqLengths))

  /**
   * Performs a region join between two RDDs (shuffle join).
   *
   * This implementation is shuffle-based, so does not require collecting one side into memory
   * like BroadcastRegionJoin.  It basically performs a global sort of each RDD by genome position
   * and then does a sort-merge join, similar to the chromsweep implementation in bedtools.  More
   * specifically, it first defines a set of bins across the genome, then assigns each object in the
   * RDDs to each bin that they overlap (replicating if necessary), performs the shuffle, and sorts
   * the object in each bin.  Finally, each bin independently performs a chromsweep sort-merge join.
   *
   * @param leftRDD The 'left' side of the join
   * @param rightRDD The 'right' side of the join
   * @param tManifest implicit type of leftRDD
   * @param uManifest implicit type of rightRDD
   * @tparam T type of leftRDD
   * @tparam U type of rightRDD
   * @return An RDD of pairs (x, y), where x is from leftRDD, y is from rightRDD, and the region
   *         corresponding to x overlaps the region corresponding to y.
   */
  def partitionAndJoin(
    leftRDD: RDD[(ReferenceRegion, T)],
    rightRDD: RDD[(ReferenceRegion, U)])(implicit tManifest: ClassTag[T],
                                         uManifest: ClassTag[U]): RDD[(RT, RU)] = {

    // Key each RDD element to its corresponding bin
    // Elements may be replicated if they overlap multiple bins
    val keyedLeft: RDD[((ReferenceRegion, Int), T)] =
      leftRDD.flatMap(kv => {
        val (region, x) = kv
        val lo = bins.value.getStartBin(region)
        val hi = bins.value.getEndBin(region)
        (lo to hi).map(i => ((region, i), x))
      })
    val keyedRight: RDD[((ReferenceRegion, Int), U)] =
      rightRDD.flatMap(kv => {
        val (region, y) = kv
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

    // Execute the sort-merge join on each partition
    // Note that we do NOT preserve the partitioning, as the ManualRegionPartitioner
    // has no meaning for the return type of RDD[(T, U)].  In fact, how
    // do you order a pair of ReferenceRegions?
    sortedLeft.zipPartitions(sortedRight, preservesPartitioning = false)(sweep)
  }

  protected def makeIterator(region: ReferenceRegion,
                             left: BufferedIterator[((ReferenceRegion, Int), T)],
                             right: BufferedIterator[((ReferenceRegion, Int), U)]): Iterator[(RT, RU)]

  // this function carries out the sort-merge join inside each Spark partition.
  // It assumes the iterators are sorted.
  def sweep(leftIter: Iterator[((ReferenceRegion, Int), T)],
            rightIter: Iterator[((ReferenceRegion, Int), U)]): Iterator[(RT, RU)] = {
    if (leftIter.isEmpty || rightIter.isEmpty) {
      emptyFn(leftIter, rightIter)
    } else {
      val bufferedLeft = leftIter.buffered
      val currentBin = bufferedLeft.head._1._2
      val region = bins.value.invert(currentBin)
      // return an Iterator[(T, U)]
      makeIterator(region, bufferedLeft, rightIter.buffered)
    }
  }

  protected def emptyFn(left: Iterator[((ReferenceRegion, Int), T)],
                        right: Iterator[((ReferenceRegion, Int), U)]): Iterator[(RT, RU)]
}

/**
 * Extends the ShuffleRegionJoin trait to implement an inner join.
 */
case class InnerShuffleRegionJoin[T, U](sd: SequenceDictionary,
                                        partitionSize: Long,
                                        @transient val sc: SparkContext) extends ShuffleRegionJoin[T, U, T, U] {

  protected def makeIterator(region: ReferenceRegion,
                             left: BufferedIterator[((ReferenceRegion, Int), T)],
                             right: BufferedIterator[((ReferenceRegion, Int), U)]): Iterator[(T, U)] = {
    InnerSortedIntervalPartitionJoin(region, left, right)
  }

  protected def emptyFn(left: Iterator[((ReferenceRegion, Int), T)],
                        right: Iterator[((ReferenceRegion, Int), U)]): Iterator[(T, U)] = {
    Iterator.empty
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement a left outer join.
 */
case class LeftOuterShuffleRegionJoin[T, U](sd: SequenceDictionary,
                                            partitionSize: Long,
                                            @transient val sc: SparkContext) extends ShuffleRegionJoin[T, U, T, Option[U]] {

  protected def makeIterator(region: ReferenceRegion,
                             left: BufferedIterator[((ReferenceRegion, Int), T)],
                             right: BufferedIterator[((ReferenceRegion, Int), U)]): Iterator[(T, Option[U])] = {
    LeftOuterSortedIntervalPartitionJoin(region, left, right)
  }

  protected def emptyFn(left: Iterator[((ReferenceRegion, Int), T)],
                        right: Iterator[((ReferenceRegion, Int), U)]): Iterator[(T, Option[U])] = {
    left.map(t => (t._2, None))
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement a right outer join.
 */
case class RightOuterShuffleRegionJoin[T, U](sd: SequenceDictionary,
                                             partitionSize: Long,
                                             @transient val sc: SparkContext) extends ShuffleRegionJoin[T, U, Option[T], U] {

  protected def makeIterator(region: ReferenceRegion,
                             left: BufferedIterator[((ReferenceRegion, Int), T)],
                             right: BufferedIterator[((ReferenceRegion, Int), U)]): Iterator[(Option[T], U)] = {
    LeftOuterSortedIntervalPartitionJoin(region, right, left).map(_.swap)
  }

  protected def emptyFn(left: Iterator[((ReferenceRegion, Int), T)],
                        right: Iterator[((ReferenceRegion, Int), U)]): Iterator[(Option[T], U)] = {
    right.map(u => (None, u._2))
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement a full outer join.
 */
case class FullOuterShuffleRegionJoin[T, U](sd: SequenceDictionary,
                                            partitionSize: Long,
                                            @transient val sc: SparkContext) extends ShuffleRegionJoin[T, U, Option[T], Option[U]] {

  protected def makeIterator(region: ReferenceRegion,
                             left: BufferedIterator[((ReferenceRegion, Int), T)],
                             right: BufferedIterator[((ReferenceRegion, Int), U)]): Iterator[(Option[T], Option[U])] = {
    FullOuterSortedIntervalPartitionJoin(region, left, right)
  }

  protected def emptyFn(left: Iterator[((ReferenceRegion, Int), T)],
                        right: Iterator[((ReferenceRegion, Int), U)]): Iterator[(Option[T], Option[U])] = {
    left.map(t => (Some(t._2), None)) ++ right.map(u => (None, Some(u._2)))
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement an inner join followed by
 * grouping by the left value.
 */
case class InnerShuffleRegionJoinAndGroupByLeft[T, U](sd: SequenceDictionary,
                                                      partitionSize: Long,
                                                      @transient val sc: SparkContext) extends ShuffleRegionJoin[T, U, T, Iterable[U]] {

  protected def makeIterator(region: ReferenceRegion,
                             left: BufferedIterator[((ReferenceRegion, Int), T)],
                             right: BufferedIterator[((ReferenceRegion, Int), U)]): Iterator[(T, Iterable[U])] = {
    SortedIntervalPartitionJoinAndGroupByLeft(region, left, right)
  }

  protected def emptyFn(left: Iterator[((ReferenceRegion, Int), T)],
                        right: Iterator[((ReferenceRegion, Int), U)]): Iterator[(T, Iterable[U])] = {
    Iterator.empty
  }
}

/**
 * Extends the ShuffleRegionJoin trait to implement a right outer join followed by
 * grouping by all non-null left values.
 */
case class RightOuterShuffleRegionJoinAndGroupByLeft[T, U](sd: SequenceDictionary,
                                                           partitionSize: Long,
                                                           @transient val sc: SparkContext) extends ShuffleRegionJoin[T, U, Option[T], Iterable[U]] {

  protected def makeIterator(region: ReferenceRegion,
                             left: BufferedIterator[((ReferenceRegion, Int), T)],
                             right: BufferedIterator[((ReferenceRegion, Int), U)]): Iterator[(Option[T], Iterable[U])] = {
    RightOuterSortedIntervalPartitionJoinAndGroupByLeft(region, left, right)
  }

  protected def emptyFn(left: Iterator[((ReferenceRegion, Int), T)],
                        right: Iterator[((ReferenceRegion, Int), U)]): Iterator[(Option[T], Iterable[U])] = {
    left.map(v => (Some(v._2), Iterable.empty)) ++ right.map(v => (None, Iterable(v._2)))
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
private[rdd] case class ManualRegionPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case (r: ReferenceRegion, p: Int) => p
    case _                            => throw new IllegalStateException("Unexpected key in ManualRegionPartitioner")
  }
}

private trait SortedIntervalPartitionJoin[T, U, RT, RU] extends Iterator[(RT, RU)] with Serializable {
  val binRegion: ReferenceRegion
  val left: BufferedIterator[((ReferenceRegion, Int), T)]
  val right: BufferedIterator[((ReferenceRegion, Int), U)]

  private var prevLeftRegion: ReferenceRegion = _

  // stores the current set of joined pairs
  protected var hits: Iterator[(RT, RU)] = Iterator.empty

  protected def advanceCache(until: Long)

  protected def pruneCache(to: Long)

  private def getHits(): Unit = {
    assert(!hits.hasNext)
    // if there is nothing more in left, then I'm done
    while (left.hasNext) {
      // there is more in left...
      val nl = left.next
      val ((nextLeftRegion, _), nextLeft) = nl
      // ...so check whether I need to advance the cache
      // (where nextLeftRegion's end is further than prevLeftRegion's end)...
      // (the null checks are for the first iteration)
      if (prevLeftRegion == null || nextLeftRegion.end > prevLeftRegion.end) {
        advanceCache(nextLeftRegion.end)
      }
      // ...and whether I need to prune the cache
      if (prevLeftRegion == null ||
        nextLeftRegion.start > prevLeftRegion.start) {
        pruneCache(nextLeftRegion.start)
      }
      // at this point, we effectively do a cross-product and filter; this could probably
      // be improved by making cache a fancier data structure than just a list
      // we filter for things that overlap, where at least one side of the join has a start position
      // in this partition
      //
      // also, see note "important: fun times with iterators" in this file, which explains
      // that these must apparently be two lines
      val newHits = processHits(nextLeft, nextLeftRegion)
      hits = hits ++ newHits

      assert(prevLeftRegion == null ||
        (prevLeftRegion.referenceName == nextLeftRegion.referenceName &&
          prevLeftRegion.start < nextLeftRegion.start ||
          (prevLeftRegion.start == nextLeftRegion.start &&
            prevLeftRegion.end <= nextLeftRegion.end)),
        "Left iterator in join violates sorted order invariant.")
      prevLeftRegion = nextLeftRegion
    }
  }

  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(RT, RU)]

  final def hasNext: Boolean = {
    // if the list of current hits is empty, try to refill it by moving forward
    if (hits.isEmpty) {
      getHits()
    }
    // if that fails, try advancing and pruning the cache
    if (hits.isEmpty) {
      advanceCache(binRegion.end)
      pruneCache(binRegion.end)
    }
    // if hits is still empty, I must really be at the end
    hits.hasNext
  }

  final def next: (RT, RU) = {
    hits.next
  }
}

private trait VictimlessSortedIntervalPartitionJoin[T, U, RU] extends SortedIntervalPartitionJoin[T, U, T, RU] with Serializable {

  // stores the rightIter values that might overlap the current value from the leftIter
  private var cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty

  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, RU)]

  protected def advanceCache(until: Long): Unit = {
    while (right.hasNext && right.head._1._1.start < until) {
      val x = right.next()
      cache += x._1._1 -> x._2
    }
  }

  protected def pruneCache(to: Long) {
    cache = cache.dropWhile(_._1.end <= to)
  }

  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(T, RU)] = {
    postProcessHits(cache
      .filter(y => {
        y._1.overlaps(currentLeftRegion) &&
          (y._1.start >= binRegion.start || currentLeftRegion.start >= binRegion.start)
      })
      .map(y => (currentLeft, y._2))
      .toIterator, currentLeft)
  }
}

private case class InnerSortedIntervalPartitionJoin[T, U](
    binRegion: ReferenceRegion,
    left: BufferedIterator[((ReferenceRegion, Int), T)],
    right: BufferedIterator[((ReferenceRegion, Int), U)]) extends VictimlessSortedIntervalPartitionJoin[T, U, U] {

  // no op!
  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, U)] = {
    iter
  }
}

private case class SortedIntervalPartitionJoinAndGroupByLeft[T, U](
    binRegion: ReferenceRegion,
    left: BufferedIterator[((ReferenceRegion, Int), T)],
    right: BufferedIterator[((ReferenceRegion, Int), U)]) extends VictimlessSortedIntervalPartitionJoin[T, U, Iterable[U]] {

  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, Iterable[U])] = {
    if (iter.hasNext) {
      Iterator((currentLeft, iter.map(_._2).toIterable))
    } else {
      Iterator.empty
    }
  }
}

private case class LeftOuterSortedIntervalPartitionJoin[T, U](
    binRegion: ReferenceRegion,
    left: BufferedIterator[((ReferenceRegion, Int), T)],
    right: BufferedIterator[((ReferenceRegion, Int), U)]) extends VictimlessSortedIntervalPartitionJoin[T, U, Option[U]] {

  // no op!
  protected def postProcessHits(iter: Iterator[(T, U)],
                                currentLeft: T): Iterator[(T, Option[U])] = {
    if (iter.hasNext) {
      iter.map(kv => (kv._1, Some(kv._2)))
    } else {
      Iterator((currentLeft, None))
    }
  }
}

private trait SortedIntervalPartitionJoinWithVictims[T, U, RT, RU] extends SortedIntervalPartitionJoin[T, U, RT, RU] with Serializable {

  // stores the rightIter values that might overlap the current value from the leftIter
  private var cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty
  private var victimCache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty

  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(RT, RU)]

  protected def processHits(currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterator[(RT, RU)] = {
    postProcessHits(cache
      .filter(y => {
        y._1.overlaps(currentLeftRegion) &&
          (y._1.start >= binRegion.start || currentLeftRegion.start >= binRegion.start)
      })
      .map(y => y._2)
      .toIterator, currentLeft)
  }

  protected def advanceCache(until: Long): Unit = {
    while (right.hasNext && right.head._1._1.start < until) {
      val x = right.next()
      victimCache += x._1._1 -> x._2
    }
  }

  protected def pruneCache(to: Long) {

    cache = cache.dropWhile(_._1.end <= to)
    cache = cache ++ victimCache.takeWhile(_._1.end > to)
    victimCache = victimCache.dropWhile(_._1.end > to)

    // important: fun times with iterators
    // 
    // for reasons known only to God, if you combine these two lines down to a
    // single line, it causes the hits iterator to be invalidated and become
    // empty.
    //
    // MORE: it seems like there's some funniness with the underlying scala imp'l
    // of append on two iterators. if the second line is written as:
    //
    // hits = hits ++ pped
    //
    // the line works as expected on scala 2.10. on scala 2.11, it occasionally
    // fails. oddly enough, if you write the line above and then do a duplicate
    // on the hits iterator (which you then reassign to hits), it works. i.e.,
    //
    // hits = hits ++ pped
    // val (d, _) = hits.duplicate
    // hits = d
    //
    // works on both scala 2.10 and 2.11 across all unit tests
    //
    // rewriting it as (pped ++ hits).toIterator seems to work all the time.
    // that appends the hits iterator to a ListBuffer, and then returns an iterator
    // over the list buffer. essentially, i think there's a bug in the Iterator.++
    // method in scala that occasionally causes it to return an empty iterator, but
    // i'm not sure why that is
    val pped = (victimCache.takeWhile(_._1.end <= to).map(u => postProcessPruned(u._2)))
    hits = (pped ++ hits).toIterator

    victimCache = victimCache.dropWhile(_._1.end <= to)
  }

  protected def postProcessPruned(pruned: U): (RT, RU)
}

private case class FullOuterSortedIntervalPartitionJoin[T, U](
    binRegion: ReferenceRegion,
    left: BufferedIterator[((ReferenceRegion, Int), T)],
    right: BufferedIterator[((ReferenceRegion, Int), U)]) extends SortedIntervalPartitionJoinWithVictims[T, U, Option[T], Option[U]] {

  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(Option[T], Option[U])] = {
    if (iter.hasNext) {
      iter.map(u => (Some(currentLeft), Some(u)))
    } else {
      Iterator((Some(currentLeft), None))
    }
  }

  protected def postProcessPruned(pruned: U): (Option[T], Option[U]) = {
    (None, Some(pruned))
  }
}

private case class RightOuterSortedIntervalPartitionJoinAndGroupByLeft[T, U](
    binRegion: ReferenceRegion,
    left: BufferedIterator[((ReferenceRegion, Int), T)],
    right: BufferedIterator[((ReferenceRegion, Int), U)]) extends SortedIntervalPartitionJoinWithVictims[T, U, Option[T], Iterable[U]] {

  protected def postProcessHits(iter: Iterator[U],
                                currentLeft: T): Iterator[(Option[T], Iterable[U])] = {
    Iterator((Some(currentLeft), iter.toIterable))
  }

  protected def postProcessPruned(pruned: U): (Option[T], Iterable[U]) = {
    (None, Iterable(pruned))
  }
}
