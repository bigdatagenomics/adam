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
package org.bdgenomics.adam.rdd.sets

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.{ PartitionMap, GenomicRDD, ManualRegionPartitioner }
import scala.reflect.ClassTag

/**
 * A trait describing join implementations that are based on a sort-merge join.
 *
 * @tparam T The type of the left records.
 * @tparam X The type of the right records.
 * @tparam RT The resulting type of the left after the join.
 * @tparam RX The resulting type of the right after the join.
 */
sealed trait ShuffleRegionJoin[T, X, RT, RX]
    extends SetOperationBetweenCollections[T, X, RT, RX] {

  override protected def condition(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   cache: SetTheoryCache[X, RT, RX],
                                   distanceThreshold: Long = 0L): Boolean = {

    firstRegion.isNearby(secondRegion,
      distanceThreshold,
      requireStranded = false)
  }

  override protected def pruneCacheCondition(cachedRegion: ReferenceRegion,
                                             to: ReferenceRegion,
                                             cache: SetTheoryCache[X, RT, RX]): Boolean = {

    cachedRegion.compareTo(to) < 0 && !cachedRegion.covers(to)
  }

  override protected def advanceCacheCondition(candidateRegion: ReferenceRegion,
                                               until: ReferenceRegion,
                                               cache: SetTheoryCache[X, RT, RX]): Boolean = {

    candidateRegion.compareTo(until) < 0 || candidateRegion.covers(until)
  }

  protected def prepareRdds(
    leftRdd: RDD[(ReferenceRegion, T)],
    rightRdd: RDD[(ReferenceRegion, X)],
    partitionMap: PartitionMap)(implicit tTag: ClassTag[T], xtag: ClassTag[X]): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, X)]) = {

    val numPartitions = optPartitions.getOrElse(leftRdd.partitions.length)

    val (preparedLeft, destinationPartitionMap) = {
      if (!partitionMap.isEmpty &&
        numPartitions != leftRdd.partitions.length) {

        (leftRdd, partitionMap)
      } else {
        val sortedLeft = leftRdd.sortByKey(ascending = true, numPartitions = numPartitions)
        (sortedLeft, PartitionMap(sortedLeft))
      }
    }

    // convert to an IntervalArray for fast range query
    val partitionMapIntervals = destinationPartitionMap.toIntervalArray()

    val preparedRight = {
      rightRdd
        .mapPartitions(iter => {
          iter.flatMap(f => {
            // we pad by the threshold here to ensure that our invariant is met
            val intervals = partitionMapIntervals.get(f._1.pad(threshold), requireOverlap = false)
            // for each index identified in intervals, create a record
            intervals.map(g => ((f._1, g._2), f._2))
          })
        }, preservesPartitioning = true)
        .repartitionAndSortWithinPartitions(
          ManualRegionPartitioner(destinationPartitionMap.length))
        .map(f => (f._1._1, f._2))
    }

    (preparedLeft, preparedRight)
  }
}

private[rdd] sealed trait ShuffleRegionJoinOnGenomicRDD[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y], RT, RX]
    extends ShuffleRegionJoin[T, X, RT, RX] {

  protected val leftRdd: GenomicRDD[T, U]
  protected val rightRdd: GenomicRDD[X, Y]

  override protected def prepare()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, X)]) = {
    prepareRdds(
      leftRdd.flattenRddByRegions(),
      rightRdd.flattenRddByRegions(),
      leftRdd.partitionMap)
  }
}

private[rdd] sealed trait ShuffleRegionJoinOnReferenceRegionKeyedRDD[T, X, RT, RX]
    extends ShuffleRegionJoin[T, X, RT, RX] {

  protected val leftRdd: RDD[(ReferenceRegion, T)]
  protected val rightRdd: RDD[(ReferenceRegion, X)]

  override protected def prepare()(implicit tTag: ClassTag[T], xTag: ClassTag[X]): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, X)]) = {
    prepareRdds(leftRdd, rightRdd, PartitionMap(None))
  }
}

sealed trait InnerShuffleRegionJoin[T, X] extends VictimlessSetOperationBetweenCollections[T, X, T, X] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, X)]): Iterator[(T, X)] = {
    Iterator.empty
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, X)]): Iterable[(T, X)] = {
    iter.map(f => (currentLeft._2, f._2))
  }
}

object InnerShuffleRegionJoin {
  def apply[T, X](
    leftRdd: RDD[(ReferenceRegion, T)],
    rightRdd: RDD[(ReferenceRegion, X)]): InnerShuffleRegionJoin[T, X] = {

    InnerShuffleRegionJoinOnReferenceRegionKeyedRDD(leftRdd, rightRdd)
  }
}

private[rdd] case class InnerShuffleRegionJoinOnReferenceRegionKeyedRDD[T, X](
  protected val leftRdd: RDD[(ReferenceRegion, T)],
  protected val rightRdd: RDD[(ReferenceRegion, X)],
  protected val threshold: Long = 0L,
  protected val optPartitions: Option[Int] = None)
    extends InnerShuffleRegionJoin[T, X]
    with ShuffleRegionJoinOnReferenceRegionKeyedRDD[T, X, T, X]

/**
 * Perform an Inner Shuffle Region Join.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param threshold The threshold for the join.
 * @param optPartitions Optionally sets the number of partitions for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
private[rdd] case class InnerShuffleRegionJoinOnGenomicRDD[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](
  protected val leftRdd: GenomicRDD[T, U],
  protected val rightRdd: GenomicRDD[X, Y],
  protected val threshold: Long = 0L,
  protected val optPartitions: Option[Int] = None)
    extends ShuffleRegionJoinOnGenomicRDD[T, U, X, Y, T, X]
    with InnerShuffleRegionJoin[T, X]

/**
 * Perform an Inner Shuffle Region Join and Group By left records.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param threshold The threshold for the join.
 * @param optPartitions Optionally sets the number of partitions for the join.
 * @tparam T The type of the left records.
 * @tparam U THe type of the right records.
 */
case class InnerShuffleRegionJoinAndGroupByLeft[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](
  protected val leftRdd: GenomicRDD[T, U],
  protected val rightRdd: GenomicRDD[X, Y],
  protected val threshold: Long = 0L,
  protected val optPartitions: Option[Int] = None)
    extends ShuffleRegionJoinOnGenomicRDD[T, U, X, Y, T, Iterable[X]]
    with VictimlessSetOperationBetweenCollections[T, X, T, Iterable[X]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, X)]): Iterator[(T, Iterable[X])] = {
    Iterator.empty
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, X)]): Iterable[(T, Iterable[X])] = {

    if (iter.nonEmpty) {
      // group all hits for currentLeft into an iterable
      Iterable((currentLeft._2, iter.map(_._2)))
    } else {
      Iterable.empty
    }
  }
}

/**
 * Perform a Left Outer Shuffle Region Join.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param threshold The threshold for the join.
 * @param optPartitions Optionally sets the number of partitions for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class LeftOuterShuffleRegionJoin[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](
  protected val leftRdd: GenomicRDD[T, U],
  protected val rightRdd: GenomicRDD[X, Y],
  protected val threshold: Long = 0L,
  protected val optPartitions: Option[Int] = None)
    extends ShuffleRegionJoinOnGenomicRDD[T, U, X, Y, T, Option[X]]
    with VictimlessSetOperationBetweenCollections[T, X, T, Option[X]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, X)]): Iterator[(T, Option[X])] = {
    left.map(t => (t._2, None))
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, X)]): Iterable[(T, Option[X])] = {
    if (iter.nonEmpty) {
      // left has some hits
      iter.map(f => (currentLeft._2, Some(f._2)))
    } else {
      // left has no hits
      Iterable((currentLeft._2, None))
    }
  }
}

/**
 * Perform a Right Outer Shuffle Region Join.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param threshold The threshold for the join.
 * @param optPartitions Optionally sets the number of partitions for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class RightOuterShuffleRegionJoin[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](
  protected val leftRdd: GenomicRDD[T, U],
  protected val rightRdd: GenomicRDD[X, Y],
  protected val threshold: Long = 0L,
  protected val optPartitions: Option[Int] = None)
    extends ShuffleRegionJoinOnGenomicRDD[T, U, X, Y, Option[T], X]
    with SetOperationBetweenCollectionsWithVictims[T, X, Option[T], X] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, X)]): Iterator[(Option[T], X)] = {
    right.map(u => (None, u._2))
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, X)]): Iterable[(Option[T], X)] = {
    if (iter.nonEmpty) {
      // group all hits for currentLeft into an iterable
      iter.map(f => (Some(currentLeft._2), f._2))
    } else {
      Iterable.empty
    }
  }

  override protected def postProcessPruned(pruned: X): (Option[T], X) = {
    (None, pruned)
  }
}

/**
 * Perform a Full Outer Shuffle Region Join.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param threshold The threshold for the join.
 * @param optPartitions Optionally sets the number of partitions for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class FullOuterShuffleRegionJoin[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](
  protected val leftRdd: GenomicRDD[T, U],
  protected val rightRdd: GenomicRDD[X, Y],
  protected val threshold: Long = 0L,
  protected val optPartitions: Option[Int] = None)
    extends ShuffleRegionJoinOnGenomicRDD[T, U, X, Y, Option[T], Option[X]]
    with SetOperationBetweenCollectionsWithVictims[T, X, Option[T], Option[X]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, X)]): Iterator[(Option[T], Option[X])] = {
    left.map(t => (Some(t._2), None)) ++ right.map(u => (None, Some(u._2)))
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, X)]): Iterable[(Option[T], Option[X])] = {
    if (iter.nonEmpty) {
      // formatting these as options for the full outer join
      iter.map(u => (Some(currentLeft._2), Some(u._2)))
    } else {
      // no hits for the currentLeft
      Iterable((Some(currentLeft._2), None))
    }
  }

  override protected def postProcessPruned(pruned: X): (Option[T], Option[X]) = {
    (None, Some(pruned))
  }
}

/**
 * Perform a Right Outer Shuffle Region Join and group by left values.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param threshold The threshold for the join.
 * @param optPartitions Optionally sets the number of partitions for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class RightOuterShuffleRegionJoinAndGroupByLeft[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](
  protected val leftRdd: GenomicRDD[T, U],
  protected val rightRdd: GenomicRDD[X, Y],
  protected val threshold: Long = 0L,
  protected val optPartitions: Option[Int] = None)
    extends ShuffleRegionJoinOnGenomicRDD[T, U, X, Y, Option[T], Iterable[X]]
    with SetOperationBetweenCollectionsWithVictims[T, X, Option[T], Iterable[X]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, X)]): Iterator[(Option[T], Iterable[X])] = {

    left.map(v => (Some(v._2), Iterable.empty)) ++
      right.map(v => (None, Iterable(v._2)))
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, X)]): Iterable[(Option[T], Iterable[X])] = {
    Iterable((Some(currentLeft._2), iter.map(_._2)))
  }

  override protected def postProcessPruned(pruned: X): (Option[T], Iterable[X]) = {
    (None, Iterable(pruned))
  }
}
