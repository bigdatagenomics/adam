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
package org.bdgenomics.adam.rdd.settheory

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ManualRegionPartitioner
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.reflect.ClassTag

/**
 * A trait describing join implementations that are based on a sort-merge join.
 *
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 * @tparam RT The resulting type of the left after the join.
 * @tparam RU The resulting type of the right after the join.
 */
sealed abstract class ShuffleRegionJoin[T: ClassTag, U: ClassTag, RT, RU]
    extends SetTheoryBetweenCollections[T, U, RT, RU] with SetTheoryPrimitive {

  override protected def condition(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   distanceThreshold: Long = 0L): Boolean = {

    firstRegion.isNearby(secondRegion,
      distanceThreshold,
      requireStranded = false)
  }

  override protected def pruneCacheCondition(cachedRegion: ReferenceRegion,
                                             to: ReferenceRegion): Boolean = {

    cachedRegion.compareTo(to) < 0 && !cachedRegion.covers(to)
  }

  override protected def advanceCacheCondition(candidateRegion: ReferenceRegion,
                                               until: ReferenceRegion): Boolean = {

    candidateRegion.compareTo(until) < 0 || candidateRegion.covers(until)
  }

  override protected def prepare(): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, U)]) = {
    
    val (preparedLeft, destinationPartitionMap) = {
      if(optPartitionMap.isDefined) {
        (leftRdd, optPartitionMap.get.map(_.get))
      } else {
        val sortedLeft = leftRdd.sortByKey()
        val partitionMap =
          sortedLeft.mapPartitions(getRegionBoundsFromPartition).collect
        (sortedLeft, partitionMap.map(_.get))
      }
    }

    val adjustedPartitionMapWithIndex =
    // the zipWithIndex gives us the destination partition ID
      destinationPartitionMap.zipWithIndex.map(g => {
        val (firstRegion, secondRegion, index) = (g._1._1, g._1._2, g._2)
        // in the case where we span multiple referenceNames using
        // IntervalArray.get with requireOverlap set to false will assign all
        // the remaining regions to this partition, in addition to all the
        // regions up to the start of the next partition.
        if (firstRegion.referenceName != secondRegion.referenceName) {

          // the first region is enough to represent the partition for
          // IntervalArray.get.
          (firstRegion, index)
        } else {
          // otherwise we just have the ReferenceRegion span from partition
          // lower bound to upper bound.
          // We cannot use the firstRegion bounds here because we may end up
          // dropping data if it doesn't map anywhere.
          (ReferenceRegion(
            firstRegion.referenceName,
            firstRegion.start,
            secondRegion.end),
            index)
        }
      })

    // convert to an IntervalArray for fast range query
    val partitionMapIntervals = IntervalArray(
      adjustedPartitionMapWithIndex,
      adjustedPartitionMapWithIndex.maxBy(_._1.width)._1.width,
      sorted = true)

    val preparedRight = {
      rightRdd.mapPartitions(iter => {
        iter.flatMap(f => {
          val intervals = partitionMapIntervals.get(f._1.pad(threshold), requireOverlap = false)
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

/**
 * Perform an Inner Shuffle Region Join.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param optPartitionMap An optional partition map defining the left RDD
 *   partition bounds.
 * @param threshold The threshold for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class InnerShuffleRegionJoin[T: ClassTag, U: ClassTag](
  leftRdd: RDD[(ReferenceRegion, T)],
  rightRdd: RDD[(ReferenceRegion, U)],
  optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None,
  threshold: Long = 0L)
    extends ShuffleRegionJoin[T, U, T, U]
    with VictimlessSetTheoryBetweenCollections[T, U, T, U] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, U)]): Iterator[(T, U)] = {
    Iterator.empty
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, U)]): Iterable[(T, U)] = {
    iter.map(f => (currentLeft._2, f._2))
  }
}

/**
 * Perform an Inner Shuffle Region Join and Group By left records.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param optPartitionMap An optional partition map defining the left RDD
 *   partition bounds.
 * @param threshold The threshold for the join.
 * @tparam T The type of the left records.
 * @tparam U THe type of the right records.
 */
case class InnerShuffleRegionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](
  leftRdd: RDD[(ReferenceRegion, T)],
  rightRdd: RDD[(ReferenceRegion, U)],
  optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None,
  threshold: Long = 0L)
    extends ShuffleRegionJoin[T, U, T, Iterable[U]]
    with VictimlessSetTheoryBetweenCollections[T, U, T, Iterable[U]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, U)]): Iterator[(T, Iterable[U])] = {
    Iterator.empty
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, U)]): Iterable[(T, Iterable[U])] = {

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
 * @param optPartitionMap An optional partition map defining the left RDD
 *   partition bounds.
 * @param threshold The threshold for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class LeftOuterShuffleRegionJoin[T: ClassTag, U: ClassTag](
  leftRdd: RDD[(ReferenceRegion, T)],
  rightRdd: RDD[(ReferenceRegion, U)],
  optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None,
  threshold: Long = 0L)
    extends ShuffleRegionJoin[T, U, T, Option[U]]
    with VictimlessSetTheoryBetweenCollections[T, U, T, Option[U]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, U)]): Iterator[(T, Option[U])] = {
    left.map(t => (t._2, None))
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, U)]): Iterable[(T, Option[U])] = {
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
 * @param optPartitionMap An optional partition map defining the left RDD
 *   partition bounds.
 * @param threshold The threshold for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class RightOuterShuffleRegionJoin[T: ClassTag, U: ClassTag](
  leftRdd: RDD[(ReferenceRegion, T)],
  rightRdd: RDD[(ReferenceRegion, U)],
  optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None,
  threshold: Long = 0L)
    extends ShuffleRegionJoin[T, U, Option[T], U]
    with SetTheoryBetweenCollectionsWithVictims[T, U, Option[T], U] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, U)]): Iterator[(Option[T], U)] = {
    right.map(u => (None, u._2))
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, U)]): Iterable[(Option[T], U)] = {
    if (iter.nonEmpty) {
      // group all hits for currentLeft into an iterable
      iter.map(f => (Some(currentLeft._2), f._2))
    } else {
      Iterable.empty
    }
  }

  override protected def postProcessPruned(pruned: U): (Option[T], U) = {
    (None, pruned)
  }
}

/**
 * Perform a Full Outer Shuffle Region Join.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param optPartitionMap An optional partition map defining the left RDD
 *   partition bounds.
 * @param threshold The threshold for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class FullOuterShuffleRegionJoin[T: ClassTag, U: ClassTag](
  leftRdd: RDD[(ReferenceRegion, T)],
  rightRdd: RDD[(ReferenceRegion, U)],
  optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None,
  threshold: Long = 0L)
    extends ShuffleRegionJoin[T, U, Option[T], Option[U]]
    with SetTheoryBetweenCollectionsWithVictims[T, U, Option[T], Option[U]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, U)]): Iterator[(Option[T], Option[U])] = {
    left.map(t => (Some(t._2), None)) ++ right.map(u => (None, Some(u._2)))
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, U)]): Iterable[(Option[T], Option[U])] = {
    if (iter.nonEmpty) {
      // formatting these as options for the full outer join
      iter.map(u => (Some(currentLeft._2), Some(u._2)))
    } else {
      // no hits for the currentLeft
      Iterable((Some(currentLeft._2), None))
    }
  }

  override protected def postProcessPruned(pruned: U): (Option[T], Option[U]) = {
    (None, Some(pruned))
  }
}

/**
 * Perform a Right Outer Shuffle Region Join and group by left values.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param optPartitionMap An optional partition map defining the left RDD
 *   partition bounds.
 * @param threshold The threshold for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class RightOuterShuffleRegionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](
  leftRdd: RDD[(ReferenceRegion, T)],
  rightRdd: RDD[(ReferenceRegion, U)],
  optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None,
  threshold: Long = 0L)
    extends ShuffleRegionJoin[T, U, Option[T], Iterable[U]]
    with SetTheoryBetweenCollectionsWithVictims[T, U, Option[T], Iterable[U]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, U)]): Iterator[(Option[T], Iterable[U])] = {

    left.map(v => (Some(v._2), Iterable.empty)) ++
      right.map(v => (None, Iterable(v._2)))
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, U)]): Iterable[(Option[T], Iterable[U])] = {
    Iterable((Some(currentLeft._2), iter.map(_._2)))
  }

  override protected def postProcessPruned(pruned: U): (Option[T], Iterable[U]) = {
    (None, Iterable(pruned))
  }
}
