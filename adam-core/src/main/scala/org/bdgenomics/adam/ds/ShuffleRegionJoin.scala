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
package org.bdgenomics.adam.ds

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
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
sealed abstract class ShuffleRegionJoin[T: ClassTag, U: ClassTag, RT, RU]
    extends RegionJoin[T, U, RT, RU] {

  protected def advanceCache(cache: SetTheoryCache[U, RT, RU],
                             right: BufferedIterator[(ReferenceRegion, U)],
                             until: ReferenceRegion)
  protected def pruneCache(cache: SetTheoryCache[U, RT, RU],
                           to: ReferenceRegion)
  protected def postProcessHits(iter: Iterable[U],
                                currentLeft: T): Iterable[(RT, RU)]
  protected def finalizeHits(cache: SetTheoryCache[U, RT, RU],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterable[(RT, RU)]
  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(RT, RU)]

  protected val leftRdd: RDD[(ReferenceRegion, T)]
  protected val rightRdd: RDD[(ReferenceRegion, U)]

  /**
   * Performs a region join between two RDDs (shuffle join). All data should be pre-shuffled and
   * copartitioned.
   *
   * @return An RDD of joins (x, y), where x is from leftRDD, y is from rightRDD, and the region
   *         corresponding to x overlaps the region corresponding to y.
   */
  def compute(): RDD[(RT, RU)] = {
    leftRdd.zipPartitions(rightRdd)(makeIterator)
  }

  def partitionAndJoin(left: RDD[(ReferenceRegion, T)], right: RDD[(ReferenceRegion, U)]): RDD[(RT, RU)] = {
    left.zipPartitions(right)(makeIterator)
  }

  protected def makeIterator(leftIter: Iterator[(ReferenceRegion, T)],
                             rightIter: Iterator[(ReferenceRegion, U)]): Iterator[(RT, RU)] = {

    if (leftIter.isEmpty || rightIter.isEmpty) {
      emptyFn(leftIter, rightIter)
    } else {
      val bufferedLeft = leftIter.buffered
      val bufferedRight = rightIter.buffered

      val cache = new SetTheoryCache[U, RT, RU]

      bufferedLeft.flatMap(f => {
        val currentLeftRegion = f._1

        advanceCache(cache, bufferedRight, currentLeftRegion)
        pruneCache(cache, currentLeftRegion)

        processHits(cache, f._2, f._1)
      }) ++ finalizeHits(cache, bufferedRight)
    }
  }

  /**
   * Process hits for a given object in left.
   *
   * @param cache The cache containing potential hits.
   * @param currentLeft The current object from the left
   * @param currentLeftRegion The ReferenceRegion of currentLeft.
   * @return An iterator containing all hits, formatted by postProcessHits.
   */
  protected def processHits(cache: SetTheoryCache[U, RT, RU],
                            currentLeft: T,
                            currentLeftRegion: ReferenceRegion): Iterable[(RT, RU)] = {
    // post processing formats the hits for each individual type of join
    postProcessHits(cache.cache
      .filter(y => {
        // everything that overlaps the left region is a hit
        y._1.overlaps(currentLeftRegion)
      })
      .map(y => y._2), currentLeft)
  }
}

case class InnerShuffleRegionJoin[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                            rightRdd: RDD[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, T, U] {

  /**
   * Handles the case where either the left or the right iterator were empty.
   * In the case of inner join, we return an empty iterator.
   *
   * @param left The left iterator.
   * @param right The right iterator.
   * @return An empty iterator.
   */
  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(T, U)] = {
    Iterator.empty
  }

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @param iter The iterable of hits.
   * @param currentLeft The current value from the left iterator.
   * @return The post processed iterable.
   */
  protected def postProcessHits(iter: Iterable[U],
                                currentLeft: T): Iterable[(T, U)] = {
    // no post-processing required
    iter.map(f => (currentLeft, f))
  }
}

case class InnerShuffleRegionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                                          rightRdd: RDD[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, T, Iterable[U]] {

  /**
   * Handles the case where either the left or the right iterator were empty.
   * In the case of inner join, we return an empty iterator.
   *
   * @param left The left iterator.
   * @param right The right iterator.
   * @return An empty iterator.
   */
  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(T, Iterable[U])] = {
    Iterator.empty
  }

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @param iter The iterator containing all hits.
   * @param currentLeft The current left value.
   * @return The post processed iterator.
   */
  protected def postProcessHits(iter: Iterable[U],
                                currentLeft: T): Iterable[(T, Iterable[U])] = {
    if (iter.nonEmpty) {
      // group all hits for currentLeft into an iterable
      Iterable((currentLeft, iter.toIterable))
    } else {
      Iterable.empty
    }
  }
}

case class LeftOuterShuffleRegionJoin[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                                rightRdd: RDD[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, T, Option[U]] {

  /**
   * Handles the case where the left or the right iterator were empty.
   *
   * @param left The left iterator.
   * @param right The right iterator.
   * @return The iterator containing properly formatted tuples.
   */
  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(T, Option[U])] = {
    left.map(t => (t._2, None))
  }

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @param iter The iterator of hits.
   * @param currentLeft The current left value.
   * @return the post processed iterator.
   */
  protected def postProcessHits(iter: Iterable[U],
                                currentLeft: T): Iterable[(T, Option[U])] = {
    if (iter.nonEmpty) {
      // left has some hits
      iter.map(f => (currentLeft, Some(f)))
    } else {
      // left has no hits
      Iterable((currentLeft, None))
    }
  }
}

case class LeftOuterShuffleRegionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                                              rightRdd: RDD[(ReferenceRegion, U)])
    extends VictimlessSortedIntervalPartitionJoin[T, U, T, Iterable[U]] {

  /**
   * Handles the case where the left or the right iterator were empty.
   *
   * @param left The left iterator.
   * @param right The right iterator.
   * @return The iterator containing properly formatted tuples.
   */
  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(T, Iterable[U])] = {
    left.map(t => (t._2, Iterable.empty[U]))
  }

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @param iter The iterator of hits.
   * @param currentLeft The current left value.
   * @return the post processed iterator.
   */
  protected def postProcessHits(iter: Iterable[U],
                                currentLeft: T): Iterable[(T, Iterable[U])] = {
    if (iter.nonEmpty) {
      // left has some hits
      Iterable((currentLeft, iter))
    } else {
      // left has no hits
      Iterable((currentLeft, Iterable.empty[U]))
    }
  }
}

case class FullOuterShuffleRegionJoin[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                                rightRdd: RDD[(ReferenceRegion, U)])
    extends SortedIntervalPartitionJoinWithVictims[T, U, Option[T], Option[U]] {

  /**
   * Handles the case where the left or the right iterator were empty.
   *
   * @param left The left iterator.
   * @param right The right iterator.
   * @return The iterator containing properly formatted tuples.
   */
  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(Option[T], Option[U])] = {
    left.map(t => (Some(t._2), None)) ++ right.map(u => (None, Some(u._2)))
  }

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @param iter The iterator of hits.
   * @param currentLeft The current left value for the join.
   * @return the post processed iterator.
   */
  protected def postProcessHits(iter: Iterable[U],
                                currentLeft: T): Iterable[(Option[T], Option[U])] = {
    if (iter.nonEmpty) {
      // formatting these as options for the full outer join
      iter.map(u => (Some(currentLeft), Some(u)))
    } else {
      // no hits for the currentLeft
      Iterable((Some(currentLeft), None))
    }
  }

  /**
   * Properly formats right values that did not join with a left.
   *
   * @param pruned The right value with no join.
   * @return The formatted tuple containing the right value.
   */
  protected def postProcessPruned(pruned: U): (Option[T], Option[U]) = {
    (None, Some(pruned))
  }
}

case class RightOuterShuffleRegionJoinAndGroupByLeft[T: ClassTag, U: ClassTag](leftRdd: RDD[(ReferenceRegion, T)],
                                                                               rightRdd: RDD[(ReferenceRegion, U)])
    extends SortedIntervalPartitionJoinWithVictims[T, U, Option[T], Iterable[U]] {

  /**
   * Handles the case where one of the iterators contains no data.
   *
   * @param left The left iterator.
   * @param right The right iterator.
   * @return The iterator containing properly formatted tuples.
   */
  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, U)]): Iterator[(Option[T], Iterable[U])] = {

    left.map(v => (Some(v._2), Iterable.empty)) ++
      right.map(v => (None, Iterable(v._2)))
  }

  /**
   * Computes post processing required to complete the join and properly format
   * hits.
   *
   * @param iter The iterator of all hits
   * @param currentLeft The current left value.
   * @return The post processed iterator.
   */
  protected def postProcessHits(iter: Iterable[U],
                                currentLeft: T): Iterable[(Option[T], Iterable[U])] = {
    Iterable((Some(currentLeft), iter.toIterable))
  }

  /**
   * Properly formats right values that did not join with a region on the left.
   *
   * @param pruned The right value that did not join.
   * @return A tuple with the postProcessed right value.
   */
  protected def postProcessPruned(pruned: U): (Option[T], Iterable[U]) = {
    (None, Iterable(pruned))
  }
}

sealed trait VictimlessSortedIntervalPartitionJoin[T, U, RT, RU]
    extends ShuffleRegionJoin[T, U, RT, RU] {

  /**
   * Adds elements from right to cache based on the next region encountered.
   *
   * @param cache The cache for this partition.
   * @param right The right iterator.
   * @param until The next region to join with.
   */
  protected def advanceCache(cache: SetTheoryCache[U, RT, RU],
                             right: BufferedIterator[(ReferenceRegion, U)],
                             until: ReferenceRegion) = {
    while (right.hasNext &&
      (right.head._1.compareTo(until) <= 0 || right.head._1.covers(until))) {

      val x = right.next()
      cache.cache += ((x._1, x._2))
    }
  }

  /**
   * Removes elements from cache in place that do not meet the condition for
   * the next region.
   *
   * @note At one point these were all variables and we built new collections
   * and reassigned the pointers every time. We fixed this by using trimStart()
   * and ++=() to improve performance. Overall, we see roughly 25% improvement
   * in runtime by doing things this way.
   *
   * @param cache The cache for this partition.
   * @param to The next region in the left iterator.
   */
  protected def pruneCache(cache: SetTheoryCache[U, RT, RU],
                           to: ReferenceRegion) {
    cache.cache.trimStart({
      val trimLocation =
        cache
          .cache
          .indexWhere(f => !(f._1.compareTo(to) < 0 && !f._1.covers(to)))

      if (trimLocation < 0) {
        0
      } else {
        trimLocation
      }
    })
  }

  /**
   * Computes all victims for the partition. NOTE: These are victimless joins
   * so we have no victims.
   *
   * @param cache The cache for this partition.
   * @param right The right iterator.
   * @return An empty iterator.
   */
  protected def finalizeHits(cache: SetTheoryCache[U, RT, RU],
                             right: BufferedIterator[(ReferenceRegion, U)]): Iterable[(RT, RU)] = {
    Iterable.empty
  }
}

sealed trait SortedIntervalPartitionJoinWithVictims[T, U, RT, RU]
    extends ShuffleRegionJoin[T, U, RT, RU] {

  protected def postProcessPruned(pruned: U): (RT, RU)

  /**
   * Adds elements from right to victimCache based on the next region
   * encountered.
   *
   * @param cache The cache for this partition.
   * @param right The right iterator.
   * @param until The next value on the left to perform the join.
   */
  protected def advanceCache(cache: SetTheoryCache[U, RT, RU],
                             right: BufferedIterator[(ReferenceRegion, U)],
                             until: ReferenceRegion) = {
    while (right.hasNext &&
      (right.head._1.compareTo(until) <= 0 || right.head._1.covers(until))) {

      val x = right.next()
      cache.victimCache += ((x._1, x._2))
    }
  }

  /**
   * Removes elements from cache in place that do not meet the condition for
   * join. Also adds the elements that are not hits to the list of pruned.
   *
   * @param cache The cache for this partition.
   * @param to The next region in the left iterator.
   */
  protected def pruneCache(cache: SetTheoryCache[U, RT, RU],
                           to: ReferenceRegion) {
    // remove everything from cache that will never again be joined
    cache.cache.trimStart({
      val trimLocation =
        cache.cache
          .indexWhere(f => !(f._1.compareTo(to) < 0 && !f._1.covers(to)))

      if (trimLocation < 0) {
        0
      } else {
        trimLocation
      }
    })

    // add the values from the victimCache that are candidates to be joined
    // the the current left
    val cacheAddition =
      cache
        .victimCache
        .takeWhile(f => f._1.compareTo(to) > 0 || f._1.covers(to))

    cache.cache ++= cacheAddition
    // remove the values from the victimCache that were just added to cache
    cache.victimCache.trimStart(cacheAddition.size)

    // add to pruned any values that do not have any matches to a left
    // and perform post processing to format the new pruned values
    val prunedAddition =
      cache
        .victimCache
        .takeWhile(f => f._1.compareTo(to) <= 0)
    cache.pruned ++= prunedAddition
      .map(u => postProcessPruned(u._2))
    // remove the values from victimCache that were added to pruned
    cache.victimCache.trimStart(prunedAddition.size)
  }

  /**
   * Computes all victims for the partition. If there are any remaining values
   * in the right iterator, those are considered victims.
   *
   * @param cache The cache for this partition.
   * @param right The right iterator containing unmatched regions.
   * @return An iterable containing all pruned hits.
   */
  override protected def finalizeHits(cache: SetTheoryCache[U, RT, RU],
                                      right: BufferedIterator[(ReferenceRegion, U)]): Iterable[(RT, RU)] = {
    cache.pruned ++
      right.map(f => postProcessPruned(f._2))
  }
}

private class SetTheoryCache[U, RT, RU] {
  // caches potential hits
  val cache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty
  // caches potential pruned and joined values
  val victimCache: ListBuffer[(ReferenceRegion, U)] = ListBuffer.empty
  // the pruned values that do not contain any hits from the left
  val pruned: ListBuffer[(RT, RU)] = ListBuffer.empty
}
