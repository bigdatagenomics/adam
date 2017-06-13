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
import org.bdgenomics.adam.rdd.GenomicRDD
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[settheory] trait SetTheory[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y], RT, RX]
    extends Serializable {

  protected val threshold: Long

  /**
   * The condition that should be met in order for the primitive to be
   * computed.
   *
   * @param firstRegion The region to test against.
   * @param secondRegion The region to test.
   * @param distanceThreshold The threshold for the primitive.
   * @return True if the threshold requirement is met.
   *         False if the threshold requirement is not met.
   */
  protected def condition(firstRegion: ReferenceRegion,
                          secondRegion: ReferenceRegion,
                          cache: SetTheoryCache[X, RT, RX],
                          distanceThreshold: Long = 0L): Boolean
}

/**
 * The parent class for all inter-collection set theory operations.
 *
 * @tparam T The left side row data.
 * @tparam U The right side row data.
 * @tparam RT The return type for the left side row data.
 * @tparam RX The return type for the right side row data.
 */
private[rdd] abstract class SetTheoryBetweenCollections[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y], RT, RX]
    extends SetTheory[T, U, X, Y, RT, RX] {

  protected val leftRdd: GenomicRDD[T, U]
  protected val rightRdd: GenomicRDD[X, Y]
  protected val optPartitions: Option[Int]
  /**
   * Post process and format the hits for a given left record.
   *
   * @param currentLeft The current left record.
   * @param iter The iterable of hits.
   * @return The post processed hits.
   */
  protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                iter: Iterable[(ReferenceRegion, X)]): Iterable[(RT, RX)]

  /**
   * The condition by which a candidate is removed from the cache.
   *
   * @see pruneCache
   * @param cachedRegion The current region in the cache.
   * @param to The region that is compared against.
   * @return True for regions that should be removed.
   *         False for all regions that should remain in the cache.
   */
  protected def pruneCacheCondition(cachedRegion: ReferenceRegion,
                                    to: ReferenceRegion,
                                    cache: SetTheoryCache[X, RT, RX]): Boolean

  /**
   * The condition by which a candidate region is added to the cache.
   *
   * @see advanceCache
   * @param candidateRegion The current candidate region.
   * @param until The region to compare against.
   * @return True for all regions to be added to the cache.
   *         False for regions that should not be added to the cache.
   */
  protected def advanceCacheCondition(candidateRegion: ReferenceRegion,
                                      until: ReferenceRegion,
                                      cache: SetTheoryCache[X, RT, RX]): Boolean

  /**
   * Handles the situation where the left or right iterator is empty.
   *
   * @param left The left iterator.
   * @param right The right iterator.
   * @return The formatted resulting RDD.
   */
  protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                        right: Iterator[(ReferenceRegion, X)]): Iterator[(RT, RX)]

  /**
   * Prunes the cache based on the condition set in pruneCacheCondition.
   *
   * @see pruneCacheCondition
   * @param to The region to prune against.
   * @param cache The cache for this partition.
   */
  protected def pruneCache(to: ReferenceRegion,
                           cache: SetTheoryCache[X, RT, RX])

  /**
   * Advances the cache based on the condition set in advanceCacheCondition
   *
   * @see advanceCacheCondition
   * @param right The right buffered iterator to pull from.
   * @param until The region to compare against.
   * @param cache The cache for this partition.
   */
  protected def advanceCache(right: BufferedIterator[(ReferenceRegion, X)],
                             until: ReferenceRegion,
                             cache: SetTheoryCache[X, RT, RX])

  /**
   * Computes all victims for the partition.
   *
   * @param cache The cache for this partition.
   * @param right The right iterator.
   * @return The finalized hits for this partition.
   */
  protected def finalizeHits(cache: SetTheoryCache[X, RT, RX],
                             right: BufferedIterator[(ReferenceRegion, X)]): Iterable[(RT, RX)]

  /**
   * Prepares and partitions the left and right. Makes no assumptions about the
   * underlying data. This is particularly important to avoid computation and
   * shuffle until the user calls compute().
   *
   * @return The prepared and partitioned left and right RDD.
   */
  protected def prepare()(implicit tTag: ClassTag[T], xtag: ClassTag[X]): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, X)])

  /**
   * Computes the set theory primitive for the two RDDs.
   *
   * @return An RDD resulting from the primitive operation.
   */
  def compute()(implicit tTag: ClassTag[T], xtag: ClassTag[X]): RDD[(RT, RX)] = {
    val (preparedLeft, preparedRight) = prepare()
    preparedLeft.zipPartitions(preparedRight)(makeIterator)
  }

  /**
   * Processes all hits from the cache and creates an iterator for the current
   * left based on the primitive operation.
   *
   * @param cache The cache of potential hits.
   * @param currentLeft The current left record.
   * @return An iterator containing all processed hits.
   */
  protected def processHits(currentLeft: (ReferenceRegion, T),
                            cache: SetTheoryCache[X, RT, RX]): Iterable[(RT, RX)] = {

    val (currentLeftRegion, _) = currentLeft
    // post processing formats the hits for each individual type of join
    postProcessHits(currentLeft,
      cache.cache.filter(y => {
        // everything that overlaps the left region is a hit
        condition(currentLeftRegion, y._1, cache, threshold)
      }))
  }

  /**
   * Computes the set theory primitive for the two Iterators on each partition.
   *
   * @see processHits
   * @param leftIter The iterator for the left side of the primitive.
   * @param rightIter The iterator for the right side of the primitive.
   * @return The resulting Iterator based on the primitive operation.
   */
  protected def makeIterator(leftIter: Iterator[(ReferenceRegion, T)],
                             rightIter: Iterator[(ReferenceRegion, X)]): Iterator[(RT, RX)] = {

    val cache = new SetTheoryCache[X, RT, RX]

    if (leftIter.isEmpty || rightIter.isEmpty) {
      emptyFn(leftIter, rightIter)
    } else {
      val leftBuffered = leftIter.buffered
      val rightBuffered = rightIter.buffered

      leftBuffered.flatMap(f => {
        val (currentRegion, _) = f
        advanceCache(rightBuffered, currentRegion, cache)
        pruneCache(currentRegion, cache)
        processHits(f, cache)
      }) ++ finalizeHits(cache, rightBuffered)
    }
  }
}

/**
 * Perform a set theory primitive with victims.
 *
 * @tparam T The left side row data.
 * @tparam U The right side row data.
 * @tparam RT The return type for the left side row data.
 * @tparam RX The return type for the right side row data.
 */
private[settheory] trait SetTheoryBetweenCollectionsWithVictims[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y], RT, RX]
    extends SetTheoryBetweenCollections[T, U, X, Y, RT, RX] {

  /**
   * Post processes the pruned records to format them appropriately.
   *
   * @param pruned The pruned record.
   * @return The formatted, post processed record.
   */
  protected def postProcessPruned(pruned: X): (RT, RX)

  override protected def pruneCache(to: ReferenceRegion,
                                    cache: SetTheoryCache[X, RT, RX]) = {

    val toThreshold = to.pad(threshold)
    // remove everything from cache that will never again be joined
    cache.cache.trimStart({
      val trimLocation =
        cache.cache
          .indexWhere(f => !pruneCacheCondition(f._1, toThreshold, cache))

      if (trimLocation < 0) {
        0
      } else {
        trimLocation
      }
    })

    // add the values from the victimCache that are candidates to be joined
    // the the current left
    val cacheAddition =
      cache.victimCache
        .takeWhile(f => !pruneCacheCondition(f._1, toThreshold, cache))

    cache.cache ++= cacheAddition
    // remove the values from the victimCache that were just added to cache
    cache.victimCache.trimStart(cacheAddition.size)

    // add to pruned any values that do not have any matches to a left
    // and perform post processing to format the new pruned values
    val prunedAddition =
      cache.victimCache
        .takeWhile(f => f._1.compareTo(toThreshold) <= 0)

    cache.pruned ++= prunedAddition
      .map(u => postProcessPruned(u._2))
    // remove the values from victimCache that were added to pruned
    cache.victimCache.trimStart(prunedAddition.size)
  }

  override protected def advanceCache(right: BufferedIterator[(ReferenceRegion, X)],
                                      until: ReferenceRegion,
                                      cache: SetTheoryCache[X, RT, RX]) = {

    while (right.hasNext &&
      advanceCacheCondition(right.head._1, until.pad(threshold), cache)) {

      val x = right.next()
      cache.victimCache += ((x._1, x._2))
    }
  }

  override protected def finalizeHits(cache: SetTheoryCache[X, RT, RX],
                                      right: BufferedIterator[(ReferenceRegion, X)]): Iterable[(RT, RX)] = {
    cache.pruned ++
      right.map(f => postProcessPruned(f._2))
  }
}

/**
 * Perform a set theory primitive without victims.
 *
 * @tparam T The left side row data.
 * @tparam U The right side row data.
 * @tparam RT The return type for the left side row data.
 * @tparam RX The return type for the right side row data.
 */
private[settheory] trait VictimlessSetTheoryBetweenCollections[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y], RT, RX]
    extends SetTheoryBetweenCollections[T, U, X, Y, RT, RX] {

  override protected def pruneCache(to: ReferenceRegion,
                                    cache: SetTheoryCache[X, RT, RX]) = {
    cache.cache.trimStart({
      val index = cache.cache.indexWhere(f => !pruneCacheCondition(f._1, to, cache))
      if (index <= 0) {
        0
      } else {
        index
      }
    })
  }

  override protected def advanceCache(right: BufferedIterator[(ReferenceRegion, X)],
                                      until: ReferenceRegion,
                                      cache: SetTheoryCache[X, RT, RX]) = {
    while (right.hasNext && advanceCacheCondition(right.head._1, until, cache)) {
      cache.cache += right.next
    }
  }

  override protected def finalizeHits(cache: SetTheoryCache[X, RT, RX],
                                      right: BufferedIterator[(ReferenceRegion, X)]): Iterable[(RT, RX)] = {
    // Victimless Set Theory drops the remaining records
    Iterable.empty
  }
}

/**
 * Contains all the caching data for a set theory operation.
 *
 * @tparam X The right side record type.
 * @tparam RT The left side result type.
 * @tparam RX The right side result type.
 */
private[settheory] class SetTheoryCache[X, RT, RX] {

  // caches potential hits
  val cache: ListBuffer[(ReferenceRegion, X)] = ListBuffer.empty

  // caches potential pruned and joined values
  val victimCache: ListBuffer[(ReferenceRegion, X)] = ListBuffer.empty

  // the pruned values that do not contain any hits from the left
  val pruned: ListBuffer[(RT, RX)] = ListBuffer.empty

  // the closest values for the hits.
  var closest: Option[ReferenceRegion] = None
}
