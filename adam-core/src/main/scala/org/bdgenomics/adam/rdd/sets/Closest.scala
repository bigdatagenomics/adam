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
import org.bdgenomics.adam.rdd.{ GenomicRDD, ManualRegionPartitioner }
import org.bdgenomics.utils.interval.array.IntervalArray
import scala.reflect.ClassTag

/**
 * A trait describing closest implementations that are based on sort-merge.
 *
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 * @tparam RT The resulting type of the left after the operation.
 * @tparam RX The resulting type of the right after the operation.
 */
sealed trait Closest[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y], RT, RX]
    extends SetOperationBetweenCollections[T, X, RT, RX] {

  protected val leftRdd: GenomicRDD[T, U]
  protected val rightRdd: GenomicRDD[X, Y]

  override protected def condition(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   cache: SetTheoryCache[X, RT, RX],
                                   threshold: Long = 0L): Boolean = {

    // we must maintain this invariant throughout the computation
    cache.closest.isDefined &&
      // we want to identify all the regions that share the same distance as our
      // current closest.
      firstRegion.unstrandedDistance(cache.closest.get)
      .exists(_ == firstRegion.unstrandedDistance(secondRegion).getOrElse(Long.MaxValue))
  }

  override protected def pruneCacheCondition(cachedRegion: ReferenceRegion,
                                             to: ReferenceRegion,
                                             cache: SetTheoryCache[X, RT, RX]): Boolean = {

    // we must maintain this invariant throughout the computation
    cache.closest.isDefined &&
      // we want to prune in the case that the unstranded distance between the
      // current query region is greater than our current closest
      cachedRegion.referenceName == to.referenceName &&
      to.unstrandedDistance(cachedRegion).get >
      to.unstrandedDistance(cache.closest.get).getOrElse(Long.MaxValue)
  }

  override protected def advanceCacheCondition(candidateRegion: ReferenceRegion,
                                               until: ReferenceRegion,
                                               cache: SetTheoryCache[X, RT, RX]): Boolean = {

    // if our current closest isn't on the same reference name, we don't
    // consider it the closest, thus we have no current closest
    if (cache.closest.isDefined &&
      cache.closest.get.referenceName != candidateRegion.referenceName) {

      cache.closest = None
    }

    // if the reference names don't match, we don't consider them the closest,
    // unless we have no current closest
    if (candidateRegion.referenceName != until.referenceName &&
      cache.closest.isDefined) {

      false
      // current closest must be set if there is no current closest. This
      // prevents us from dropping results when we don't have any records of that
      // reference name in the dataset. otherwise, we set current closest if it
      // is closer than our current
    } else if (cache.closest.isEmpty ||
      until.referenceName != cache.closest.get.referenceName ||
      until.unstrandedDistance(candidateRegion).get <=
      until.unstrandedDistance(cache.closest.get).getOrElse(Long.MaxValue)) {
      // this object can be short lived, but the overhead should be low for
      // options
      cache.closest = Some(candidateRegion)
      true
    } else {
      // we reach this on the region immediately after we have passed the
      // closest region
      false
    }
  }

  override protected def prepare()(
    implicit tTag: ClassTag[T], xtag: ClassTag[X]): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, X)]) = {

    val numPartitions = optPartitions.getOrElse(leftRdd.rdd.partitions.length)

    val (preparedLeftRdd, destinationPartitionMap) = {
      if (leftRdd.partitionMap.isDefined &&
        numPartitions != leftRdd.rdd.partitions.length) {

        (leftRdd.flattenRddByRegions(), leftRdd.partitionMap)
      } else {
        val sortedLeft = leftRdd.sortLexicographically(numPartitions)
        (sortedLeft.flattenRddByRegions(), sortedLeft.partitionMap)
      }
    }

    // we use an interval array to quickly look up the destination partitions
    val partitionMapIntervals = destinationPartitionMap.toIntervalArray()

    val assignedRightRdd: RDD[((ReferenceRegion, Int), X)] = {
      // copartitioning for the closest is tricky, and requires that we handle
      // unique edge cases, described below.
      // the first pass gives us the initial destination partitions.
      val firstPass = rightRdd.flattenRddByRegions()
        .mapPartitions(iter => {
          iter.flatMap(f => {
            val rangeOfHits = partitionMapIntervals.get(f._1, requireOverlap = false)
            rangeOfHits.map(g => ((f._1, g._2), f._2))
          })
        }, preservesPartitioning = true)

      // we have to find the partitions that don't have right data going there
      // so we can send the flanking partitions' data there
      val partitionsWithoutData =
        destinationPartitionMap.indices.filterNot(firstPass.map(_._1._2).distinct().collect.contains)

      // this gives us a list of partitions that are sending copies of their
      // data and the number of nodes to send to. a negative number of nodes
      // indicates that the data needs to be sent to lower numbered nodes, a
      // positive number indicates that the data needs to be sent to higher
      // numbered nodes. the way this is written, it will handle an arbitrary
      // run of empty partitions.
      val partitionsToSend = partitionsWithoutData.foldLeft(List.empty[List[Int]])((b, a) => {
        if (b.isEmpty) {
          List(List(a))
        } else if (a == b.last.last + 1) {
          b.dropRight(1).:+(b.last.:+(a))
        } else {
          b.:+(List(a))
        }
        // we end up getting all the data from both flanking nodes. we use the
        // length here so we know how many destinations we have resulting from
        // runs of empty partitions.
      }).flatMap(f => List((f.head - 1, f.length), (f.last + 1, -1 * f.length)))

      firstPass.flatMap(f => {
        // extract the destinations for this data record
        val destinations = partitionsToSend.filter(g => g._1 == f._1._2)
        // we use an inclusive range to specify all destinations
        val duplicatedRecords = {
          if (destinations.length == 1) {
            // the data is only going  to lower numbered nodes
            if (destinations.head._2 < 0) {
              destinations.head._2 to 0
              // the data is only going to higher numbered nodes
            } else {
              0 to destinations.head._2
            }
            // the data is going to higher and lower numbered nodes
          } else if (destinations.length == 2) {
            destinations.last._2 to destinations.head._2
            // the data is only going to its original destination
          } else {
            0 to 0
          }
          // add the destination
        }.map(g => ((f._1._1, f._1._2 + g), f._2))

        duplicatedRecords
      })
    }

    val preparedRightRdd =
      assignedRightRdd
        .repartitionAndSortWithinPartitions(
          ManualRegionPartitioner(destinationPartitionMap.length))
        // return to an RDD[(ReferenceRegion, T)], removing the partition ID
        .map(f => (f._1._1, f._2))

    (preparedLeftRdd, preparedRightRdd)
  }
}

/**
 * Perform a sort-merge closest operation.
 *
 * @param leftRdd The left RDD.
 * @param rightRdd The right RDD.
 * @param threshold The maximum distance allowed for the closest.
 * @param optPartitions Optionally sets the number of partitions for the join.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class ShuffleClosestRegion[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](
  protected val leftRdd: GenomicRDD[T, U],
  protected val rightRdd: GenomicRDD[X, Y],
  protected val threshold: Long = Long.MaxValue,
  protected val optPartitions: Option[Int] = None)
    extends Closest[T, U, X, Y, T, Iterable[X]]
    with VictimlessSetOperationBetweenCollections[T, X, T, Iterable[X]] {

  override protected def emptyFn(left: Iterator[(ReferenceRegion, T)],
                                 right: Iterator[(ReferenceRegion, X)]): Iterator[(T, Iterable[X])] = {

    // if the left iterator is not empty, we have failed to correctly
    // partition the data. the right iterator is only allowed to be empty
    // when the left iterator is empty, but we don't care if there's data
    // on the right side if there's no data on the left.
    require(left.isEmpty)
    Iterator.empty
  }

  override protected def postProcessHits(currentLeft: (ReferenceRegion, T),
                                         iter: Iterable[(ReferenceRegion, X)]): Iterable[(T, Iterable[X])] = {
    Iterable((currentLeft._2, iter.map(_._2)))
  }
}
