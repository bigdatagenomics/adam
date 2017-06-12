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
    extends SetTheoryBetweenCollections[T, U, X, Y, RT, RX]
    with SetTheoryPrimitive {

  var currentClosest: ReferenceRegion = ReferenceRegion.empty

  override protected def condition(firstRegion: ReferenceRegion,
                                   secondRegion: ReferenceRegion,
                                   threshold: Long = 0L): Boolean = {
    firstRegion.unstrandedDistance(currentClosest)
      .exists(_ == firstRegion.unstrandedDistance(secondRegion).getOrElse(Long.MaxValue))
  }

  override protected def pruneCacheCondition(cachedRegion: ReferenceRegion,
                                             to: ReferenceRegion): Boolean = {
    if (cachedRegion.referenceName != to.referenceName) {
      true
    } else {
      to.unstrandedDistance(cachedRegion).get >
        to.unstrandedDistance(currentClosest).getOrElse(Long.MaxValue)
    }
  }

  override protected def advanceCacheCondition(candidateRegion: ReferenceRegion,
                                               until: ReferenceRegion): Boolean = {

    if (candidateRegion.referenceName != until.referenceName) {
      false
    } else if (until.referenceName != currentClosest.referenceName ||
      until.unstrandedDistance(candidateRegion).get <=
      until.unstrandedDistance(currentClosest).getOrElse(Long.MaxValue)) {

      currentClosest = candidateRegion
      true
    } else {
      false
    }
  }

  override protected def prepare()(implicit tTag: ClassTag[T], xtag: ClassTag[X]): (RDD[(ReferenceRegion, T)], RDD[(ReferenceRegion, X)]) = {

    val (preparedLeftRdd, partitionMap) = {
      if (leftRdd.optPartitionMap.isDefined) {
        (leftRdd.flattenRddByRegions, leftRdd.optPartitionMap.get)
      } else {
        val sortedLeft = leftRdd.sortLexicographically(storePartitionMap = true)
        (sortedLeft.flattenRddByRegions, sortedLeft.optPartitionMap.get)
      }
    }

    val adjustedPartitionMapWithIndex = partitionMap
      // the zipWithIndex gives us the destination partition ID
      .zipWithIndex
      .filter(_._1.nonEmpty)
      .map(f => (f._1.get, f._2))
      .map(g => {
        // in the case where we span multiple referenceNames
        if (g._1._1.referenceName != g._1._2.referenceName) {
          // create a ReferenceRegion that goes to the end of the chromosome
          (ReferenceRegion(
            g._1._1.referenceName,
            g._1._1.start,
            g._1._1.end),
            g._2)
        } else {
          // otherwise we just have the ReferenceRegion span from partition
          // start to end
          (ReferenceRegion(
            g._1._1.referenceName,
            g._1._1.start,
            g._1._2.end),
            g._2)
        }
      })

    val partitionMapIntervals = IntervalArray(
      adjustedPartitionMapWithIndex,
      adjustedPartitionMapWithIndex.maxBy(_._1.width)._1.width,
      sorted = true)

    val assignedRightRdd = {
      val firstPass = rightRdd.flattenRddByRegions.mapPartitions(iter => {
        iter.flatMap(f => {
          val rangeOfHits = partitionMapIntervals.get(f._1, requireOverlap = false)
          rangeOfHits.map(g => ((f._1, g._2), f._2))
        })
      }, preservesPartitioning = true)

      val partitionsWithoutData =
        partitionMap.indices.filterNot(firstPass.map(_._1._2).distinct().collect.contains)

      val partitionsToSend = partitionsWithoutData.foldLeft(List.empty[List[Int]])((b, a) => {
        if (b.isEmpty) {
          List(List(a))
        } else if (a == b.last.last + 1) {
          b.dropRight(1).:+(b.last.:+(a))
        } else {
          b.:+(List(a))
        }
      }).flatMap(f => List((f.head - 1, f.length), (f.last + 1, -1 * f.length)))

      firstPass.flatMap(f => {
        val index = partitionsToSend.indexWhere(_._1 == f._1._2)
        if (index < 0) {
          List(f)
        } else {
          if (partitionsToSend(index)._2 < 0) {
            (partitionsToSend(index)._2 to 0)
              .map(g => ((f._1._1, f._1._2 + g), f._2))
          } else {
            (0 to partitionsToSend(index)._2)
              .map(g => ((f._1._1, f._1._2 + g), f._2)) ++ {
                if (index == partitionsToSend.lastIndexWhere(_._1 == f._1._2)) {
                  List()
                } else {
                  val endIndex = partitionsToSend.lastIndexWhere(_._1 == f._1._2)
                  (partitionsToSend(endIndex)._2 to -1)
                    .map(g => ((f._1._1, f._1._2 + g), f._2))
                }
              }
          }
        }
      })
    }

    val preparedRightRdd =
      assignedRightRdd
        .repartitionAndSortWithinPartitions(
          ManualRegionPartitioner(partitionMap.length))
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
 * @param optPartitionMap An optional partition map defining the left RDD
 *   partition bounds.
 * @param threshold The maximum distance allowed for the closest.
 * @tparam T The type of the left records.
 * @tparam U The type of the right records.
 */
case class ShuffleClosestRegion[T, U <: GenomicRDD[T, U], X, Y <: GenomicRDD[X, Y]](
  protected val leftRdd: GenomicRDD[T, U],
  protected val rightRdd: GenomicRDD[X, Y],
  protected val optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]],
  protected val threshold: Long = Long.MaxValue,
  protected val optPartitions: Option[Int] = None)
    extends Closest[T, U, X, Y, T, Iterable[X]]
    with VictimlessSetTheoryBetweenCollections[T, U, X, Y, T, Iterable[X]] {

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
