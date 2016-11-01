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

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.utils.intervalarray.IntervalArray
import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Implements a shuffle free (broadcast) region join.
 *
 * The broadcast values are stored in a sorted array. It was going to be an
 * ensemble of interval trees, but, that didn't work out.
 */
trait TreeRegionJoin[T, U] {

  /**
   * Performs an inner region join between two RDDs, and groups by the
   * value on the right side of the join.
   *
   * @param leftRdd RDD on the left side of the join. Will be collected to the
   *   driver and broadcast.
   * @param rightRdd RDD on the right side of the join.
   * @return Returns an RDD where each element is a value from the right RDD,
   *   along with all values from the left RDD that it overlapped.
   */
  private[rdd] def runJoinAndGroupByRight(
    leftRdd: RDD[(ReferenceRegion, T)],
    rightRdd: RDD[(ReferenceRegion, U)])(
      implicit tTag: ClassTag[T]): RDD[(Iterable[T], U)] = TreeJoin.time {

    // build the tree from the left RDD
    val tree = IntervalArray(leftRdd)

    RunningMapSideJoin.time {
      // broadcast this tree
      val broadcastTree = leftRdd.context
        .broadcast(tree)

      // map and join
      rightRdd.map(kv => {
        val (rr, u) = kv

        // what values keys does this overlap in the tree?
        val overlappingValues = broadcastTree.value
          .get(rr)
          .map(_._2)

        (overlappingValues, u)
      })
    }
  }
}

/**
 * Implements an inner region join where the left side of the join is broadcast.
 */
case class InnerTreeRegionJoin[T, U]() extends RegionJoin[T, U, T, U] with TreeRegionJoin[T, U] {

  /**
   * Performs an inner region join between two RDDs.
   *
   * @param baseRDD The 'left' side of the join
   * @param joinedRDD The 'right' side of the join
   * @param tManifest implicit type of baseRDD
   * @param uManifest implicit type of joinedRDD
   * @tparam T type of baseRDD
   * @tparam U type of joinedRDD
   * @return An RDD of pairs (x, y), where x is from baseRDD, y is from joinedRDD, and the region
   *         corresponding to x overlaps the region corresponding to y.
   */
  def partitionAndJoin(
    baseRDD: RDD[(ReferenceRegion, T)],
    joinedRDD: RDD[(ReferenceRegion, U)])(implicit tManifest: ClassTag[T],
                                          uManifest: ClassTag[U]): RDD[(T, U)] = {
    runJoinAndGroupByRight(baseRDD, joinedRDD)
      .flatMap(kv => {
        val (leftIterable, right) = kv
        leftIterable.map(left => (left, right))
      })
  }
}

/**
 * Implements a right outer region join where the left side of the join is
 * broadcast.
 */
case class RightOuterTreeRegionJoin[T, U]() extends RegionJoin[T, U, Option[T], U] with TreeRegionJoin[T, U] {

  /**
   * Performs a right outer region join between two RDDs.
   *
   * @param baseRDD The 'left' side of the join
   * @param joinedRDD The 'right' side of the join
   * @param tManifest implicit type of baseRDD
   * @param uManifest implicit type of joinedRDD
   * @tparam T type of baseRDD
   * @tparam U type of joinedRDD
   * @return An RDD of pairs (Option[x], y), where the optional x value is from
   *   baseRDD, y is from joinedRDD, and the region corresponding to x overlaps
   *   the region corresponding to y. If there are no keys in the baseRDD that
   *   overlap a given key (y) from the joinedRDD, x will be None.
   */
  def partitionAndJoin(
    baseRDD: RDD[(ReferenceRegion, T)],
    joinedRDD: RDD[(ReferenceRegion, U)])(implicit tManifest: ClassTag[T],
                                          uManifest: ClassTag[U]): RDD[(Option[T], U)] = {
    runJoinAndGroupByRight(baseRDD, joinedRDD)
      .flatMap(kv => {
        val (leftIterable, right) = kv

        if (leftIterable.isEmpty) {
          Iterable((None, right))
        } else {
          leftIterable.map(left => (Some(left), right))
        }
      })
  }
}

/**
 * Performs an inner region join, followed logically by grouping by the right
 * value. This is implemented without any shuffling; the join naturally returns
 * values on the left grouped by the right value.
 */
case class InnerTreeRegionJoinAndGroupByRight[T, U]() extends RegionJoin[T, U, Iterable[T], U] with TreeRegionJoin[T, U] {

  /**
   * Performs an inner join between two RDDs, followed by a groupBy on the
   * right object.
   *
   * @param baseRDD The 'left' side of the join
   * @param joinedRDD The 'right' side of the join
   * @param tManifest implicit type of baseRDD
   * @param uManifest implicit type of joinedRDD
   * @tparam T type of baseRDD
   * @tparam U type of joinedRDD
   * @return An RDD of pairs (Iterable[x], y), where the Iterable[x] is from
   *   baseRDD, y is from joinedRDD, and all values in the Iterable[x] are
   *   aligned at regions that overlap the region corresponding to y. If the
   *   iterable is empty, the key-value pair is filtered out.
   */
  def partitionAndJoin(
    baseRDD: RDD[(ReferenceRegion, T)],
    joinedRDD: RDD[(ReferenceRegion, U)])(implicit tManifest: ClassTag[T],
                                          uManifest: ClassTag[U]): RDD[(Iterable[T], U)] = {
    runJoinAndGroupByRight(baseRDD, joinedRDD)
      .filter(_._1.nonEmpty)
  }
}

/**
 * Performs a right outer region join, followed logically by grouping by the right
 * value. This is implemented without any shuffling; the join naturally returns
 * values on the left grouped by the right value. In this implementation, empty
 * collections on the left side of the join are kept.
 */
case class RightOuterTreeRegionJoinAndGroupByRight[T, U]() extends RegionJoin[T, U, Iterable[T], U] with TreeRegionJoin[T, U] {

  /**
   * Performs an inner join between two RDDs, followed by a groupBy on the
   * right object.
   *
   * @param baseRDD The 'left' side of the join
   * @param joinedRDD The 'right' side of the join
   * @param tManifest implicit type of baseRDD
   * @param uManifest implicit type of joinedRDD
   * @tparam T type of baseRDD
   * @tparam U type of joinedRDD
   * @return An RDD of pairs (Iterable[x], y), where the Iterable[x] is from
   *   baseRDD, y is from joinedRDD, and all values in the Iterable[x] are
   *   aligned at regions that overlap the region corresponding to y. If the
   *   iterable is empty, the key-value pair is NOT filtered out.
   */
  def partitionAndJoin(
    baseRDD: RDD[(ReferenceRegion, T)],
    joinedRDD: RDD[(ReferenceRegion, U)])(implicit tManifest: ClassTag[T],
                                          uManifest: ClassTag[U]): RDD[(Iterable[T], U)] = {
    runJoinAndGroupByRight(baseRDD, joinedRDD)
  }
}
