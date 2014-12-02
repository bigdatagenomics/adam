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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import scala.math._

import scala.reflect.ClassTag

/**
 * PairingRDD provides some simple helper methods, allowing us take an RDD (presumably an
 * RDD whose values are in some reasonable or intelligible order within and across partitions)
 * and get paired or windowed views on that list of items.
 *
 * @param rdd The RDD of ordered values
 * @param kt The type of the values in the RDD must be manifest
 * @tparam T The type of the values in the RDD
 */
class PairingRDD[T](rdd: RDD[T])(implicit kt: ClassTag[T], ordering: Ordering[T]) extends Serializable {

  private val sorted: RDD[T] = rdd.sortBy(k => k, ascending = true)

  /**
   * Replicates the Seq.sliding(int) method, where we turn an RDD[T] into an RDD[Seq[T]],
   * where each internal Seq contains exactly 'width' values taken (in order) from the original
   * RDD, and where all such windows are presented 'in order' in the output set.
   *
   * E.g. the result of 'sliding(3)' on an RDD of the elements
   *    1, 2, 3, 4, 5
   *
   * Should be an RDD of
   *    Seq(1, 2, 3), Seq(2, 3, 4), Seq(3, 4, 5)
   *
   * @param width The 'width' of the sliding window to calculate
   * @return An RDD of the sliding window values
   */
  def sliding(width: Int): RDD[Seq[T]] = {
    val base: RDD[(Long, T)] = sorted.zipWithIndex().map(p => (p._2, p._1))

    val allOffsets: RDD[(Long, (Long, T))] = base.flatMap(
      (p: (Long, T)) =>
        (0 until min(width, p._1.toInt + 1)).map(w => (p._1 - w, (p._1, p._2)))
    )

    val grouped: RDD[Seq[T]] = allOffsets.groupByKey().map {
      case (index: Long, values: Iterable[(Long, T)]) =>
        values.toSeq.sortBy(_._1).map(_._2)
    }

    grouped.filter(_.length == width)
  }

  /**
   * The 'pair' method is a simplified version of .sliding(2), returning just pairs
   * of (T, T) values for every consecutive pair of T values in the input RDD.
   *
   * For example, calling .pair() on a (sorted) RDD of
   *   1, 2, 3, 4
   *
   * should return the following pairs
   *   (1, 2), (2, 3), (3, 4)
   *
   * @return an RDD[(T, T)] of all consecutive pairs of values
   */
  def pair(): RDD[(T, T)] = {
    val indexed: RDD[(Long, T)] = sorted.zipWithIndex().map(p => (p._2, p._1)).sortByKey()
    val indexMinusOne: RDD[(Long, T)] = indexed.map(p => (p._1 - 1, p._2))

    indexed.join(indexMinusOne).map(_._2)
  }

  /**
   * The 'pairWithEnds' method is a variation on 'pairs', except that it returns two
   * _extra_ pairs (relative to 'pairs') corresponding to the first and last elements
   * of the original RDD.  Every (t1, t2) from .pair() now becomes a (Some(t1), Some(t2))
   * with .pairWithEnds().  The first element is a (None, Some(t0)) and the last element
   * is a (Some(tN), None).
   *
   * For example, calling .pairWithEnds() on a (sorted) RDD of
   *   1, 2, 3
   *
   * should return the following pairs
   *   (None, Some(1)), (Some(1), Some(2)), (Some(2), Some(3)), (Some(3), None)
   *
   * (This is immediately useful as a helper method inside the Coverage class, but also
   * might be useful to other applications as well, that rely on a total ordering of the
   * elements within a single RDD.)
   *
   * @return an RDD[(T, T)] of all consecutive pairs of values
   */
  def pairWithEnds(): RDD[(Option[T], Option[T])] = {
    val indexed: RDD[(Long, Option[T])] = sorted.zipWithIndex().map(p => (p._2, Some(p._1)))
    val indexMinusOne: RDD[(Long, Option[T])] = indexed.map(p => (p._1 - 1, p._2))
    val max: Long = indexed.map(_._1).count()

    if (max == 0) { return rdd.sparkContext.emptyRDD }

    val initial: RDD[(Long, Option[T])] = indexed.sparkContext.parallelize(Seq(-1L -> None))
    val terminal: RDD[(Long, Option[T])] = indexed.sparkContext.parallelize(Seq((max - 1) -> None))

    val initialed = indexed.union(initial)
    val terminated = indexMinusOne.union(terminal)
    val joined = initialed.join(terminated).sortByKey(ascending = true)

    joined.map(_._2)
  }

}

object PairingRDD extends Serializable {
  implicit def rddToPairingRDD[T](rdd: RDD[T])(implicit kt: ClassTag[T], ordering: Ordering[T]): PairingRDD[T] =
    new PairingRDD[T](rdd)
}
