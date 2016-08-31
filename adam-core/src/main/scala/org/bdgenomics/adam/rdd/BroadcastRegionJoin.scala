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

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  MultiContigNonoverlappingRegions,
  ReferenceRegion
}
import scala.Predef._
import scala.reflect.ClassTag

/**
 * Contains multiple implementations of a 'region join', an operation that joins two sets of
 * regions based on the spatial overlap between the regions.
 *
 * Different implementations will have different performance characteristics -- and new implementations
 * will likely be added in the future, see the notes to each individual method for more details.
 */
sealed trait BroadcastRegionJoin[T, U, RT] extends RegionJoin[T, U, RT, U] {

  /**
   * Performs a region join between two RDDs (broadcast join).
   *
   * This implementation first _collects_ the left-side RDD; therefore, if the left-side RDD is large
   * or otherwise idiosyncratic in a spatial sense (i.e. contains a set of regions whose unions overlap
   * a significant fraction of the genome) then the performance of this implementation will likely be
   * quite bad.
   *
   * Once the left-side RDD is collected, its elements are reduced to their distinct unions;
   * these can then be used to define the partitions over which the region-join will be computed.
   *
   * The regions in the left-side are keyed by their corresponding partition (each such region should have
   * exactly one partition).  The regions in the right-side are also keyed by their corresponding partitions
   * (here there can be more than one partition for a region, since a region may cross the boundaries of
   * the partitions defined by the left-side).
   *
   * Finally, within each separate partition, we essentially perform a cartesian-product-and-filter
   * operation.  The result is the region-join.
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
                                          uManifest: ClassTag[U]): RDD[(RT, U)] = {

    val sc = baseRDD.context

    /**
     * Original Join Design:
     *
     * Parameters:
     *   (1) f : (Range, Range) => T  // an aggregation function
     *   (2) a : RDD[Range]
     *   (3) b : RDD[Range]
     *
     * Return type: RDD[(Range,T)]
     *
     * Algorithm:
     *   1. a.collect() (where a is smaller than b)
     *   2. build a non-overlapping partition on a
     *   3. ak = a.map( v => (partition(v), v) )
     *   4. bk = b.flatMap( v => partitions(v).map( i=>(i,v) ) )
     *   5. joined = ak.join(bk).filter( (i, (r1, r2)) => r1.overlaps(r2) ).map( (i, (r1,r2))=>(r1, r2) )
     *   6. return: joined.reduceByKey(f)
     *
     * Ways in which we've generalized this plan:
     * - removed the aggregation step altogether
     * - carry a sequence dictionary through the computation.
     */

    // First, we group the regions in the left side of the join by their referenceName,
    // and collect them.
    val collectedLeft: Seq[(String, Iterable[ReferenceRegion])] =
      baseRDD
        .map(_._1) // RDD[ReferenceRegion]
        .keyBy(_.referenceName) // RDD[(String,ReferenceRegion)]
        .groupByKey() // RDD[(String,Seq[ReferenceRegion])]
        .collect() // Iterable[(String,Seq[ReferenceRegion])]
        .toSeq // Seq[(String,Seq[ReferenceRegion])]

    // Next, we turn that into a data structure that reduces those regions to their non-overlapping
    // pieces, which we will use as a partition.
    val multiNonOverlapping = new MultiContigNonoverlappingRegions(collectedLeft)

    // Then, we broadcast those partitions -- this will be the function that allows us to
    // partition all the regions on the right side of the join.
    val regions = sc.broadcast(multiNonOverlapping)

    // each element of the left-side RDD should have exactly one partition.
    val smallerKeyed: RDD[(ReferenceRegion, (ReferenceRegion, T))] =
      baseRDD.map(t => (regions.value.regionsFor(t).head, t))

    // each element of the right-side RDD may have 0, 1, or more than 1 corresponding partition.
    val largerKeyed: RDD[(ReferenceRegion, (ReferenceRegion, U))] =
      joinedRDD.flatMap(t => regionsFor(t, regions).map((r: ReferenceRegion) => (r, t)))

    joinAndFilterFn(smallerKeyed, largerKeyed)
  }

  protected def regionsFor(u: (ReferenceRegion, U),
                           regions: Broadcast[MultiContigNonoverlappingRegions]): Iterable[ReferenceRegion]

  protected def joinAndFilterFn(tRdd: RDD[(ReferenceRegion, (ReferenceRegion, T))],
                                uRdd: RDD[(ReferenceRegion, (ReferenceRegion, U))]): RDD[(RT, U)]
}

/**
 * Extends the BroadcastRegionJoin trait to implement an inner join.
 */
case class InnerBroadcastRegionJoin[T, U]() extends BroadcastRegionJoin[T, U, T] {

  protected def joinAndFilterFn(tRdd: RDD[(ReferenceRegion, (ReferenceRegion, T))],
                                uRdd: RDD[(ReferenceRegion, (ReferenceRegion, U))]): RDD[(T, U)] = {
    // this is (essentially) performing a cartesian product within each partition...
    val joined: RDD[(ReferenceRegion, ((ReferenceRegion, T), (ReferenceRegion, U)))] =
      tRdd.join(uRdd)

    // ... so we need to filter the final pairs to make sure they're overlapping.
    joined.flatMap(kv => {
      val (_, (t: (ReferenceRegion, T), u: (ReferenceRegion, U))) = kv

      if (t._1.overlaps(u._1)) {
        Some((t._2, u._2))
      } else {
        None
      }
    })
  }

  protected def regionsFor(u: (ReferenceRegion, U),
                           regions: Broadcast[MultiContigNonoverlappingRegions]): Iterable[ReferenceRegion] = {
    regions.value.regionsFor(u)
  }
}

/**
 * Extends the BroadcastRegionJoin trait to implement a right outer join.
 */
case class RightOuterBroadcastRegionJoin[T, U]() extends BroadcastRegionJoin[T, U, Option[T]] {

  protected def joinAndFilterFn(tRdd: RDD[(ReferenceRegion, (ReferenceRegion, T))],
                                uRdd: RDD[(ReferenceRegion, (ReferenceRegion, U))]): RDD[(Option[T], U)] = {
    // this is (essentially) performing a cartesian product within each partition...
    val joined: RDD[(ReferenceRegion, (Option[(ReferenceRegion, T)], (ReferenceRegion, U)))] =
      tRdd.rightOuterJoin(uRdd)

    // ... so we need to filter the final pairs to make sure they're overlapping.
    joined.map(kv => {
      val (_, (optT: Option[(ReferenceRegion, T)], u: (ReferenceRegion, U))) = kv

      (optT.filter(t => t._1.overlaps(u._1)).map(_._2), u._2)
    })
  }

  protected def regionsFor(u: (ReferenceRegion, U),
                           regions: Broadcast[MultiContigNonoverlappingRegions]): Iterable[ReferenceRegion] = {
    val reg = regions.value.regionsFor(u)
    if (reg.isEmpty) {
      Iterable(u._1)
    } else {
      reg
    }
  }
}
