/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.adam.rdd.RegionMultijoin

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceMapping
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object YetAnotherRegionJoin extends Serializable {

  /**
   * Multi-joins together two RDDs that contain objects that map to reference regions.
   * The elements from the first RDD become the key of the output RDD, and the value
   * contains all elements from the second RDD which overlap the region of the key.
   * This is a multi-join, so it preserves n-to-m relationships between regions.
   *
   * @tparam T1 Type of the objects in the first RDD.
   * @tparam T2 Type of the objects in the second RDD.
   *
   * @param sc A spark context from the cluster that will perform the join
   * @param rdd1 RDD of values on which we build an interval tree. Assume |rdd1| < |rdd2|
   */
  def overlapJoin[T1, T2](sc: SparkContext,
                          rdd1: RDD[T1],
                          rdd2: RDD[T2])(implicit t1Mapping: ReferenceMapping[T1],
                                         t2Mapping: ReferenceMapping[T2],
                                         t1Manifest: ClassTag[T1],
                                         t2Manifest: ClassTag[T2]): RDD[(T1, Iterable[T2])] = {

    val numPartitions = rdd2.partitions.length
    /*Create an index that we will use for rdd1. Make sure it contains the same number
    of partitions with rdd1 because of zip*/
    val indices = sc.parallelize(1L to rdd1.count, numPartitions)

    val indexedRdd1 = indices.zip(rdd1).keyBy(_._1)

    /*Collect only Reference regions and the index of indexedRdd1*/
    val localIntervals = indexedRdd1.map(x => (t1Mapping.getReferenceRegion(x._2._2), x._1)).collect()
    /*Create and broadcast an interval tree*/
    val intervalTree = sc.broadcast(new IntervalTree[Long](localIntervals.head._1.referenceName, localIntervals.toList))

    val kvrdd2 = rdd2.map(x => (intervalTree.value.getAllOverlappings(t2Mapping.getReferenceRegion(x)), x)) //join entry with the intervals returned from the interval tree
      .filter(x => x._1 != Nil) //filter out entries that do not join anywhere
      .flatMap(t => t._1.map(s => (s._2, t._2))) //create pairs of (index1, rdd2Elem)
      .groupBy(_._1)
      .map(kv => (kv._1, kv._2.map(t => t._2))) //lose the index from the value part

    val ret = indexedRdd1
      .join(kvrdd2)
      .map(x => (x._2._1._2, x._2._2)) //fix the output format
    ret
  }

}
