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
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferenceMapping, ReferencePosition, ReferenceRegion }
import scala.annotation.tailrec
import scala.reflect.ClassTag

object ParallelRegionMultijoin extends Serializable {

  /**
   * Multi-joins together two RDDs that contain objects that map to reference regions.
   * The elements from the first RDD become the key of the output RDD, and the value
   * contains all elements from the second RDD which overlap the region of the key.
   * This is a multi-join, so it preserves n-to-m relationships between regions.
   *
   * @tparam T1 Type of the objects in the first RDD.
   * @tparam T2 Type of the objects in the second RDD.
   *
   * @param rdd1 RDD of values
   */
  def overlapJoin[T1, T2](rdd1: RDD[T1],
                          rdd2: RDD[T2])(implicit t1Mapping: ReferenceMapping[T1],
                                         t2Mapping: ReferenceMapping[T2],
                                         t1Manifest: ClassTag[T1],
                                         t2Manifest: ClassTag[T2]): RDD[(T1, Iterable[T2])] = {
    // map to join items and then cache
    val jRdd1: RDD[Item] = rdd1.map(r => {
      val i: Item = JoinItem(t1Mapping.getReferenceRegion(r), r)
      i
    })
    val jRdd2: RDD[Item] = rdd2.map(r => {
      val i: Item = JoinItem(t2Mapping.getReferenceRegion(r), r)
      i
    })
    jRdd1.cache()
    jRdd2.cache()

    // generate the set of points that exist
    val points = jRdd1.flatMap(i => {
      // cast
      val ji = i.asInstanceOf[JoinItem[T1]]

      ji.r.toPositions.map(p => (p, (ji.r, true)))
    }) ++ jRdd2.flatMap(i => {
      // cast
      val ji = i.asInstanceOf[JoinItem[T2]]

      ji.r.toPositions.map(p => (p, (ji.r, false)))
    })

    // tail recursive helper function to see if a collection contains both true and false
    // boolean values that opportunistically returns early
    def hasTrueAndFalse(iter: Iterator[Boolean]): Boolean = {
      @tailrec def hasTrueAndFalseHelper(iter: Iterator[Boolean],
                                         b: Boolean): Boolean = {
        if (!iter.hasNext) {
          false
        } else {
          val curr = iter.next

          if (curr != b) {
            true
          } else {
            hasTrueAndFalseHelper(iter, curr)
          }
        }
      }

      // do we have elements?
      if (iter.hasNext) {
        val b = iter.next
        hasTrueAndFalseHelper(iter, b)
      } else {
        false
      }
    }

    // group the points by their position and filter all points where we don't
    // have members from both original rdds
    val overlappingPositions = points.groupByKey()
      .filter(kv => hasTrueAndFalse(kv._2.map(p => p._2).toIterator))

    // if we have points from both sets, are we at the earliest possible intersection point?
    // if so, emit that region pair
    val overlappingRegions: RDD[Item] = overlappingPositions.flatMap(kv => {
      val (pos, regions) = kv
      val firstRegions = regions.filter(p => p._2).map(p => p._1)
      val secondRegions = regions.filter(p => !p._2).map(p => p._1)

      firstRegions.flatMap(r1 => {
        secondRegions.flatMap(r2 => {
          if (r1.start == pos.pos || r2.start == pos.pos) {
            val item: Item = RegionItem(r1, r2)
            Some(item)
          } else {
            None
          }
        })
      })
    })

    // create second rdd joined against
    val firstJoinRdd: RDD[Item] = (jRdd2 ++ overlappingRegions.map(_.asInstanceOf[RegionItem].flip))
      .groupBy(_.r)
      .flatMap(kv => {
        val (_, items) = kv

        // do we have items to join at this site? if so, cartesian and return, else emit nullset
        val joinItems = items.flatMap(i => i match {
          case ji: JoinItem[T2] => Some(ji)
          case _                => None
        })
        if (joinItems.size > 0) {
          items.flatMap(i => i match {
            case ri: RegionItem => Some(ri)
            case _              => None
          }).map(r => JoinedItem[T2](r.item, joinItems.map(_.item)))
        } else {
          Iterable[JoinedItem[T2]]()
        }
      })

    // unpersist intermediate join rdds
    jRdd2.unpersist()

    // now, create final join rdd
    val finalJoinRdd: RDD[(T1, Iterable[T2])] = (jRdd1 ++ firstJoinRdd)
      .groupBy(_.r)
      .flatMap(kv => {
        val (_, items) = kv

        // get our items of type 1 and of type 2
        val firstClassItems: Iterable[T1] = items.flatMap(i => i match {
          case ji: JoinItem[T1] => Some(ji.item)
          case _                => None
        })
        val secondClassItems: Iterable[T2] = items.flatMap(i => i match {
          case ji: JoinedItem[T2] => ji.items
          case _                  => Iterable[T2]()
        })
        // the second class items aren't second class citizens, rather, they're class 2 out of {T1, T2}
        // we do not begrudge their existance

        // cartesian these good classes up, if we've got them
        if (firstClassItems.size > 0 && secondClassItems.size > 0) {
          firstClassItems.map(fci => (fci, secondClassItems))
        } else {
          Iterable[(T1, Iterable[T2])]()
        }
      })

    // unpersist intermediate join rdd
    jRdd1.unpersist()

    // return
    finalJoinRdd
  }
}

class Item(val r: ReferenceRegion) extends Serializable {
  override def toString: String = r.toString
}

case class JoinItem[T](override val r: ReferenceRegion, item: T) extends Item(r) {
  override def toString: String = r.toString + ", " + item.toString
}

case class JoinedItem[T](override val r: ReferenceRegion, items: Iterable[T]) extends Item(r) {
  override def toString: String = r.toString + ", (" + items.map(_.toString).reduce(_ + ", " + _) + ")"
}

case class RegionItem(override val r: ReferenceRegion, item: ReferenceRegion) extends Item(r) {
  def flip: RegionItem = RegionItem(item, r)

  override def toString: String = r.toString + ", " + item.toString
}
