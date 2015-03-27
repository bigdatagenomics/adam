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
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegionWithOrientation, ReferenceRegion }
import org.bdgenomics.adam.models.ReferenceRegionContext._
import scala.reflect.ClassTag
import scala.math.max

/**
 * Functions (joins, filters, groups, counts) that are often invoked on RDD[R] where R is a
 * type which extends ReferenceRegion (and is often just ReferenceRegion directly).
 *
 * @param rdd The argument RDD
 * @param kt The type of the argument RDD must be explicit
 * @tparam Region The type of the argument RDD
 */
class RegionRDDFunctions[Region <: ReferenceRegion](rdd: RDD[Region])(implicit kt: ClassTag[Region])
    extends Serializable {

  import RegionRDDFunctions._

  /**
   * Performs an 'overlap join' on the input RDD, returning pairs whose first element is a member of the
   * input RDD and whose second element is a member of the argument RDD which overlaps the first element
   * of the pair.
   *
   * @param that The argument RDD
   * @param kt2 The type of the values in the argument RDD must be explicit.
   * @tparam R2 The explicit type of the regions in the argument RDD
   * @return The RDD of pairs (f, s) where f comes from the input RDD and s comes from 'that', the argument
   *         RDD.
   */
  def joinByOverlap[R2 <: ReferenceRegion](that: RDD[R2])(implicit kt2: ClassTag[R2]): RDD[(Region, R2)] =
    BroadcastRegionJoin.partitionAndJoin[Region, Region, R2, R2](rdd.keyBy(r => r), that.keyBy(r => r))

  /**
   * Performs a 'range join' on the input RDD, returning pairs whose first element is a member of the
   * input RDD and whose second element is a member of the argument RDD which is within 'range' bases
   * of the first element of the pair.
   *
   * @param that The argument RDD
   * @param range A long integer; if two regions are within 'range' of each other (along the same chromosome),
   *              then they are present in the returned RDD.
   * @param kt2 The type of the values in the argument RDD must be explicit.
   * @tparam R2 The explicit type of the regions in the argument RDD
   * @return The RDD of pairs (f, s) where f comes from the input RDD and s comes from 'that', the argument
   *         RDD.
   */
  def joinWithinRange[R2 <: ReferenceRegion](that: RDD[R2], range: Long)(implicit kt2: ClassTag[R2]): RDD[(Region, R2)] =
    rdd.keyBy(expander(range))
      .joinByOverlap(that.keyBy(r => r))

  /**
   * Performs a 'filter by overlap' on the input RDD, returning only the subset of values in the input
   * RDD which overlap at least one region in the argument RDD.
   *
   * @param that the argument RDD
   * @param kt2 the type of the values in the argument RDD must be explicit
   * @tparam R2 the type of the values in the argument RDD
   * @return The subset of values from the input RDD which overlap at least one element of the
   *         argument RDD.
   */
  def filterByOverlap[R2 <: ReferenceRegion](that: RDD[R2])(implicit kt2: ClassTag[R2]): RDD[Region] =
    rdd.joinByOverlap(that).map(_._1).distinct()

  /**
   * Performs a 'filter by range' on the input RDD, returning only the subset of values in the input
   * RDD which are within a fixed number of basepairs of at least one region in the argument RDD.
   *
   * @param that the argument RDD
   * @param range a long integer; defines the window within which an input region must be of an argument
   *              region, in order to be reported in the output RDD
   * @param kt2 the type of the values in the argument RDD must be explicit
   * @tparam R2 the type of the values in the argument RDD
   * @return The subset of values from the input RDD which overlap at least one element of the
   *         argument RDD.
   */
  def filterWithinRange[R2 <: ReferenceRegion](that: RDD[R2], range: Long)(implicit kt2: ClassTag[R2]): RDD[Region] =
    rdd.joinWithinRange(that, range).map(_._1).distinct()

  /**
   * Performs the equivalent of a joinByOverlap followed by a groupByKey; in effect, this returns
   * an Iterable of the regions in the argument RDD that overlap a region from the input RDD, _for each_
   * region of the input RDD.
   *
   * @param that the argument RDD
   * @param kt2 the type of the values in the argument RDD must be explicit
   * @tparam R2 the type of the values in the argument RDD
   * @return An RDD of pairs, where the first element is a region from the input RDD and the second
   *         is an Iterable of regions from the argument RDD which overlap the first element.
   */
  def groupByOverlap[R2 <: ReferenceRegion](that: RDD[R2])(implicit kt2: ClassTag[R2]): RDD[(Region, Iterable[R2])] =
    rdd.joinByOverlap(that).groupByKey()

  /**
   * Performs the equivalent of a joinByRange followed by a groupByKey; in effect, this returns
   * an Iterable of the regions in the argument RDD that overlap a region from the input RDD, _for each_
   * region of the input RDD, where 'overlap' is expanded to include a range around each region
   * in the argument RDD.
   *
   * @param that the argument RDD
   * @param range a long integer; pairs of regions within this range of each other are considered to
   *              overlap
   * @param kt2 the type of the values in the argument RDD must be explicit
   * @tparam R2 the type of the values in the argument RDD
   * @return An RDD of pairs, where the first element is a region from the input RDD and the second
   *         is an Iterable of regions from the argument RDD which overlaps (within the range) the
   *         first element.
   */
  def groupByWithinRange[R2 <: ReferenceRegion](that: RDD[R2], range: Long)(implicit kt2: ClassTag[R2]): RDD[(Region, Iterable[R2])] =
    rdd.joinWithinRange(that, range).groupByKey()

  /**
   * Calculates windows of a fixed width that tile the entire genome, and then counts how many
   * elements of the input RDD overlap each window.
   *
   * @param seqDict The dictionary which defines the sequences and lengths of the genome, used for
   *                defining the set of windows over which counts are to be calculated.
   * @param windowSize The size of the window (in base-pairs)
   * @return An RDD of (window, count) pairs
   */
  def windowCounts(seqDict: SequenceDictionary, windowSize: Long): RDD[(ReferenceRegion, Int)] =
    new Coverage(windowSize).getAllWindows(rdd.sparkContext, seqDict)
      .joinByOverlap(rdd)
      .groupByKey()
      .map {
        case (window: ReferenceRegion, values: Iterable[Region]) =>
          (window, values.size)
      }

  private val coverage = new Coverage(100)

  def spatialUnion[R2 <: ReferenceRegion](other: RDD[R2])(implicit r2: ClassTag[R2]): RDD[ReferenceRegion] = {
    val rdd1: RDD[ReferenceRegion] = rdd.map(_.asInstanceOf[ReferenceRegion])
    val rdd2: RDD[ReferenceRegion] = other.map(_.asInstanceOf[ReferenceRegion])
    coverage.findCoverageRegions(rdd1.union(rdd2))
  }

  def spatialIntersection[R2 <: ReferenceRegion](other: RDD[R2])(implicit r2: ClassTag[R2]): RDD[ReferenceRegion] = {
    val joined: RDD[ReferenceRegion] = rdd.joinByOverlap(other).flatMap(p => RegionRDDFunctions.intersect(p._1, p._2))
    coverage.findCoverageRegions(joined)
  }

  def spatialDifference[R2 <: ReferenceRegion](other: RDD[R2])(implicit r2: ClassTag[R2]): RDD[ReferenceRegion] = {
    ???
  }
}

class OrientedRegionRDDFunctions[Region <: ReferenceRegionWithOrientation](rdd: RDD[Region])(implicit kt: ClassTag[Region]) extends Serializable {

  def extend(targetLength: Long): RDD[ReferenceRegionWithOrientation] =
    rdd.map(r => r.extend(targetLength))
}

class RegionKeyedRDDFunctions[R <: ReferenceRegion, V](rdd: RDD[(R, V)])(implicit rt: ClassTag[R], vt: ClassTag[V]) extends Serializable {

  import RegionRDDFunctions._

  def joinByOverlap[R2 <: ReferenceRegion, V2](that: RDD[(R2, V2)])(implicit v2t: ClassTag[V2]): RDD[(V, V2)] =
    BroadcastRegionJoin.partitionAndJoin(rdd, that)

  def filterWithinRange[R2 <: ReferenceRegion, V2](range: Long, that: RDD[(R2, V2)])(implicit v2t: ClassTag[V2]): RDD[(R, V)] =
    rdd.keyBy(rpair => expander(range)(rpair._1))
      .joinByOverlap(that)
      .map(_._1)
      .distinct()
}

object RegionRDDFunctions extends Serializable {

  /**
   * Given a size, produces an 'expander function,' which translates ReferenceRegion values
   * into ReferenceRegions that are larger (on each end) by the given size. The start of the expanded
   * region will never be negative -- although the end of the expanded region is unbounded.
   *
   * @param range The size of the expansion, on one end (if this is N, the ReferenceRegions produced
   *              by the expander function will be at most 2N bases larger).
   * @param r The region argument to the expander function.
   * @tparam Region The expander function has an argument type parameter, which must be a subtype of ReferenceRegion
   * @return The expanded region.
   */
  def expander[Region <: ReferenceRegion](range: Long)(r: Region): ReferenceRegion =
    ReferenceRegion(r.referenceName, max(0, r.start - range), r.end + range)

  def intersect[R1 <: ReferenceRegion, R2 <: ReferenceRegion](r1: R1, r2: R2): Option[ReferenceRegion] =
    if (r1.overlaps(r2)) {
      Some(r1.intersection(r2))
    } else {
      None
    }

  implicit def rddToRegionRDDFunctions[R <: ReferenceRegion](rdd: RDD[R])(implicit kt: ClassTag[R]): RegionRDDFunctions[R] =
    new RegionRDDFunctions[R](rdd)
  implicit def rddToOrientedRegionRDDFunctions[R <: ReferenceRegionWithOrientation](rdd: RDD[R])(implicit kt: ClassTag[R]): OrientedRegionRDDFunctions[R] =
    new OrientedRegionRDDFunctions[R](rdd)
  implicit def rddToRegionKeyedRDDFunctions[R <: ReferenceRegion, V](rdd: RDD[(R, V)])(implicit kt: ClassTag[R], vt: ClassTag[V]): RegionKeyedRDDFunctions[R, V] =
    new RegionKeyedRDDFunctions[R, V](rdd)
}
