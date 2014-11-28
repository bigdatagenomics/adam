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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.math._
import org.bdgenomics.adam.models.{ ReferenceRegionContext, ReferenceRegionOrdering, SequenceDictionary, ReferenceRegion }
import PairingRDD._
import ReferenceRegionContext._

/**
 * A base is 'covered' by a region set if any region in the set contains the base itself.
 *
 * The 'coverage regions' of a region set are the unique, disjoint, non-adjacent,
 * minimal set of regions which contain every covered base, and no bases which are not covered.
 *
 * The Coverage class calculates the coverage regions for a given region set.
 *
 * @param window A parameter (which should be a positive number) that determines the parallelism
 *               which Coverage uses to calculate the coverage regions -- larger window sizes
 *               indicate less parallelism, but also fewer subsequent passes.
 */
class Coverage(val window: Long) extends Serializable {

  require(window > 0)

  type Region = ReferenceRegion

  /**
   * Calling findCoverageRegions calculates (as an RDD) the coverage regions for a given
   * RDD of input regions.
   *
   * The primary method.
   *
   * @param coveringRegions The input regions whose coverage regions are to be calculated
   * @return an RDD containing the ReferenceRegions corresponding to the coverage regions
   *         of the input set 'coveringRegions'
   */
  def findCoverageRegions(coveringRegions: RDD[ReferenceRegion]): RDD[ReferenceRegion] = {

    // First, map each input region to a window
    val windowKeyedRegions: RDD[(Region, Region)] = coveringRegions.flatMap(regionToWindows)

    // Then, within each window, calculate the coverage regions.  This complete list
    // might contain pairs of regions that are adjacent (i.e. adjacent at the window
    // boundaries), therefore we ...
    val possiblyAdjacent: RDD[Region] = windowKeyedRegions.groupByKey().flatMap {
      case (window: Region, cRegions: Iterable[Region]) =>
        calculateCoverageRegions(cRegions)
    }

    // ... collapse the adjacent regions down into single contiguous regions.
    collapseAdjacent(possiblyAdjacent)
  }

  /**
   * Uses the fixed window-width to key each Region by the corresponding window Region
   * to which it belongs (through overlap).  Since a Region can overlap several windows,
   * there may be >1 value in the resulting Seq.
   *
   * @param region An input Region which is to be keyed to 1 or more windows.
   * @return A Seq of Region pairs, where the first element of each pair is one of the windows
   *         (of fixed-width) and the second element is the input Region
   */
  def regionToWindows(region: ReferenceRegion): Seq[(Region, Region)] = {
    val windowStart = region.start / window
    val windowEnd = region.end / window

    (windowStart to windowEnd).map {
      case (widx: Long) =>
        val wstart = widx * window
        val wend = wstart + window
        val wRegion = ReferenceRegion(region.referenceName, wstart, wend)
        val clippedRegion = ReferenceRegion(region.referenceName, max(wstart, region.start), min(wend, region.end))
        (wRegion, clippedRegion)
    }
  }

  def optionOrdering(or1: Option[Region], or2: Option[Region]): Int =
    (or1, or2) match {
      case (None, None)         => 0
      case (None, Some(r2))     => -1
      case (Some(r1), None)     => 1
      case (Some(r1), Some(r2)) => ReferenceRegionOrdering.compare(r1, r2)
    }

  case class OrientedPoint(chrom: String, pos: Long, polarity: Boolean) extends Ordered[OrientedPoint] with Serializable {
    override def compare(that: OrientedPoint): Int = {
      if (chrom != that.chrom) {
        chrom.compare(that.chrom)
      } else {
        val c1 = pos.compare(that.pos)
        if (c1 != 0) {
          c1
        } else {
          // we actually want the *reverse* ordering from the Java Boolean.compareTo
          // function!
          // c.f. https://docs.oracle.com/javase/7/docs/api/java/lang/Boolean.html#compareTo(java.lang.Boolean)
          -polarity.compare(that.polarity)
        }
      }
    }
  }

  /**
   * This is a helper function for findCoverageRegions -- basically, it takes a set
   * of input ReferenceRegions, it finds all pairs of regions that are adjacent to each
   * other (i.e. pairs (r1, r2) where r1.end == r2.start and r1.referenceName == r2.referenceName),
   * and it collapses all such adjacent regions into single contiguous regions.
   *
   * @param regions The input regions set; we assume that this input set is non-overlapping
   *                (that no two regions in the input set overlap each other)
   * @return The collapsed set of regions -- no two regions in the returned RDD should be
   *         adjacent, all should be at least one base-pair apart (or on separate
   *         chromosomes).
   */
  def collapseAdjacent(regions: RDD[Region]): RDD[Region] = {

    val pairs = regions.sortBy(p => p).pairWithEnds()

    val points: RDD[OrientedPoint] = pairs.flatMap {
      case (None, Some(region)) =>
        Seq(OrientedPoint(region.referenceName, region.start, true))
      case (Some(region), None) =>
        Seq(OrientedPoint(region.referenceName, region.end, false))
      case (Some(r1), Some(r2)) =>
        if (r1.isAdjacent(r2)) {
          Seq()
        } else {
          Seq(
            OrientedPoint(r1.referenceName, r1.end, false),
            OrientedPoint(r2.referenceName, r2.start, true))
        }
    }
    val paired = points.pair()
    val pairedAndFiltered = paired.filter(p =>
      p._1.chrom == p._2.chrom && p._1.polarity && p._2.pos - p._1.pos >= 0)

    pairedAndFiltered.map {
      case (p1: OrientedPoint, p2: OrientedPoint) => ReferenceRegion(p1.chrom, p1.pos, p2.pos)
    }
  }

  def getAllWindows(sc: SparkContext, dict: SequenceDictionary): RDD[ReferenceRegion] = {

    val chromRegions: RDD[ReferenceRegion] = sc.parallelize(
      dict.records.toSeq.map {
        case seqRecord =>
          ReferenceRegion(seqRecord.name, 0, seqRecord.length)
      })

    val windowRegions: RDD[ReferenceRegion] = chromRegions.flatMap {
      case chromRegion =>
        (0 until chromRegion.length().toInt by window.toInt).map { start =>
          ReferenceRegion(chromRegion.referenceName, start, start + window)
        }
    }

    windowRegions
  }

  def calculateCoverageRegions(regions: Iterable[ReferenceRegion]): Iterator[ReferenceRegion] =
    calculateCoverageRegions(regions.iterator)

  /**
   * Calculates the coverage regions for an input set -- note that this input set is an
   * Iterable, not an RDD.  This is the method which we call on each individual partition
   * of the RDD, in order to calculate an initial set of disjoint-but-possibly-adjacent
   * regions within the partition.
   *
   * @param regions The input set of ReferenceRegion objects
   * @return The 'coverage regions' of the input set
   */
  def calculateCoverageRegions(regions: Iterator[ReferenceRegion]): Iterator[ReferenceRegion] = {
    if (regions.isEmpty) {
      Iterator()

    } else {
      val sregions = regions.toArray.sorted
      if (sregions.size == 1) {
        return sregions.iterator
      }

      // We're calculating the 'coverage regions' here.
      // We do this in a few steps:
      // 1. sort the regions in lexicographic (seq-start-end) order -- this happened above.
      //    let the conceptual variables STARTS and ENDS be two arrays, each of len(regions),
      //    which contain the .start and .end fields of the (ordered) regions.
      // 2. Next, we calculate an array of length len(regions), called MAXENDS, where
      //    MAXENDS(i) = max(ENDS[0:i-1])
      // 3. Now, for any index i, if STARTS(i) > MAXENDS(i), then we call region i a
      //    'split' region -- a region that doesn't overlap any region that came before it,
      //    and which _starts_ a 'coverage region.' We calculate the set
      //       SPLITS = { i : STARTS(i) > MAXENDS(i) }
      // 4. Finally, we pair the splits -- each pair of splits corresponds to a single,
      //    contiguous coverage region.

      // TODO:
      // Calculating the MAXENDS and SPLITS sets in two passes here, although we could probably
      // do it in one if we really thought about it...
      val maxEnds: Array[Long] = sregions.map(_.end).scanLeft(0L)(max)
      val splitIndices: Seq[Int] =
        0 +:
          (1 until sregions.size).filter(i => sregions(i).start > maxEnds(i)) :+
          sregions.size

      //
      splitIndices.sliding(2).map {
        case Vector(i1, i2) =>
          ReferenceRegion(sregions(i1).referenceName, sregions(i1).start, maxEnds(i2))
      }.toIterator
    }
  }

}
