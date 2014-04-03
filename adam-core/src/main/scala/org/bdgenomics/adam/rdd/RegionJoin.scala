/*
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceMapping, ReferenceRegion }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.Predef._
import org.apache.spark.SparkContext

/**
 * Contains multiple implementations of a 'region join', an operation that joins two sets of
 * regions based on the spatial overlap between the regions.
 *
 * Different implementations will have different performance characteristics -- and new implementations
 * will likely be added in the future, see the notes to each individual method for more details.
 */
object RegionJoin {

  /**
   * Performs a region join between two RDDs.
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
   * @param sc A SparkContext for the cluster that will perform the join
   * @param seqDict A SequenceDictionary -- every region corresponding to either the baseRDD or joinedRDD
   *                values must be mapped to a chromosome with an entry in this dictionary.
   * @param baseRDD The 'left' side of the join, a set of values which correspond (through an implicit
   *                ReferenceMapping) to regions on the genome.
   * @param joinedRDD The 'right' side of the join, a set of values which correspond (through an implicit
   *                  ReferenceMapping) to regions on the genome
   * @param tMapping implicit reference mapping for baseRDD regions
   * @param uMapping implicit reference mapping for joinedRDD regions
   * @param tManifest implicit type of baseRDD
   * @param uManifest implicit type of joinedRDD
   * @tparam T type of baseRDD
   * @tparam U type of joinedRDD
   * @return An RDD of pairs (x, y), where x is from baseRDD, y is from joinedRDD, and the region
   *         corresponding to x overlaps the region corresponding to y.
   */
  def partitionAndJoin[T, U](sc: SparkContext,
    seqDict: SequenceDictionary,
    baseRDD: RDD[T],
    joinedRDD: RDD[U])(implicit tMapping: ReferenceMapping[T],
      uMapping: ReferenceMapping[U],
      tManifest: ClassManifest[T],
      uManifest: ClassManifest[U]): RDD[(T, U)] = {

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

    // First, we group the regions in the left side of the join by their refId,
    // and collect them.
    val collectedLeft: Seq[(Int, Seq[ReferenceRegion])] =
      baseRDD
        .map(t => (tMapping.getReferenceId(t), tMapping.getReferenceRegion(t))) // RDD[(Int,ReferenceRegion)]
        .groupBy(_._1) // RDD[(Int,Seq[(Int,ReferenceRegion)])]
        .map(t => (t._1, t._2.map(_._2))) // RDD[(Int,Seq[ReferenceRegion])]
        .collect() // Iterable[(Int,Seq[ReferenceRegion])]
        .toSeq // Seq[(Int,Seq[ReferenceRegion])]

    // Next, we turn that into a data structure that reduces those regions to their non-overlapping
    // pieces, which we will use as a partition.
    val multiNonOverlapping = new MultiContigNonoverlappingRegions(seqDict, collectedLeft)

    // Then, we broadcast those partitions -- this will be the function that allows us to
    // partition all the regions on the right side of the join.
    val regions = sc.broadcast(multiNonOverlapping)

    // each element of the left-side RDD should have exactly one partition.
    val smallerKeyed: RDD[(ReferenceRegion, T)] =
      baseRDD.keyBy(t => regions.value.regionsFor(t).head)

    // each element of the right-side RDD may have 0, 1, or more than 1 corresponding partition.
    val largerKeyed: RDD[(ReferenceRegion, U)] =
      joinedRDD.filter(regions.value.filter(_))
        .flatMap(t => regions.value.regionsFor(t).map((r: ReferenceRegion) => (r, t)))

    // this is (essentially) performing a cartesian product within each partition...
    val joined: RDD[(ReferenceRegion, (T, U))] =
      smallerKeyed.join(largerKeyed)

    // ... so we need to filter the final pairs to make sure they're overlapping.
    val filtered: RDD[(ReferenceRegion, (T, U))] = joined.filter({
      case (rr: ReferenceRegion, (t: T, u: U)) =>
        tMapping.getReferenceRegion(t).overlaps(uMapping.getReferenceRegion(u))
    })

    // finally, erase the partition key and return the result.
    filtered.map(rrtu => rrtu._2)
  }

  /**
   * This method does a join between different types which can have a corresponding ReferenceMapping.
   *
   * This method does a cartesian product between the two, then removes mismatched regions.
   *
   * This is SLOW SLOW SLOW, and shouldn't be used for anything other than correctness-testing on
   * realistic sized sets.
   *
   */
  def cartesianFilter[T, U](baseRDD: RDD[T],
    joinedRDD: RDD[U])(implicit tMapping: ReferenceMapping[T],
      uMapping: ReferenceMapping[U],
      tManifest: ClassManifest[T],
      uManifest: ClassManifest[U]): RDD[(T, U)] = {
    baseRDD.cartesian(joinedRDD).filter({
      case (t: T, u: U) =>
        tMapping.getReferenceRegion(t).overlaps(uMapping.getReferenceRegion(u))
    })
  }
}

/**
 * The evaluation of a regionJoin takes place with respect to a complete partition on the total space
 * of the genome.  NonoverlappingRegions is a class to compute the value of that partition, and to allow
 * us to assign one or more elements of that partition to a new ReferenceRegion (see the 'regionsFor' method).
 *
 * NonoverlappingRegions takes, as input, and 'input-set' of regions.  These are arbitrary ReferenceRegions,
 * which may be overlapping, identical, disjoint, etc.  The input-set of regions _must_ all be located on
 * the same reference chromosome (i.e. must all have the same refId); the generalization to reference
 * regions from multiple chromosomes is in MultiContigNonoverlappingRegions, below.
 *
 * NonoverlappingRegions produces, internally, a 'nonoverlapping-set' of regions.  This is basically
 * the set of _distinct unions_ of the input-set regions.
 *
 * @param seqDict A SequenceDictionary; every region in the input-set will need to be located on a chromosome
 *                with an entry in this dictionary
 * @param regions The input-set of regions.
 */
class NonoverlappingRegions(seqDict: SequenceDictionary, regions: Seq[ReferenceRegion]) extends Serializable {

  assert(regions != null, "regions parameter cannot be null")

  // The regions Seq needs to be of non-zero size, since otherwise we have to add special-case
  // checks to all the methods below to make sure that 'endpoints' isn't empty.  Also, it shouldn't
  // make any sense to have a set of non-overlapping regions for ... no regions.
  // Also, without this check, we can't tell which chromosome this NonoverlappingRegions object is for.
  assert(regions.size > 0, "regions list must be non-empty")
  assert(regions.head != null, "regions must have at least one non-null entry")

  assert(seqDict != null, "Sequence Dictionary cannot be null")

  val referenceId: Int = regions.head.refId
  val referenceLength: Long = seqDict(referenceId).length

  // invariant: all the values in the 'regions' list have the same referenceId
  assert(regions.forall(_.refId == referenceId))

  // We represent the distinct unions, the 'nonoverlapping-set' of regions, as a set of endpoints,
  // so that we can do reasonably-fast binary searching on them to determine the slice of nonoverlapping-set
  // regions that are overlapped by a new, query region (see findOverlappingRegions, below).
  val endpoints: Array[Long] =
    mergeRegions(regions.sortBy(r => r.start)).flatMap(r => Seq(r.start, r.end)).distinct.sorted.toArray

  private def updateListWithRegion(list: List[ReferenceRegion], newRegion: ReferenceRegion): List[ReferenceRegion] = {
    list match {
      case head :: tail =>

        // using overlaps || isAdjacent is an important feature!  it means that
        // we can use the "alternating" optimization, described below, which reduces
        // the number of regions returned as keys (by a factor of 2) and therefore
        // the number of partitions that need to be examined during a regionJoin.
        if (head.overlaps(newRegion) || head.isAdjacent(newRegion)) {
          head.hull(newRegion) :: tail
        } else {
          newRegion :: list
        }
      case _ => List(newRegion)
    }
  }

  def mergeRegions(regs: Seq[(ReferenceRegion)]): List[ReferenceRegion] =
    regs.aggregate(List[ReferenceRegion]())(
      (lst: List[ReferenceRegion], p: (ReferenceRegion)) => updateListWithRegion(lst, p),
      (a, b) => a ++ b)

  def binaryPointSearch(pos: Long, lessThan: Boolean): Int = {
    var i = 0
    var j = endpoints.size - 1

    while (j - i > 1) {
      val ij2 = (i + j) / 2
      val mid = endpoints(ij2)
      if (mid < pos) {
        i = ij2
      } else {
        j = ij2
      }
    }

    if (lessThan) i else j
  }

  def findOverlappingRegions(query: ReferenceRegion): Seq[ReferenceRegion] = {

    assert(query != null, "query region was null")
    assert(endpoints != null, "endpoints field was null")

    if (query.end <= endpoints.head || query.start >= endpoints.last) {
      Seq()
    } else {
      val firsti = binaryPointSearch(query.start, lessThan = true)
      val lasti = binaryPointSearch(query.end, lessThan = false)

      // Slice is an inclusive start, exclusive end operation
      val firstRegionIsHit = firsti % 2 == 0

      val startSlice = endpoints.slice(firsti, lasti)
      val endSlice = endpoints.slice(firsti + 1, lasti + 1)

      /*
       * The use of NonoverlappingRegions.alternating is an important optimization --
       * basically, because we used "overlaps || isAdjacent" as the predicate for 
       * when to join two regions in the input-set into a single nonoverlapping-region, above,
       * then we know that the set of nonoverlapping-regions defined by the points in 'endpoints'
       * are "alternating." In other words, we know that each nonoverlapping-region
       * defined by an overlapping set of input-regions (in the constructor) is followed by
       * a nonoverlapping-region which had _no_ input-set regions within it, and vice-versa.
       *
       * And _this_ is important because it means that, here, we only need to return
       * _half_ the regions we otherwise would have -- because this is being used for a
       * regionJoin, we don't need to return the 'empty' regions since we know a priori
       * that these don't correspond to any region the input-set, and therefore
       * will never result in any pairs in the ultimate join result.
       */
      NonoverlappingRegions.alternating(startSlice.zip(endSlice).map {
        case (start, end) =>
          ReferenceRegion(referenceId, start, end)
      }.toSeq, firstRegionIsHit)
    }
  }

  /**
   * Given a "regionable" value (corresponds to a ReferencRegion through an implicit ReferenceMapping),
   * return the set of nonoverlapping-regions to be used as a partitions for the input value in a
   * region-join.  Basically, return the set of any non-empty nonoverlapping-regions that overlap the
   * region corresponding to this input.
   *
   * @param regionable The input, which corresponds to a region
   * @param mapping The implicit mapping to turn the input into a region
   * @tparam U The type of the input
   * @return An Iterable[ReferenceRegion], where each element of the Iterable is a nonoverlapping-region
   *         defined by 1 or more input-set regions.
   */
  def regionsFor[U](regionable: U)(implicit mapping: ReferenceMapping[U]): Iterable[ReferenceRegion] =
    findOverlappingRegions(mapping.getReferenceRegion(regionable))

  /**
   * A quick filter, to find out if we even need to examine a particular input value for keying by
   * nonoverlapping-regions.  Basically, reject the input value if its corresponding region is
   * completely outside the hull of all the input-set regions.
   *
   * @param regionable The input value
   * @param mapping an implicity mapping of the input value to a ReferenceRegion
   * @tparam U
   * @return a boolean -- the input value should only participate in the regionJoin if the return value
   *         here is 'true'.
   */
  def hasRegionsFor[U](regionable: U)(implicit mapping: ReferenceMapping[U]): Boolean = {
    val region = mapping.getReferenceRegion(regionable)
    !(region.end <= endpoints.head || region.start >= endpoints.last)
  }

  override def toString: String =
    "%d:%d-%d (%s)".format(referenceId, endpoints.head, endpoints.last, endpoints.mkString(","))
}

object NonoverlappingRegions {

  def apply[T](seqDict: SequenceDictionary, values: Seq[T])(implicit refMapping: ReferenceMapping[T]) =
    new NonoverlappingRegions(seqDict, values.map(value => refMapping.getReferenceRegion(value)))

  def alternating[T](seq: Seq[T], includeFirst: Boolean): Seq[T] = {
    val inds = if (includeFirst) { 0 until seq.size } else { 1 until seq.size + 1 }
    seq.zip(inds).filter(p => p._2 % 2 == 0).map(_._1)
  }

}

/**
 * Creates a multi-reference-region collection of NonoverlappingRegions -- see the scaladocs to
 * NonoverlappingRegions.
 *
 * @param seqDict A sequence dictionary for all the possible reference regions that could be aligned to.
 * @param regions A Seq of ReferencRegions, pre-partitioned by their refIds -- so, for a given pair
 *                (x, regs) in this Seq, all regions R in regs must satisfy R.refId == x.  Furthermore,
 *                all the x's must be valid refIds with respect to the sequence dictionary.
 */
class MultiContigNonoverlappingRegions(seqDict: SequenceDictionary, regions: Seq[(Int, Seq[ReferenceRegion])])
  extends Serializable {

  assert(regions != null,
    "Regions was set to null")

  assert(!regions.map(_._1).exists(!seqDict.containsRefId(_)),
    "SeqDict doesn't contain a refId from the regions sequence")

  val regionMap: Map[Int, NonoverlappingRegions] =
    Map(regions.map(r => (r._1, new NonoverlappingRegions(seqDict, r._2))): _*)

  def regionsFor[U](regionable: U)(implicit mapping: ReferenceMapping[U]): Iterable[ReferenceRegion] =
    regionMap.get(mapping.getReferenceId(regionable)) match {
      case None => Seq()
      case Some(nr) => nr.regionsFor(regionable)
    }

  def filter[U](value: U)(implicit mapping: ReferenceMapping[U]): Boolean =
    regionMap.get(mapping.getReferenceId(value)) match {
      case None => false
      case Some(nr) => nr.hasRegionsFor(value)
    }
}

object MultiContigNonoverlappingRegions {
  def apply[T](seqDict: SequenceDictionary, values: Seq[T])(implicit mapping: ReferenceMapping[T]): MultiContigNonoverlappingRegions = {
    new MultiContigNonoverlappingRegions(seqDict,
      values.map(v => (mapping.getReferenceId(v), mapping.getReferenceRegion(v)))
        .groupBy(t => t._1)
        .map(t => (t._1, t._2.map(k => k._2)))
        .toSeq)
  }
}

