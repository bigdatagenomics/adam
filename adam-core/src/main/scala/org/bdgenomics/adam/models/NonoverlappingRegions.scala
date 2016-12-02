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
package org.bdgenomics.adam.models

/**
 * The evaluation of a regionJoin takes place with respect to a complete partition on the total space
 * of the genome.  NonoverlappingRegions is a class to compute the value of that partition, and to allow
 * us to assign one or more elements of that partition to a new ReferenceRegion (see the 'regionsFor' method).
 *
 * NonoverlappingRegions takes, as input, and 'input-set' of regions.  These are arbitrary ReferenceRegions,
 * which may be overlapping, identical, disjoint, etc.  The input-set of regions _must_ all be located on
 * the same reference chromosome (i.e. must all have the same refName); the generalization to reference
 * regions from multiple chromosomes is in MultiContigNonoverlappingRegions, below.
 *
 * NonoverlappingRegions produces, internally, a 'nonoverlapping-set' of regions.  This is basically
 * the set of _distinct unions_ of the input-set regions.
 *
 * @param regions The input-set of regions.
 */
private[adam] class NonoverlappingRegions(regions: Iterable[ReferenceRegion]) extends Serializable {

  assert(regions != null, "regions parameter cannot be null")

  // The regions Seq needs to be of non-zero size, since otherwise we have to add special-case
  // checks to all the methods below to make sure that 'endpoints' isn't empty.  Also, it shouldn't
  // make any sense to have a set of non-overlapping regions for ... no regions.
  // Also, without this check, we can't tell which chromosome this NonoverlappingRegions object is for.
  assert(regions.nonEmpty, "regions list must be non-empty")
  assert(regions.head != null, "regions must have at least one non-null entry")

  /**
   * The name of the reference contig this covers.
   */
  val referenceName: String = regions.head.referenceName

  // invariant: all the values in the 'regions' list have the same referenceId
  assert(regions.forall(_.referenceName == referenceName))

  /**
   * We represent the distinct unions, the 'nonoverlapping-set' of regions, as a
   * set of endpoints, so that we can do reasonably-fast binary searching on
   * them to determine the slice of nonoverlapping-set regions that are
   * overlapped by a new, query region (see findOverlappingRegions, below).
   */
  val endpoints: Array[Long] =
    mergeRegions(regions.toSeq.sortBy(r => r.start)).flatMap(r => Seq(r.start, r.end)).distinct.sorted.toArray

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

  private def mergeRegions(regs: Seq[(ReferenceRegion)]): List[ReferenceRegion] =
    regs.aggregate(List[ReferenceRegion]())(
      (lst: List[ReferenceRegion], p: (ReferenceRegion)) => updateListWithRegion(lst, p),
      (a, b) => a ++ b
    )

  private def binaryPointSearch(pos: Long, lessThan: Boolean): Int = {
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

  /**
   * Finds all regions that a query region overlaps.
   *
   * @param query The region to check for overlaps.
   * @return All the regions that overlap the query region.
   */
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
          ReferenceRegion(referenceName, start, end)
      }.toSeq, firstRegionIsHit)
    }
  }

  /**
   * Given a "regionable" value (corresponds to a ReferencRegion through an implicit ReferenceMapping),
   * return the set of nonoverlapping-regions to be used as partitions for the input value in a
   * region-join.  Basically, return the set of any non-empty nonoverlapping-regions that overlap the
   * region corresponding to this input.
   *
   * @param regionable The input, which corresponds to a region
   * @tparam U The type of the input
   * @return An Iterable[ReferenceRegion], where each element of the Iterable is a nonoverlapping-region
   *         defined by 1 or more input-set regions.
   */
  def regionsFor[U](regionable: (ReferenceRegion, U)): Iterable[ReferenceRegion] =
    findOverlappingRegions(regionable._1)

  /**
   * A quick filter, to find out if we even need to examine a particular input value for keying by
   * nonoverlapping-regions.  Basically, reject the input value if its corresponding region is
   * completely outside the hull of all the input-set regions.
   *
   * @param regionable The input value
   * @tparam U The type of the value in the key-value tuple.
   * @return True if we have a region that overlaps the region key in a tuple.
   */
  def hasRegionsFor[U](regionable: (ReferenceRegion, U)): Boolean = {
    !(regionable._1.end <= endpoints.head || regionable._1.start >= endpoints.last)
  }

  override def toString: String =
    "%s:%d-%d (%s)".format(referenceName, endpoints.head, endpoints.last, endpoints.mkString(","))
}

private[adam] object NonoverlappingRegions {

  /**
   * Builds a non-overlapping contig map from a seq of region tagged data.
   *
   * @param values A seq of data which is keyed by the region it overlaps.
   * @return Returns a nonoverlapping region map.
   *
   * @tparam T The type of the value in the input seq.
   */
  def apply[T](values: Seq[(ReferenceRegion, T)]) =
    new NonoverlappingRegions(values.map(_._1))

  private[models] def alternating[T](seq: Seq[T], includeFirst: Boolean): Seq[T] = {
    val inds = if (includeFirst) { 0 until seq.size } else { 1 until seq.size + 1 }
    seq.zip(inds).filter(p => p._2 % 2 == 0).map(_._1)
  }
}

/**
 * Creates a multi-reference-region collection of NonoverlappingRegions -- see
 * the scaladocs to NonoverlappingRegions.
 *
 * @param regions A Seq of ReferencRegions, pre-partitioned by their
 *                referenceNames.  So, for a given pair (x, regs) in
 *                this Seq, all regions R in regs must satisfy
 *                R.referenceName == x.  Furthermore, all the x's must
 *                be valid reference names with respect to the sequence
 *                dictionary.
 */
private[adam] class MultiContigNonoverlappingRegions(
    regions: Seq[(String, Iterable[ReferenceRegion])]) extends Serializable {
  assert(
    regions != null,
    "Regions was set to null"
  )

  private val regionMap: Map[String, NonoverlappingRegions] =
    Map(regions.map(r => (r._1, new NonoverlappingRegions(r._2))): _*)

  /**
   * Given a "regionable" value (corresponds to a ReferencRegion through an implicit ReferenceMapping),
   * return the set of nonoverlapping-regions to be used as partitions for the input value in a
   * region-join.  Basically, return the set of any non-empty nonoverlapping-regions that overlap the
   * region corresponding to this input.
   *
   * @param regionable The input, which corresponds to a region
   * @tparam U The type of the input
   * @return An Iterable[ReferenceRegion], where each element of the Iterable is a nonoverlapping-region
   *         defined by 1 or more input-set regions.
   */
  def regionsFor[U](regionable: (ReferenceRegion, U)): Iterable[ReferenceRegion] =
    regionMap.get(regionable._1.referenceName).fold(Iterable[ReferenceRegion]())(_.regionsFor(regionable))

  /**
   * A quick filter, to find out if we even need to examine a particular input value for keying by
   * nonoverlapping-regions.  Basically, reject the input value if its corresponding region is
   * completely outside the hull of all the input-set regions.
   *
   * @param value The input value
   * @tparam U
   * @return a boolean -- the input value should only participate in the regionJoin if the return value
   *         here is 'true'.
   */
  def filter[U](value: (ReferenceRegion, U)): Boolean =
    regionMap.get(value._1.referenceName).fold(false)(_.hasRegionsFor(value))
}

private[adam] object MultiContigNonoverlappingRegions {

  /**
   * Builds a non-overlapping region map from a seq of region tagged data.
   *
   * Can cover multiple contigs.
   *
   * @param values A seq of data which is keyed by the region it overlaps.
   * @return Returns a nonoverlapping region map.
   *
   * @tparam T The type of the value in the input seq.
   */
  def apply[T](values: Seq[(ReferenceRegion, T)]): MultiContigNonoverlappingRegions = {
    new MultiContigNonoverlappingRegions(
      values.map(kv => (kv._1.referenceName, kv._1))
        .groupBy(t => t._1)
        .map(t => (t._1, t._2.map(k => k._2)))
        .toSeq
    )
  }
}

