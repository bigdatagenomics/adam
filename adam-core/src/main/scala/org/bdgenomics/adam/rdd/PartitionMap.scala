package org.bdgenomics.adam.rdd

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.utils.interval.array.IntervalArray

private[rdd] object PartitionMap {

  def apply[T, U <: GenomicRDD[T, U]](genomicRdd: GenomicRDD[T, U]): PartitionMap = {
    if (!genomicRdd.isSorted) {
      PartitionMap(None)
    } else {
      PartitionMap(Some(genomicRdd.flattenRddByRegions().mapPartitions(iter => {
        getRegionBoundsFromPartition(iter)
      }).collect))
    }
  }

  def apply[T](rdd: RDD[(ReferenceRegion, T)]): PartitionMap = {
    PartitionMap(Some(rdd.mapPartitions(iter => {
      getRegionBoundsFromPartition(iter)
    }).collect))
  }

  /**
   * Gets the partition bounds from a ReferenceRegion keyed Iterator.
   *
   * @param iter The data on a given partition. ReferenceRegion keyed.
   * @return The bounds of the ReferenceRegions on that partition, in an Iterator.
   */
  private def getRegionBoundsFromPartition[T](
    iter: Iterator[(ReferenceRegion, T)]): Iterator[Option[(ReferenceRegion, ReferenceRegion)]] = {

    if (iter.isEmpty) {
      // This means that there is no data on the partition, so we have no bounds
      Iterator(None)
    } else {
      val firstRegion = iter.next
      val lastRegion =
        if (iter.hasNext) {
          // we have to make sure we get the full bounds of this partition, this
          // includes any extremely long regions. we include the firstRegion for
          // the case that the first region is extremely long
          (iter ++ Iterator(firstRegion)).maxBy(f => (f._1.referenceName, f._1.end, f._1.start))
        } else {
          // only one record on this partition, so this is the extent of the bounds
          firstRegion
        }
      Iterator(Some((firstRegion._1, lastRegion._1)))
    }
  }
}

/**
 * The partition map is structured as follows:
 * The outer option is for whether or not there is a partition map.
 *   - This is None in the case that we don't know the bounds on each
 *     partition.
 * The Array is the length of the number of partitions.
 * The inner option is in case there is no data on a partition.
 * The (ReferenceRegion, ReferenceRegion) tuple contains the bounds of the
 *   partition, such that the lowest start is first and the highest end is
 *   second.
 *
 * @param optPartitionMap An optional PartitionMap, format described above.
 */
private[rdd] case class PartitionMap(
    private val optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) {

  lazy val get: Array[Option[(ReferenceRegion, ReferenceRegion)]] = {
    optPartitionMap.get
  }

  def isEmpty: Boolean = {
    optPartitionMap.isEmpty
  }

  def isDefined: Boolean = {
    optPartitionMap.isDefined
  }

  def exists(f: (Array[Option[(ReferenceRegion, ReferenceRegion)]]) => Boolean): Boolean = {
    optPartitionMap.exists(f)
  }

  def length: Int = {
    get.length
  }

  def indices: Range = {
    get.indices
  }

  def toIntervalArray(): IntervalArray[ReferenceRegion, Int] = {

    val adjustedPartitionMapWithIndex =
      // the zipWithIndex gives us the destination partition ID
      get.zipWithIndex
        .filter(_._1.nonEmpty)
        .map(f => (f._1.get, f._2)).map(g => {
          // first region for the bound
          val rr = g._1._1
          // second region for the bound
          val secondrr = g._1._2
          // in the case where we span multiple referenceNames
          if (rr.referenceName != g._1._2.referenceName) {
            // create a ReferenceRegion that goes to the end of the chromosome
            (ReferenceRegion(rr.referenceName, rr.start, rr.end), g._2)
          } else {
            // otherwise we just have the ReferenceRegion span from partition
            // start to end
            (ReferenceRegion(rr.referenceName, rr.start, secondrr.end), g._2)
          }
        })

    // we use an interval array to quickly look up the destination partitions
    IntervalArray(
      adjustedPartitionMapWithIndex,
      adjustedPartitionMapWithIndex.maxBy(_._1.width)._1.width,
      sorted = true)
  }
}

