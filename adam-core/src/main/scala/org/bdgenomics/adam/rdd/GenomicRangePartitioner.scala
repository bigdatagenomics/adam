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

import java.util.Arrays
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  RegionOrdering,
  SequenceDictionary
}
import scala.annotation.tailrec
import scala.reflect.ClassTag

private[rdd] object GenomicRangePartitioner {

  private def getHeads[V](rdd: RDD[(ReferenceRegion, V)]): Array[ReferenceRegion] = {
    rdd.mapPartitionsWithIndex((idx, iter) => {
      iter.take(1).map(p => (idx, p._1))
    }).collect
      .toSeq
      .sortBy(_._1)
      .tail
      .map(_._2)
      .toArray
  }

  def isLexSorted(heads: Array[ReferenceRegion]): Boolean = {
    if (heads.size > 1) {
      heads.sliding(2)
        .forall(p => {
          p(0).compareTo(p(1)) <= 0
        })
    } else {
      true
    }
  }

  def partitionerIsCompatibleWithFlankSize(
    flankSize: Int,
    partitioner: Partitioner): Boolean = {
    if (partitioner.numPartitions <= 1) {
      true
    } else {
      partitioner match {
        case LexicographicalGenomicRangePartitioner(regions) => {
          regions.sliding(2)
            .forall(p => {
              p(0).referenceName == p(1).referenceName &&
                (p(1).start - p(0).start) > flankSize
            })
        }
        case IndexedGenomicRangePartitioner(indices, _) => {
          indices.sliding(2)
            .forall(p => {
              p(0)._1 == p(1)._1 &&
                (p(1)._1 - p(0)._1) > flankSize
            })
        }
        case _ => false
      }
    }
  }

  /**
   * Creates a genomic range partitioner from a sorted RDD.
   *
   * Detects whether the RDD is lexicographically ordered or sorted by contig
   * index.
   *
   * @param rdd The sorted RDD to infer the partitioner from.
   * @param sequences The sequences this RDD is sorted against.
   * @return Returns the genomic partitioner inferred from this RDD.
   */
  def fromRdd[V](rdd: RDD[(ReferenceRegion, V)],
                 sequences: SequenceDictionary): GenomicRangePartitioner[_] = {

    val heads = getHeads(rdd)

    if (isLexSorted(heads)) {
      LexicographicalGenomicRangePartitioner(heads)
    } else {
      IndexedGenomicRangePartitioner(heads,
        sequences)
    }
  }
}

private[rdd] case class LexicographicalGenomicRangePartitioner(
    rangeBounds: Array[ReferenceRegion]) extends GenomicRangePartitioner[ReferenceRegion] {

  protected val ordering = RegionOrdering

  override def toString: String = {
    "LexicographicalGenomicRangePartitioner(Array(%s))".format(
      rangeBounds.mkString(", "))
  }

  @tailrec private def findOverlappingPartitions(
    idx: Int,
    rr: ReferenceRegion,
    partitions: List[Int]): Iterable[Int] = {

    if (idx >= rangeBounds.length ||
      !rangeBounds(idx).overlaps(rr)) {
      partitions.toIterable
    } else {
      findOverlappingPartitions(idx + 1,
        rr,
        (idx + 1) :: partitions)
    }
  }

  protected def internalPartitionsForRegion(
    rr: ReferenceRegion): Iterable[Int] = {

    // binarySearch either returns the match location or -[insertion point]-1
    // get partition will find the first partition containing the key
    val bucket = binarySearch(rr)
    val partition = cleanBinarySearchResult(bucket)

    if (bucket < 0 &&
      partition < rangeBounds.length &&
      rr.overlaps(rangeBounds(partition))) {
      findOverlappingPartitions(partition + 1,
        rr,
        List(partition, partition + 1))
    } else {
      Iterable(partition)
    }
  }
}

private object IndexedGenomicRangePartitioner {

  def apply(rangeBounds: Array[ReferenceRegion],
            sequences: SequenceDictionary): IndexedGenomicRangePartitioner = {
    require(sequences.hasSequenceOrdering)

    IndexedGenomicRangePartitioner(rangeBounds.map(rr => {
      (sequences(rr.referenceName).getOrElse({
        throw new IllegalArgumentException("Did not find %s in %s.".format(
          rr.referenceName, sequences))
      }).referenceIndex.get, rr.start)
    }), sequences)
  }
}

private[rdd] case class IndexedGenomicRangePartitioner(
    rangeBounds: Array[(Int, Long)],
    sequences: SequenceDictionary) extends GenomicRangePartitioner[(Int, Long)] {

  protected val ordering = Ordering[(Int, Long)]

  private def overlaps(sequenceIdx: Int,
                       rr: ReferenceRegion,
                       idx: Int): Boolean = {
    val (partitionIdx, partitionStart) = rangeBounds(idx)

    (partitionIdx == sequenceIdx &&
      partitionStart >= rr.start &&
      partitionStart < rr.end)
  }

  @tailrec private def findOverlappingPartitions(
    idx: Int,
    rr: ReferenceRegion,
    sequenceIdx: Int,
    partitions: List[Int]): Iterable[Int] = {

    if (idx >= rangeBounds.length) {
      partitions.toIterable
    } else {
      if (overlaps(sequenceIdx, rr, idx)) {
        partitions.toIterable
      } else {
        findOverlappingPartitions(idx + 1,
          rr,
          sequenceIdx,
          (idx + 1) :: partitions)
      }
    }
  }

  protected def internalPartitionsForRegion(
    rr: ReferenceRegion): Iterable[Int] = {

    sequences(rr.referenceName)
      .flatMap(_.referenceIndex)
      .fold(Iterable.empty[Int])(sequenceIdx => {

        // binarySearch either returns the match location or -[insertion point]-1
        // get partition will find the first partition containing the key
        val bucket = binarySearch((sequenceIdx, rr.start))
        val partition = cleanBinarySearchResult(bucket)

        if (bucket < 0 &&
          partition < rangeBounds.length &&
          overlaps(sequenceIdx, rr, partition)) {
          findOverlappingPartitions(partition + 1,
            rr,
            sequenceIdx,
            List(partition, partition + 1))
        } else {
          Iterable(partition)
        }
      })
  }
}

/**
 * This is almost entirely lifted from Spark's RangePartitioner class.
 *
 * Alas, it could've been eliminated entirely if they'd made `rangeBounds`
 * protected instead of private.
 */
sealed trait GenomicRangePartitioner[K] extends Partitioner {

  protected val ordering: Ordering[K]

  protected val binarySearch: K => Int = {
    def binSearch(x: K): Int = {
      Arrays.binarySearch(rangeBounds.asInstanceOf[Array[AnyRef]],
        x,
        ordering.asInstanceOf[java.util.Comparator[Any]])
    }
    binSearch(_)
  }

  def numPartitions: Int = rangeBounds.length + 1

  def partitionsForRegion(rr: ReferenceRegion): Iterable[Int] = {
    if (rangeBounds.isEmpty) {
      Iterable(0)
    } else {
      internalPartitionsForRegion(rr)
    }
  }

  protected def internalPartitionsForRegion(rr: ReferenceRegion): Iterable[Int]

  def copartitionAgainst[T](
    rdd: RDD[(ReferenceRegion, T)])(
      implicit tTag: ClassTag[T]): RDD[(ReferenceRegion, T)] = {
    val outputPartitioner = ManualRegionPartitioner(numPartitions)

    rdd.flatMap(kv => {
      val (rr, v) = kv
      val idxs = partitionsForRegion(rr)

      idxs.map(idx => {
        ((rr, idx), v)
      })
    }).repartitionAndSortWithinPartitions(outputPartitioner)
      .map(kv => {
        val ((rr, _), v) = kv
        (rr, v)
      })
  }

  private val isSmall: Boolean = rangeBounds.length <= 128

  protected val rangeBounds: Array[K]

  final def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    if (isSmall) {
      var partition = 0

      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }

      partition
    } else {
      cleanBinarySearchResult(binarySearch(k))
    }
  }

  protected def cleanBinarySearchResult(partition: Int): Int = {
    // binarySearch either returns the match location or -[insertion point]-1
    if (partition < 0) {
      -partition - 1
    } else if (partition > rangeBounds.length) {
      rangeBounds.length
    } else {
      partition
    }
  }
}
