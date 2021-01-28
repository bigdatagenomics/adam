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

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.ds.variant.VariantDataset
import scala.annotation.tailrec
import scala.math.{ max, min }

/**
 * A table containing all of the SNPs in a known variation dataset.
 *
 * @param indices A map of reference names to the (first, last) index in the
 *   site array that contain data from this reference.
 * @param sites An array containing positions that have masked SNPs. Sorted by
 *   reference name and then position.
 */
class SnpTable private[models] (
    private[models] val indices: Map[String, (Int, Int)],
    private[models] val sites: Array[Long]) extends Serializable {

  private val midpoints: Map[String, Int] = {
    @tailrec def pow2ceil(length: Int, i: Int = 1): Int = {
      if (2 * i >= length) {
        i
      } else {
        pow2ceil(length, 2 * i)
      }
    }

    indices.mapValues(p => {
      val (start, end) = p
      pow2ceil(end - start + 1)
    })
  }

  @tailrec private def binarySearch(rr: ReferenceRegion,
                                    offset: Int,
                                    length: Int,
                                    step: Int,
                                    idx: Int = 0): Option[Int] = {
    if (length == 0) {
      None
    } else if (rr.start <= sites(offset + idx) && rr.end > sites(offset + idx)) {
      // if we've satistfied this last condition, then the read is overlapping the
      // current index and we have a hit
      Some(offset + idx)
    } else if (step == 0) {
      None
    } else {
      val stepIdx = idx + step
      val nextIdx: Int = if (stepIdx >= length ||
        rr.end <= sites(offset + stepIdx)) {
        idx
      } else {
        stepIdx
      }
      binarySearch(rr, offset, length, step / 2, nextIdx)
    }
  }

  @tailrec private def extendForward(rr: ReferenceRegion,
                                     offset: Int,
                                     idx: Int,
                                     list: List[Long] = List.empty): List[Long] = {
    if (idx < offset) {
      list
    } else {
      if (rr.start > sites(idx)) {
        list
      } else {
        extendForward(rr, offset, idx - 1, sites(idx) :: list)
      }
    }
  }

  @tailrec private def extendBackwards(rr: ReferenceRegion,
                                       end: Int,
                                       idx: Int,
                                       list: List[Long]): Set[Long] = {
    if (idx > end) {
      list.toSet
    } else {
      if (rr.end <= sites(idx)) {
        list.toSet
      } else {
        extendBackwards(rr, end, idx + 1, sites(idx) :: list)
      }
    }
  }

  /**
   * Is there a known SNP at the reference location of this Residue?
   */
  private[adam] def maskedSites(rr: ReferenceRegion): Set[Long] = {
    val optRange = indices.get(rr.referenceName)

    optRange.flatMap(range => {
      val (offset, end) = range
      val optIdx = binarySearch(rr, offset, end - offset + 1, midpoints(rr.referenceName))

      optIdx.map(idx => {
        extendBackwards(rr, end, idx + 1, extendForward(rr, offset, idx))
          .map(_.toLong)
      })
    }).getOrElse(Set.empty)
  }
}

/**
 * Companion object with helper functions for building SNP tables.
 */
object SnpTable {

  /**
   * Creates an empty SNP Table.
   *
   * @return An empty SNP table.
   */
  def apply(): SnpTable = {
    new SnpTable(Map.empty,
      Array.empty)
  }

  /**
   * Creates a SNP Table from a VariantDataset.
   *
   * @param variants The variants to populate the table from.
   * @return Returns a new SNPTable containing the input variants.
   */
  def apply(variants: VariantDataset): SnpTable = {
    val (indices, positions) = {
      val sortedVariants = variants.sort()
        .rdd
        .cache()

      val referenceIndices = sortedVariants.map(_.getReferenceName)
        .zipWithIndex
        .mapValues(v => (v.toInt, v.toInt))
        .reduceByKeyLocally((p1, p2) => {
          (min(p1._1, p2._1), max(p1._2, p2._2))
        }).toMap
      val sites = sortedVariants.map(_.getStart: Long).collect()

      // unpersist the cached variants
      sortedVariants.unpersist()

      (referenceIndices, sites)
    }
    new SnpTable(indices, positions)
  }
}

private[adam] class SnpTableSerializer extends Serializer[SnpTable] {

  def write(kryo: Kryo, output: Output, obj: SnpTable) {
    output.writeInt(obj.indices.size)
    obj.indices.foreach(kv => {
      val (referenceName, (lowerBound, upperBound)) = kv
      output.writeString(referenceName)
      output.writeInt(lowerBound)
      output.writeInt(upperBound)
    })
    output.writeInt(obj.sites.length)
    obj.sites.foreach(output.writeLong(_))
  }

  def read(kryo: Kryo, input: Input, klazz: Class[SnpTable]): SnpTable = {
    val indicesSize = input.readInt()
    val indices = new Array[(String, (Int, Int))](indicesSize)
    (0 until indicesSize).foreach(i => {
      indices(i) = (input.readString(), (input.readInt(), input.readInt()))
    })
    val sitesSize = input.readInt()
    val sites = new Array[Long](sitesSize)
    (0 until sitesSize).foreach(i => {
      sites(i) = input.readLong()
    })
    new SnpTable(indices.toMap, sites)
  }
}
