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
package org.bdgenomics.adam.ds

import org.bdgenomics.adam.models.ReferenceRegion
import scala.math._

/**
 * Partition a genome into a set of bins.
 *
 * Note that this class will not tolerate invalid input, so filter in advance if you use it.
 *
 * @param binSize The size of each bin in nucleotides
 * @param seqLengths A map containing the length of each contig
 */
case class GenomeBins(binSize: Long, seqLengths: Map[String, Long]) extends Serializable {
  private val names: Seq[String] = seqLengths.keys.toSeq.sortWith(_ < _)
  private val lengths: Seq[Long] = names.map(seqLengths(_))
  private val parts: Seq[Int] = lengths.map(v => round(ceil(v.toDouble / binSize)).toInt)
  private val cumulParts: Seq[Int] = parts.scan(0)(_ + _)
  private val contig2cumulParts: Map[String, Int] = Map(names.zip(cumulParts): _*)

  /**
   * The total number of bins induced by this partition.
   */
  def numBins: Int = parts.sum

  /**
   * Get the bin number corresponding to a query ReferenceRegion.
   *
   * @param region the query ReferenceRegion
   * @param useStart whether to use the start or end point of region to pick the bin
   */
  def get(region: ReferenceRegion, useStart: Boolean = true): Int = {
    val pos = if (useStart) region.start else (region.end - 1)
    (contig2cumulParts(region.referenceName) + pos / binSize).toInt
  }

  /**
   * Get the bin number corresponding to the start of the query ReferenceRegion.
   */
  def getStartBin(region: ReferenceRegion): Int = get(region, useStart = true)

  /**
   * Get the bin number corresponding to the end of the query ReferenceRegion.
   */
  def getEndBin(region: ReferenceRegion): Int = get(region, useStart = false)

  /**
   * Given a bin number, return its corresponding ReferenceRegion.
   */
  def invert(bin: Int): ReferenceRegion = {
    val idx = cumulParts.indexWhere(_ > bin) - 1
    val name = names(idx)
    val relPartition = bin - contig2cumulParts(name)
    val start = relPartition * binSize
    val end = min((relPartition + 1) * binSize, seqLengths(name))
    ReferenceRegion(name, start, end)
  }
}
