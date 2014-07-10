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

import org.bdgenomics.adam.models.{ ReferencePosition, SequenceDictionary }
import org.apache.spark.{ Logging, Partitioner }
import scala.math._

/**
 * GenomicRegionPartitioner partitions ReferencePosition objects into separate, spatially-coherent
 * regions of the genome.
 *
 * This can be used to organize genomic data for computation that is spatially distributed (e.g. GATK and Queue's
 * "scatter-and-gather" for locus-parallelizable walkers).
 *
 * @param numParts The number of equally-sized regions into which the total genomic space is partitioned;
 *                 the total number of partitions is numParts + 1, with the "+1" resulting from one
 *                 extra partition that is used to capture null or UNMAPPED values of the ReferencePosition
 *                 type.
 * @param seqLengths a map relating sequence name-to-length and indicating the set and length of all extant
 *                   sequences in the genome.
 */
class GenomicRegionPartitioner(val numParts: Int, val seqLengths: Map[String, Long]) extends Partitioner with Logging {

  log.info("Have genomic region partitioner with " + numParts + " partitions, and sequences:")
  seqLengths.foreach(kv => log.info("Contig " + kv._1 + " with length " + kv._2))

  def this(numParts: Int, seqDict: SequenceDictionary) =
    this(numParts, GenomicRegionPartitioner.extractLengthMap(seqDict))

  val names: Seq[String] = seqLengths.keys.toSeq.sortWith(_ < _)
  val lengths: Seq[Long] = names.map(seqLengths(_))
  private val cumuls: Seq[Long] = lengths.scan(0L)(_ + _)

  // total # of bases in the sequence dictionary
  val totalLength: Long = lengths.reduce(_ + _)

  // referenceName -> cumulative length before this sequence (using seqDict.records as the implicit ordering)
  val cumulativeLengths: Map[String, Long] = Map(
    names.zip(cumuls): _*)

  /**
   * 'parts' is the total number of partitions for non-UNMAPPED ReferencePositions --
   * the total number of partitions (see numPartitions, below) is parts+1, with the
   * extra partition being included for handling ReferencePosition.UNMAPPED
   */
  private val parts = min(numParts, totalLength).toInt

  override def numPartitions: Int = parts + 1

  override def getPartition(key: Any): Int = {

    // This allows partitions that cross chromosome boundaries.
    // The computation is slightly more complicated if you want to avoid this.
    def getPart(referenceName: String, pos: Long): Int = {
      val totalOffset = cumulativeLengths(referenceName) + pos
      val totalFraction: Double = totalOffset.toDouble / totalLength
      // Need to use 'parts' here, rather than 'numPartitions' -- see the note
      // on 'parts', above.
      min(floor(totalFraction * parts.toDouble).toInt, numPartitions)
    }

    key match {
      // "unmapped" positions get put in the "top" or last bucket
      case ReferencePosition.UNMAPPED => parts

      // everything else gets assigned normally.
      case refpos: ReferencePosition  => getPart(refpos.referenceName, refpos.pos)

      // only ReferencePosition values are partitioned using this partitioner
      case _                          => throw new IllegalArgumentException("Only ReferencePosition values can be partitioned by GenomicRegionPartitioner")
    }
  }

  override def equals(x: Any): Boolean = {
    x match {
      case y: GenomicRegionPartitioner =>
        y.numPartitions == numPartitions && names == y.names && lengths == y.lengths
      case _ => false
    }
  }

  override def toString(): String = {
    return "%d parts, %d partitions, %s" format (parts, numPartitions, cumulativeLengths.toString)
  }

  override def hashCode(): Int = 37 * (37 * parts + names.hashCode()) + lengths.hashCode()
}

object GenomicRegionPartitioner {

  def apply(N: Int, lengths: Map[String, Long]) = new GenomicRegionPartitioner(N, lengths)

  def extractLengthMap(seqDict: SequenceDictionary): Map[String, Long] =
    Map(seqDict.records.toSeq.map(rec => (rec.name.toString, rec.length)): _*)
}
