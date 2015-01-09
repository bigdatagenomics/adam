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

import org.bdgenomics.adam.models.{ ReferenceRegion, ReferenceMapping, ReferencePosition, SequenceDictionary }
import org.apache.spark.{ Logging, Partitioner }
import scala.math._

/**
 * GenomicPositionPartitioner partitions ReferencePosition objects into separate, spatially-coherent
 * regions of the genome.
 *
 * This can be used to organize genomic data for computation that is spatially distributed (e.g. GATK and Queue's
 * "scatter-and-gather" for locus-parallelizable walkers).
 *
 * @param numParts The number of equally-sized regions into which the total genomic space is partitioned;
 *                 the total number of partitions is numParts + 1, with the "+1" resulting from one
 *                 extra partition that is used to capture null or UNMAPPED values of the ReferencePosition
 *                 type.
 * @param seqLengths a map relating sequence-name to length and indicating the set and length of all extant
 *                   sequences in the genome.
 */
case class GenomicPositionPartitioner(numParts: Int, seqLengths: Map[String, Long]) extends Partitioner with Logging {

  log.info("Have genomic position partitioner with " + numParts + " partitions, and sequences:")
  seqLengths.foreach(kv => log.info("Contig " + kv._1 + " with length " + kv._2))

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
      case _                          => throw new IllegalArgumentException("Only ReferencePosition values can be partitioned by GenomicPositionPartitioner")
    }
  }

  override def toString(): String = {
    return "%d parts, %d partitions, %s" format (parts, numPartitions, cumulativeLengths.toString)
  }

}

object GenomicPositionPartitioner {

  def apply(numParts: Int, seqDict: SequenceDictionary): GenomicPositionPartitioner =
    GenomicPositionPartitioner(numParts, extractLengthMap(seqDict))

  def extractLengthMap(seqDict: SequenceDictionary): Map[String, Long] =
    Map(seqDict.records.toSeq.map(rec => (rec.name.toString, rec.length)): _*)
}

case class GenomicRegionPartitioner[T: ReferenceMapping](partitionSize: Long, seqLengths: Map[String, Long], start: Boolean = true) extends Partitioner with Logging {
  private val names: Seq[String] = seqLengths.keys.toSeq.sortWith(_ < _)
  private val lengths: Seq[Long] = names.map(seqLengths(_))
  private val parts: Seq[Int] = lengths.map(v => round(ceil(v.toDouble / partitionSize)).toInt)
  private val cumulParts: Map[String, Int] = Map(names.zip(parts.scan(0)(_ + _)): _*)

  private def extractReferenceRegion(k: T)(implicit tMapping: ReferenceMapping[T]): ReferenceRegion = {
    tMapping.getReferenceRegion(k)
  }

  private def computePartition(refReg: ReferenceRegion): Int = {
    val pos = if (start) refReg.start else (refReg.end - 1)
    (cumulParts(refReg.referenceName) + pos / partitionSize).toInt
  }

  override def numPartitions: Int = parts.sum

  override def getPartition(key: Any): Int = {
    key match {
      case mappable: T => computePartition(extractReferenceRegion(mappable))
      case _           => throw new IllegalArgumentException("Only ReferenceMappable values can be partitioned by GenomicRegionPartitioner")
    }
  }
}

object GenomicRegionPartitioner {
  def apply[T: ReferenceMapping](partitionSize: Long, seqDict: SequenceDictionary): GenomicRegionPartitioner[T] =
    GenomicRegionPartitioner(partitionSize, GenomicPositionPartitioner.extractLengthMap(seqDict))
}
