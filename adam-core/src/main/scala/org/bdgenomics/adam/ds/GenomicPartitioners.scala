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

import grizzled.slf4j.Logging
import org.bdgenomics.adam.models.{ ReferenceRegion, ReferencePosition, SequenceDictionary }
import org.apache.spark.Partitioner
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

  info("Have genomic position partitioner with " + numParts + " partitions, and sequences:")
  if (isInfoEnabled) {
    seqLengths.foreach(kv => info("Contig " + kv._1 + " with length " + kv._2))
  }

  private val names: Seq[String] = seqLengths.keys.toSeq.sortWith(_ < _)
  private val lengths: Seq[Long] = names.map(seqLengths(_))
  private val cumuls: Seq[Long] = lengths.scan(0L)(_ + _)

  // total # of bases in the sequence dictionary
  private val totalLength: Long = lengths.sum

  // referenceName -> cumulative length before this sequence (using seqDict.records as the implicit ordering)
  private[ds] val cumulativeLengths: Map[String, Long] = Map(
    names.zip(cumuls): _*
  )

  /**
   * 'parts' is the total number of partitions for non-UNMAPPED ReferencePositions --
   * the total number of partitions (see numPartitions, below) is parts+1, with the
   * extra partition being included for handling ReferencePosition.UNMAPPED
   *
   * @see numPartitions
   */
  private val parts = min(numParts, totalLength).toInt

  /**
   * This is the total number of partitions for both mapped and unmapped
   * positions. All unmapped positions go into the last partition.
   *
   * @see parts
   */
  override def numPartitions: Int = parts + 1

  /**
   * Computes the partition for a key.
   *
   * @param key A key to compute the partition for.
   * @return The partition that this key belongs to.
   *
   * @throws IllegalArgumentException if the key is not a ReferencePosition, or
   *   (ReferencePosition, _) tuple.
   */
  override def getPartition(key: Any): Int = {

    // This allows partitions that cross chromosome boundaries.
    // The computation is slightly more complicated if you want to avoid this.
    def getPart(referenceName: String, pos: Long): Int = {
      require(
        seqLengths.contains(referenceName),
        "Received key (%s) that did not map to a known contig. Contigs are:\n%s".format(
          referenceName,
          seqLengths.keys.mkString("\n")
        )
      )
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
      case refpos: ReferencePosition => {
        getPart(refpos.referenceName, refpos.pos)
      }
      case (refpos: ReferencePosition, k: Any) => {
        getPart(refpos.referenceName, refpos.pos)
      }

      // only ReferencePosition values are partitioned using this partitioner
      case _ => throw new IllegalArgumentException("Only ReferencePosition values can be partitioned by GenomicPositionPartitioner")
    }
  }

  override def toString(): String = {
    return "%d parts, %d partitions, %s" format (parts, numPartitions, cumulativeLengths.toString)
  }
}

/**
 * Helper for creating genomic position partitioners.
 */
object GenomicPositionPartitioner {

  /**
   * Creates a GenomicRegionPartitioner with a specific number of partitions.
   *
   * @param numParts The number of partitions to have in the new partitioner.
   * @param seqDict A sequence dictionary describing the known genomic contigs.
   * @return Returns a partitioner that divides the known genome into a set number of partitions.
   */
  def apply(numParts: Int, seqDict: SequenceDictionary): GenomicPositionPartitioner =
    GenomicPositionPartitioner(numParts, extractLengthMap(seqDict))

  private[ds] def extractLengthMap(seqDict: SequenceDictionary): Map[String, Long] =
    seqDict.records.toSeq.map(rec => (rec.name, rec.length)).toMap
}

/**
 * A partitioner for ReferenceRegion-keyed data.
 *
 * @param partitionSize The number of bases per partition.
 * @param seqLengths A map between contig names and contig lengths.
 * @param start If true, use the start position (instead of the end position) to
 *   decide which partition a key belongs to.
 */
case class GenomicRegionPartitioner(partitionSize: Long,
                                    seqLengths: Map[String, Long],
                                    start: Boolean = true) extends Partitioner with Logging {
  private val names: Seq[String] = seqLengths.keys.toSeq.sortWith(_ < _)
  private val lengths: Seq[Long] = names.map(seqLengths(_))
  private val parts: Seq[Int] = lengths.map(v => round(ceil(v.toDouble / partitionSize)).toInt)
  private val cumulParts: Map[String, Int] = Map(names.zip(parts.scan(0)(_ + _)): _*)

  private def computePartition(refReg: ReferenceRegion): Int = {
    require(
      seqLengths.contains(refReg.referenceName),
      "Received key (%s) that did not map to a known contig. Contigs are:\n%s".format(
        refReg.referenceName,
        seqLengths.keys.mkString("\n")
      )
    )
    val pos = if (start) refReg.start else (refReg.end - 1)
    (cumulParts(refReg.referenceName) + pos / partitionSize).toInt
  }

  /**
   * @return The number of partitions described by this partitioner. Roughly the
   *   size of the genome divided by the partition length.
   */
  override def numPartitions: Int = parts.sum

  /**
   * @param key The key to get the partition index for.
   * @return The partition that a key should map to.
   *
   * @throws IllegalArgumentException Throws an exception if the data is not a
   *   ReferenceRegion or a tuple of (ReferenceRegion, _).
   */
  override def getPartition(key: Any): Int = {
    key match {
      case region: ReferenceRegion => {
        computePartition(region)
      }
      case (region: ReferenceRegion, k: Any) => {
        computePartition(region)
      }
      case _ => throw new IllegalArgumentException("Only ReferenceMappable values can be partitioned by GenomicRegionPartitioner")
    }
  }
}

/**
 * Helper object for creating GenomicRegionPartitioners.
 */
object GenomicRegionPartitioner {

  /**
   * Creates a GenomicRegionPartitioner where partitions cover a specific range of the genome.
   *
   * @param partitionSize The number of bases in the reference genome that each partition should cover.
   * @param seqDict A sequence dictionary describing the known genomic contigs.
   * @return Returns a partitioner that divides the known genome into partitions of fixed size.
   */
  def apply(partitionSize: Long, seqDict: SequenceDictionary): GenomicRegionPartitioner =
    GenomicRegionPartitioner(partitionSize, GenomicPositionPartitioner.extractLengthMap(seqDict))

  /**
   * Creates a GenomicRegionPartitioner with a specific number of partitions.
   *
   * @param numParts The number of partitions to have in the new partitioner.
   * @param seqDict A sequence dictionary describing the known genomic contigs.
   * @return Returns a partitioner that divides the known genome into a set number of partitions.
   */
  def apply(numParts: Int, seqDict: SequenceDictionary): GenomicRegionPartitioner = {
    val lengths = GenomicPositionPartitioner.extractLengthMap(seqDict)
    GenomicRegionPartitioner(lengths.values.sum / numParts, lengths)
  }
}
