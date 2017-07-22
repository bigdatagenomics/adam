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
package org.bdgenomics.adam.rdd.sequence

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rdd.{
  AvroGenomicRDD,
  JavaSaveArgs
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro.{
  Read,
  Sequence,
  Slice,
  Strand
}
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.bdgenomics.utils.misc.Logging
import scala.reflect.ClassTag

private[adam] case class ReadArray(
    array: Array[(ReferenceRegion, Read)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Read] {

  def duplicate(): IntervalArray[ReferenceRegion, Read] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Read)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Read] = {
    ReadArray(arr, maxWidth)
  }
}

private[adam] class ReadArraySerializer extends IntervalArraySerializer[ReferenceRegion, Read, ReadArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Read]

  protected def builder(arr: Array[(ReferenceRegion, Read)],
                        maxIntervalWidth: Long): ReadArray = {
    ReadArray(arr, maxIntervalWidth)
  }
}

object ReadRDD {

  /**
   * Builds a ReadRDD with an empty sequence dictionary and without a partition map.
   *
   * @param rdd The underlying Read RDD to build from.
   * @return Returns a new ReadRDD.
   */
  def apply(rdd: RDD[Read]): ReadRDD = {
    ReadRDD(rdd, SequenceDictionary.empty, optPartitionMap = None)
  }
}

/**
 * A GenomicRDD that wraps Read data.
 *
 * @param rdd An RDD of genomic Reads.
 * @param sequences The reference genome these data are aligned to, if any.
 * @param optPartitionMap Optional partition map.
 */
case class ReadRDD(rdd: RDD[Read],
                   sequences: SequenceDictionary,
                   optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends AvroGenomicRDD[Read, ReadRDD] with Logging {

  protected def buildTree(rdd: RDD[(ReferenceRegion, Read)])(
    implicit tTag: ClassTag[Read]): IntervalArray[ReferenceRegion, Read] = {
    IntervalArray(rdd, ReadArray.apply(_, _))
  }

  def union(rdds: ReadRDD*): ReadRDD = {
    val iterableRdds = rdds.toSeq
    ReadRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      optPartitionMap = None)
  }

  /**
   * Convert this RDD of reads into sequences.
   *
   * @return Returns a new SequenceRDD converted from this RDD of reads.
   */
  def toSequences: SequenceRDD = {
    def toSequence(read: Read): Sequence = {
      Sequence.newBuilder()
        .setName(read.getName)
        .setDescription(read.getDescription)
        .setAlphabet(read.getAlphabet)
        .setSequence(read.getSequence)
        .setLength(read.getLength)
        .setAttributes(read.getAttributes)
        .build()
    }
    SequenceRDD(rdd.map(toSequence), sequences, optPartitionMap = None)
  }

  /**
   * Convert this RDD of reads into slices.
   *
   * @return Returns a new SliceRDD converted from this RDD of reads.
   */
  def toSlices: SliceRDD = {
    def toSlice(read: Read): Slice = {
      Slice.newBuilder()
        .setName(read.getName)
        .setDescription(read.getDescription)
        .setAlphabet(read.getAlphabet)
        .setSequence(read.getSequence)
        .setLength(read.getLength)
        .setTotalLength(read.getLength)
        .setStart(0L)
        .setEnd(read.getLength)
        .setStrand(Strand.INDEPENDENT)
        .setAttributes(read.getAttributes)
        .build()
    }
    SliceRDD(rdd.map(toSlice), sequences, optPartitionMap = None)
  }

  /**
   * Save reads as Parquet or FASTQ.
   *
   * If filename ends in .fq or .fastq, saves as FASTQ. If not, saves reads
   * to Parquet.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   */
  def save(filePath: java.lang.String, asSingleFile: java.lang.Boolean) {
    if (filePath.endsWith(".fq") || filePath.endsWith(".fastq")) {
      saveAsFastq(filePath, asSingleFile = asSingleFile)
    } else {
      if (asSingleFile) {
        log.warn("asSingleFile = true ignored when saving as Parquet.")
      }
      saveAsParquet(new JavaSaveArgs(filePath))
    }
  }

  /**
   * Save reads in FASTQ format.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   */
  def saveAsFastq(filePath: String, asSingleFile: Boolean = false) {

    def toFastq(read: Read): String = {
      val sb = new StringBuilder()
      sb.append("@")
      sb.append(read.getName)
      Option(read.getDescription).foreach(n => sb.append(" ").append(n))
      sb.append("\n")
      sb.append(read.getSequence)
      sb.append("\n+\n")
      sb.append(read.getQualityScores)
      sb.append("\n")
      sb.toString
    }

    writeTextRdd(rdd.map(toFastq), filePath, asSingleFile)
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new ReadRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Read],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): ReadRDD = {
    copy(rdd = newRdd, optPartitionMap = newPartitionMap)
  }

  /**
   * @param read Read to extract a region from.
   * @return Returns a reference region that covers the entirety of the read.
   */
  protected def getReferenceRegions(read: Read): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(read.getName, 0L, read.getLength))
  }
}
