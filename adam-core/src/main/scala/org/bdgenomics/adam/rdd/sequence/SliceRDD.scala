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
  QualityScoreVariant,
  Read,
  Sequence,
  Slice
}
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

private[adam] case class SliceArray(
    array: Array[(ReferenceRegion, Slice)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Slice] {

  def duplicate(): IntervalArray[ReferenceRegion, Slice] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Slice)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Slice] = {
    SliceArray(arr, maxWidth)
  }
}

private[adam] class SliceArraySerializer extends IntervalArraySerializer[ReferenceRegion, Slice, SliceArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Slice]

  protected def builder(arr: Array[(ReferenceRegion, Slice)],
                        maxIntervalWidth: Long): SliceArray = {
    SliceArray(arr, maxIntervalWidth)
  }
}

object SliceRDD {

  /**
   * Builds a SliceRDD with an empty sequence dictionary and without a partition map.
   *
   * @param rdd The underlying Slice RDD to build from.
   * @return Returns a new SliceRDD.
   */
  def apply(rdd: RDD[Slice]): SliceRDD = {
    SliceRDD(rdd, SequenceDictionary.empty, optPartitionMap = None)
  }
}

/**
 * A GenomicRDD that wraps Slice data.
 *
 * @param rdd An RDD of genomic Slices.
 * @param sequences The reference genome these data are aligned to, if any.
 * @param optPartitionMap Optional partition map.
 */
case class SliceRDD(rdd: RDD[Slice],
                    sequences: SequenceDictionary,
                    optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends AvroGenomicRDD[Slice, SliceRDD] with Logging {

  protected def buildTree(rdd: RDD[(ReferenceRegion, Slice)])(
    implicit tTag: ClassTag[Slice]): IntervalArray[ReferenceRegion, Slice] = {
    IntervalArray(rdd, SliceArray.apply(_, _))
  }

  def union(rdds: SliceRDD*): SliceRDD = {
    val iterableRdds = rdds.toSeq
    SliceRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      optPartitionMap = None)
  }

  /**
   * Merge slices into sequences.
   *
   * @return Returns a SequenceRDD containing merged slices.
   */
  def merge(): SequenceRDD = {
    def toSequence(slice: Slice): Sequence = {
      Sequence.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setAttributes(slice.getAttributes)
        .build
    }

    def mergeSequences(first: Sequence, second: Sequence): Sequence = {
      Sequence.newBuilder(first)
        .setLength(first.getLength + second.getLength)
        .setSequence(first.getSequence + second.getSequence)
        .setAttributes(first.getAttributes ++ second.getAttributes)
        .build
    }

    val merged: RDD[Sequence] = rdd
      .sortBy(slice => (slice.getName, slice.getStart))
      .map(slice => (slice.getName, toSequence(slice)))
      .reduceByKey(mergeSequences)
      .values

    SequenceRDD(merged)
  }

  /**
   * Convert this RDD of slices into reads.
   *
   * @return Returns a new ReadRDD converted from this RDD of slices.
   */
  def toReads: ReadRDD = {
    def toRead(slice: Slice): Read = {
      Read.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setQualityScoreVariant(QualityScoreVariant.FASTQ_SANGER)
        .setQualityScores("B" * (if (slice.getLength == null) 0 else slice.getLength.toInt))
        .setAttributes(slice.getAttributes)
        .build()
    }
    ReadRDD(rdd.map(toRead), sequences, optPartitionMap = None)
  }

  /**
   * Convert this RDD of slices into sequences.
   *
   * @return Returns a new SequenceRDD converted from this RDD of slices.
   */
  def toSequences: SequenceRDD = {
    def toSequence(slice: Slice): Sequence = {
      Sequence.newBuilder()
        .setName(slice.getName)
        .setDescription(slice.getDescription)
        .setAlphabet(slice.getAlphabet)
        .setSequence(slice.getSequence)
        .setLength(slice.getLength)
        .setAttributes(slice.getAttributes)
        .build()
    }
    SequenceRDD(rdd.map(toSequence), sequences, optPartitionMap = None)
  }

  /**
   * Save slices as Parquet or FASTA.
   *
   * If filename ends in .fa or .fasta, saves as FASTA. If not, saves slices
   * to Parquet. Defaults to 60 character line length, if saving to FASTA.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   */
  def save(filePath: java.lang.String, asSingleFile: java.lang.Boolean) {
    if (filePath.endsWith(".fa") || filePath.endsWith(".fasta")) {
      saveAsFasta(filePath, asSingleFile = asSingleFile)
    } else {
      if (asSingleFile) {
        log.warn("asSingleFile = true ignored when saving as Parquet.")
      }
      saveAsParquet(new JavaSaveArgs(filePath))
    }
  }

  /**
   * Save slices in FASTA format.
   *
   * The coordinate fields for this slice are appended to the description field
   * for the FASTA description line:
   * <pre>
   * &gt;description start-slice:strand
   * </pre>
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   * @param lineWidth Hard wrap FASTA formatted slice at line width, default 60.
   */
  def saveAsFasta(filePath: String, asSingleFile: Boolean = false, lineWidth: Int = 60) {

    def toFasta(slice: Slice): String = {
      val sb = new StringBuilder()
      sb.append(">")
      sb.append(slice.getName)
      Option(slice.getDescription).foreach(n => sb.append(" ").append(n))
      sb.append(s" slice.getStart-slice.getEnd:slice.getStrand")
      slice.getSequence.grouped(lineWidth).foreach(line => {
        sb.append("\n")
        sb.append(line)
      })
      sb.toString
    }

    writeTextRdd(rdd.map(toFasta), filePath, asSingleFile)
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new SliceRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Slice],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): SliceRDD = {
    copy(rdd = newRdd, optPartitionMap = newPartitionMap)
  }

  /**
   * @param slice Slice to extract a region from.
   * @return Returns a reference region that covers the entirety of the slice.
   */
  protected def getReferenceRegions(slice: Slice): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(slice.getName, slice.getStart, slice.getEnd, slice.getStrand))
  }
}
