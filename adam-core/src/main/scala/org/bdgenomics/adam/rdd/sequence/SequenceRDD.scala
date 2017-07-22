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
  Slice,
  Strand
}
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.bdgenomics.utils.misc.Logging
import scala.collection.mutable.MutableList
import scala.reflect.ClassTag

private[adam] case class SequenceArray(
    array: Array[(ReferenceRegion, Sequence)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Sequence] {

  def duplicate(): IntervalArray[ReferenceRegion, Sequence] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Sequence)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Sequence] = {
    SequenceArray(arr, maxWidth)
  }
}

private[adam] class SequenceArraySerializer extends IntervalArraySerializer[ReferenceRegion, Sequence, SequenceArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Sequence]

  protected def builder(arr: Array[(ReferenceRegion, Sequence)],
                        maxIntervalWidth: Long): SequenceArray = {
    SequenceArray(arr, maxIntervalWidth)
  }
}

object SequenceRDD {

  /**
   * Builds a SequenceRDD with an empty sequence dictionary and without a partition map.
   *
   * @param rdd The underlying Sequence RDD to build from.
   * @return Returns a new SequenceRDD.
   */
  def apply(rdd: RDD[Sequence]): SequenceRDD = {
    SequenceRDD(rdd, SequenceDictionary.empty, optPartitionMap = None)
  }
}

/**
 * A GenomicRDD that wraps Sequence data.
 *
 * @param rdd An RDD of genomic Sequences.
 * @param sequences The reference genome these data are aligned to, if any.
 * @param optPartitionMap Optional partition map.
 */
case class SequenceRDD(rdd: RDD[Sequence],
                       sequences: SequenceDictionary,
                       optPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]]) extends AvroGenomicRDD[Sequence, SequenceRDD] with Logging {

  protected def buildTree(rdd: RDD[(ReferenceRegion, Sequence)])(
    implicit tTag: ClassTag[Sequence]): IntervalArray[ReferenceRegion, Sequence] = {
    IntervalArray(rdd, SequenceArray.apply(_, _))
  }

  def union(rdds: SequenceRDD*): SequenceRDD = {
    val iterableRdds = rdds.toSeq
    SequenceRDD(rdd.context.union(rdd, iterableRdds.map(_.rdd): _*),
      iterableRdds.map(_.sequences).fold(sequences)(_ ++ _),
      optPartitionMap = None)
  }

  /**
   * Slice the sequences in this RDD to the specified maximum length.
   *
   * @param maximumLength Maximum length.
   * @return Returns a new SliceRDD from the sequences in this RDD sliced
   *    to the specified maximum length.
   */
  def slice(maximumLength: Long): SliceRDD = {
    def sliceSequence(sequence: Sequence): Seq[Slice] = {
      val slices: MutableList[Slice] = MutableList()

      val sb = Slice.newBuilder
        .setName(sequence.getName)
        .setDescription(sequence.getDescription)
        .setAlphabet(sequence.getAlphabet)
        .setSequence(sequence.getSequence)
        .setStrand(Strand.INDEPENDENT)
        .setTotalLength(sequence.getLength)
        .setAttributes(sequence.getAttributes)

      var index = 0
      var count = (sequence.getLength / maximumLength).toInt
      if (sequence.getLength % maximumLength != 0) count += 1
      for (start <- 0L until sequence.getLength by maximumLength) {
        val end = math.min(sequence.getLength, start + maximumLength)
        slices += sb
          .setStart(start)
          .setEnd(end)
          .setLength(end - start)
          .setSequence(sequence.getSequence.substring(start.toInt, end.toInt))
          .setIndex(index)
          .setSlices(count)
          .build()
        index += 1
      }
      slices
    }
    SliceRDD(rdd.flatMap(sliceSequence), sequences, optPartitionMap = None)
  }

  /**
   * Slice the specified sequence overlapping the specified region.
   *
   * @param region Region to overlap.
   * @return Returns a new Slice from the sequence overlapping the specified region.
   */
  private def slice(sequence: Sequence, region: ReferenceRegion): Slice = {
    // region may be open-ended
    val end = math.min(sequence.getLength, region.end)
    Slice.newBuilder()
      .setName(sequence.getName)
      .setDescription(sequence.getDescription)
      .setAlphabet(sequence.getAlphabet)
      .setSequence(sequence.getSequence.substring(region.start.toInt, end.toInt))
      .setLength(end - region.start)
      .setTotalLength(sequence.getLength)
      .setStart(region.start)
      .setEnd(end)
      .setStrand(region.strand) // perhaps Sequence should have strand?
      .setAttributes(sequence.getAttributes)
      .build()
  }

  /**
   * Slice the sequences in this RDD overlapping the specified region.
   *
   * @param region Region to overlap.
   * @return Returns a new SliceRDD from the sequences in this RDD sliced
   *    to overlap the specified region.
   */
  def slice(region: ReferenceRegion): SliceRDD = {
    SliceRDD(filterByOverlappingRegion(region).rdd.map(sequence => slice(sequence, region)))
  }

  /**
   * Slice the specified sequence overlapping the specified regions.
   *
   * @param regions Regions to overlap.
   * @return Returns one or more slices from the sequence overlapping the specified regions.
   */
  private def slice(sequence: Sequence, regions: Iterable[ReferenceRegion]): Iterable[Slice] = {
    val sequenceRegion = ReferenceRegion(sequence)
    regions.map(region =>
      if (region.covers(sequenceRegion)) {
        Some(slice(sequence, region))
      } else {
        None
      }).flatten
  }

  /**
   * Slice the sequences in this RDD overlapping the specified regions.
   *
   * @param region Regions to overlap.
   * @return Returns a new SliceRDD from the sequences in this RDD sliced
   *    to overlap the specified regions.
   */
  def slice(regions: Iterable[ReferenceRegion]): SliceRDD = {
    SliceRDD(filterByOverlappingRegions(regions).rdd.flatMap(sequence => slice(sequence, regions)))
  }

  /**
   * Convert this RDD of sequences into reads.
   *
   * @return Returns a new ReadRDD converted from this RDD of sequences.
   */
  def toReads: ReadRDD = {
    def toRead(sequence: Sequence): Read = {
      Read.newBuilder()
        .setName(sequence.getName)
        .setDescription(sequence.getDescription)
        .setAlphabet(sequence.getAlphabet)
        .setSequence(sequence.getSequence)
        .setLength(sequence.getLength)
        .setQualityScoreVariant(QualityScoreVariant.FASTQ_SANGER)
        .setQualityScores("B" * (if (sequence.getLength == null) 0 else sequence.getLength.toInt))
        .setAttributes(sequence.getAttributes)
        .build()
    }
    ReadRDD(rdd.map(toRead), sequences, optPartitionMap = None)
  }

  /**
   * Convert this RDD of sequences into slices.
   *
   * @return Returns a new SliceRDD converted from this RDD of sequences.
   */
  def toSlices: SliceRDD = {
    def toSlice(sequence: Sequence): Slice = {
      Slice.newBuilder()
        .setName(sequence.getName)
        .setDescription(sequence.getDescription)
        .setAlphabet(sequence.getAlphabet)
        .setSequence(sequence.getSequence)
        .setLength(sequence.getLength)
        .setTotalLength(sequence.getLength)
        .setStart(0L)
        .setEnd(sequence.getLength)
        .setStrand(Strand.INDEPENDENT)
        .setAttributes(sequence.getAttributes)
        .build()
    }
    SliceRDD(rdd.map(toSlice), sequences, optPartitionMap = None)
  }

  /**
   * Save sequences as Parquet or FASTA.
   *
   * If filename ends in .fa or .fasta, saves as FASTA. If not, saves fragments
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
   * Save sequences in FASTA format.
   *
   * @param filePath Path to save files to.
   * @param asSingleFile If true, saves output as a single file.
   * @param lineWidth Hard wrap FASTA formatted sequence at line width, default 60.
   */
  def saveAsFasta(filePath: String, asSingleFile: Boolean = false, lineWidth: Int = 60) {

    def toFasta(sequence: Sequence): String = {
      val sb = new StringBuilder()
      sb.append(">")
      sb.append(sequence.getName)
      Option(sequence.getDescription).foreach(n => sb.append(" ").append(n))
      sequence.getSequence.grouped(lineWidth).foreach(line => {
        sb.append("\n")
        sb.append(line)
      })
      sb.toString
    }

    writeTextRdd(rdd.map(toFasta), filePath, asSingleFile)
  }

  /**
   * @param newRdd The RDD to replace the underlying RDD with.
   * @return Returns a new SequenceRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Sequence],
                           newPartitionMap: Option[Array[Option[(ReferenceRegion, ReferenceRegion)]]] = None): SequenceRDD = {
    copy(rdd = newRdd, optPartitionMap = newPartitionMap)
  }

  /**
   * @param sequence Sequence to extract a region from.
   * @return Returns a reference region that covers the entirety of the sequence.
   */
  protected def getReferenceRegions(sequence: Sequence): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(sequence))
  }
}
