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
package org.bdgenomics.adam.util

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.formats.avro.NucleotideContigFragment

/**
 * A broadcastable ReferenceFile backed by a map containing contig name ->
 * Seq[NucleotideContigFragment] pairs.
 *
 * @param contigMap a map containing a Seq of contig fragments per contig.
 */
case class ReferenceContigMap(contigMap: Map[String, Seq[NucleotideContigFragment]]) extends ReferenceFile {

  /**
   * The sequence dictionary corresponding to the contigs in this collection of fragments.
   */
  val sequences: SequenceDictionary = new SequenceDictionary(contigMap.map(r =>
    SequenceRecord(r._1, r._2.map(_.getFragmentEndPosition).max)).toVector)

  /**
   * Extract reference sequence from the file.
   *
   * @param region The desired ReferenceRegion to extract.
   * @return The reference sequence at the desired locus.
   */
  override def extract(region: ReferenceRegion): String = {
    contigMap
      .getOrElse(
        region.referenceName,
        throw new Exception(
          s"Contig ${region.referenceName} not found in reference map with keys: ${contigMap.keys.toList.sortBy(x => x).mkString(", ")}"
        )
      )
      .dropWhile(f => f.getFragmentStartPosition + f.getFragmentSequence.length < region.start)
      .takeWhile(_.getFragmentStartPosition < region.end)
      .map(
        clipFragment(_, region.start, region.end)
      )
      .mkString("")
  }

  private def clipFragment(fragment: NucleotideContigFragment, start: Long, end: Long): String = {
    val min =
      math.max(
        0L,
        start - fragment.getFragmentStartPosition
      ).toInt

    val max =
      math.min(
        fragment.getFragmentSequence.length,
        end - fragment.getFragmentStartPosition
      ).toInt

    fragment.getFragmentSequence.substring(min, max)
  }
}

/**
 * Companion object for creating a ReferenceContigMap from an RDD of contig
 * fragments.
 */
object ReferenceContigMap {

  /**
   * Builds a ReferenceContigMap from an RDD of fragments.
   *
   * @param fragments RDD of nucleotide contig fragments describing a genome
   *   reference.
   * @return Returns a serializable wrapper around these fragments that enables
   *   random access into the reference genome.
   */
  def apply(fragments: RDD[NucleotideContigFragment]): ReferenceContigMap = {
    ReferenceContigMap(
      fragments
        .groupBy(_.getContig.getContigName)
        .mapValues(_.toSeq.sortBy(_.getFragmentStartPosition))
        .collectAsMap
        .toMap
    )
  }
}
