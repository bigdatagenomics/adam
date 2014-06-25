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
package org.bdgenomics.adam.converters

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.bdgenomics.formats.avro.{ ADAMContig, ADAMNucleotideContigFragment }
import scala.Int
import scala.Predef._
import scala.Some
import scala.collection.mutable

/**
 * Object for converting an RDD containing FASTA sequence data into ADAM FASTA data.
 */
private[adam] object FastaConverter {

  case class FastaDescriptionLine(val fileIndex: Long = -1L, val seqId: Int = 0, val descriptionLine: Option[String] = None) {
    val (contigName, contigDescription) = parseDescriptionLine(descriptionLine, fileIndex)

    private def parseDescriptionLine(descriptionLine: Option[String], id: Long): (Option[String], Option[String]) = {
      if (descriptionLine.isEmpty) {
        assert(id == -1L, "Cannot have a headerless line in a file with more than one fragment.")
        (None, None)
      } else {
        val splitIndex = descriptionLine.get.indexOf(' ')
        if (splitIndex >= 0) {
          val split = descriptionLine.get.splitAt(splitIndex)

          val contigName: String = split._1.stripPrefix(">").trim
          val contigDescription: String = split._2.trim

          (Some(contigName), Some(contigDescription))

        } else {
          (Some(descriptionLine.get.stripPrefix(">").trim), None)
        }
      }
    }
  }

  /**
   * Converts an RDD containing ints and strings into an RDD containing ADAM nucleotide
   * contig fragments.
   *
   * @note Input dataset is assumed to have come in from a Hadoop TextInputFormat reader. This sets
   * a specific format for the RDD's Key-Value pairs.
   *
   * @throws AssertionError Thrown if there appear to be multiple sequences in a single file
   * that do not have descriptions.
   * @throws IllegalArgumentError Thrown if a sequence does not have sequence data.
   *
   * @param rdd RDD containing Long,String tuples, where the Long corresponds to the number
   * of the file line, and the String is the line of the file.
   * @param maxFragmentLength The maximum length of fragments in the contig.
   * @return An RDD of ADAM FASTA data.
   */
  def apply(rdd: RDD[(Long, String)],
            maxFragmentLength: Long = 10000L): RDD[ADAMNucleotideContigFragment] = {
    val filtered = rdd.map(kv => (kv._1, kv._2.trim()))
      .filter((kv: (Long, String)) => !kv._2.startsWith(";"))

    val descriptionLines: Map[Long, FastaDescriptionLine] = getDescriptionLines(filtered)
    val indexToContigDescription = rdd.context.broadcast(descriptionLines)

    val sequenceLines = filtered.filter(kv => !isDescriptionLine(kv._2))

    val keyedSequences = if (indexToContigDescription.value.size == 0) {
      sequenceLines.keyBy(kv => -1L)
    } else {
      sequenceLines.keyBy(row => findContigIndex(row._1, indexToContigDescription.value.keys.toList))
    }
    val groupedContigs = keyedSequences.groupByKey()

    val converter = new FastaConverter(maxFragmentLength)

    groupedContigs.flatMap {
      case (id, lines) =>

        val descriptionLine = indexToContigDescription.value.getOrElse(id, FastaDescriptionLine())
        assert(lines.size != 0, "Sequence " + descriptionLine.seqId + " has no sequence data.")

        val sequence: Seq[String] = lines.toSeq.sortBy(_._1).map(kv => cleanSequence(kv._2))
        converter.convert(descriptionLine.contigName,
          descriptionLine.seqId,
          sequence,
          descriptionLine.contigDescription)
    }

  }

  private def cleanSequence(sequence: String): String = {
    sequence.stripSuffix("*")
  }

  private def isDescriptionLine(line: String): Boolean = {
    line.startsWith(">")
  }

  def getDescriptionLines(rdd: RDD[(Long, String)]): Map[Long, FastaDescriptionLine] = {

    rdd.filter(kv => isDescriptionLine(kv._2))
      .collect()
      .zipWithIndex
      .map(kv => (kv._1._1, FastaDescriptionLine(kv._1._1, kv._2, Some(kv._1._2))))
      .toMap
  }

  def findContigIndex(rowIdx: Long, indices: List[Long]): Long = {
    val idx = indices.filter(_ <= rowIdx)
    idx.max
  }
}

/**
 * Conversion methods for single FASTA sequences into ADAM FASTA data.
 */
private[converters] class FastaConverter(fragmentLength: Long) extends Serializable {

  /**
   * Remaps the fragments that we get coming in into our expected fragment size.
   *
   * @param sequences Fragments coming in.
   * @return A sequence of strings "recut" to the proper fragment size.
   */
  def mapFragments(sequences: Seq[String]): Seq[String] = {
    // internal "fsm" variables
    var sequence: StringBuilder = new StringBuilder
    var sequenceSeq: mutable.MutableList[String] = mutable.MutableList()

    /**
     * Adds a string fragment to our accumulator. If this string fragment causes the accumulator
     * to grow longer than our max fragment size, we split the accumulator and add it to the end
     * of our list of fragments.
     *
     * @param seq Fragment string to add.
     */
    def addFragment(seq: String) {
      sequence.append(seq)

      while (sequence.length > fragmentLength) {
        sequenceSeq += sequence.take(fragmentLength.toInt).toString()
        sequence = sequence.drop(fragmentLength.toInt)
      }
    }

    // run addFragment on all fragments
    sequences.foreach(addFragment)

    // if we still have a remaining sequence that is not part of a fragment, add it
    if (sequence.length != 0) {
      sequenceSeq += sequence.toString()
    }

    // return our fragments
    sequenceSeq.toSeq
  }

  /**
   * Converts a single FASTA sequence into an ADAM FASTA contig.
   *
   * @throws IllegalArgumentException Thrown if sequence contains an illegal character.
   *
   * @param name String option for the sequence name.
   * @param id Numerical identifier for the sequence.
   * @param sequence Nucleotide sequence.
   * @param description Optional description of the sequence.
   * @return The converted ADAM FASTA contig.
   */
  def convert(name: Option[String],
              id: Int,
              sequence: Seq[String],
              description: Option[String]): Seq[ADAMNucleotideContigFragment] = {

    // get sequence length
    val sequenceLength = sequence.map(_.length).reduce(_ + _)

    // map sequences into fragments
    val sequencesAsFragments = mapFragments(sequence)

    // get number of fragments
    val fragmentCount = sequencesAsFragments.length

    // make new builder and set up non-optional fields
    val fragments = sequencesAsFragments.zipWithIndex
      .map(si => {
        val (bases, index) = si

        val contig = ADAMContig.newBuilder
          .setContigLength(sequenceLength)

        val builder = ADAMNucleotideContigFragment.newBuilder()
          .setFragmentSequence(bases)
          .setFragmentNumber(index)
          .setFragmentStartPosition(index * fragmentLength)
          .setNumberOfFragmentsInContig(fragmentCount)

        // map over optional fields
        name.foreach(contig.setContigName(_))
        description.foreach(builder.setDescription(_))
        builder.setContig(contig.build)
        // build and return
        builder.build()
      })

    fragments
  }
}
