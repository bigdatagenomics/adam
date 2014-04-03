/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.bdgenomics.adam.avro.ADAMNucleotideContigFragment
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.math.Ordering._

/**
 * Object for converting an RDD containing FASTA sequence data into ADAM FASTA data.
 */
private[adam] object FastaConverter {

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
   * @param rdd RDD containing Int,String tuples, where the Int corresponds to the number
   * of the file line, and the String is the line of the file.
   * @param maxFragmentLength The maximum length of fragments in the contig.
   * @return An RDD of ADAM FASTA data.
   */
  def apply(rdd: RDD[(Int, String)],
    maxFragmentLength: Long = 10000L): RDD[ADAMNucleotideContigFragment] = {
    val filtered = rdd.map(kv => (kv._1, kv._2.trim()))
      .filter((kv: (Int, String)) => !kv._2.startsWith(";"))

    val indices: Map[Int, Int] = rdd.filter((kv: (Int, String)) => kv._2.startsWith(">"))
      .map((kv: (Int, String)) => kv._1)
      .collect
      .toList
      .sortWith(_ < _)
      .zipWithIndex
      .reverse
      .toMap

    val groupedContigs: RDD[(Int, Seq[(Int, String)])] = if (indices.size == 0) {
      filtered.keyBy(kv => -1)
        .groupByKey()
    } else {
      filtered.keyBy((kv: (Int, String)) => indices.find(p => p._1 <= kv._1))
        .filter((kv: (Option[(Int, Int)], (Int, String))) => kv._1.isDefined)
        .map(kv => (kv._1.get._2, kv._2))
        .groupByKey()
    }

    val converter = new FastaConverter(maxFragmentLength)

    val convertedData = groupedContigs.flatMap(kv => {
      val (id, lines) = kv

      val descriptionLine = lines.filter(kv => kv._2.startsWith(">"))

      val (name, comment): (Option[String], Option[String]) = if (descriptionLine.size == 0) {
        assert(id == -1, "Cannot have a headerless line in a file with more than one fragment.")
        (None, None)
      } else if (descriptionLine.forall(kv => kv._2.contains(' '))) {
        val description: String = descriptionLine.head._2
        val splitIndex = description.indexOf(' ')
        val split = description.splitAt(splitIndex)

        val contigName: String = split._1.stripPrefix(">").trim
        val contigDescription: String = split._2.trim

        (Some(contigName), Some(contigDescription))
      } else {
        (Some(descriptionLine.head._2.stripPrefix(">").trim), None)
      }

      val seqId = if (id == -1) {
        0
      } else {
        id
      }

      val sequenceLines: Seq[(Int, String)] = lines.filter(kv => !kv._2.startsWith(">"))
      assert(sequenceLines.length != 0, "Sequence " + seqId + " has no sequence data.")

      val sequence: Seq[String] = sequenceLines.sortBy(kv => kv._1)
        .map(kv => kv._2.stripSuffix("*"))

      converter.convert(name, seqId, sequence, comment)
    })

    convertedData
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
    var sequence: String = ""
    var sequenceSeq: List[String] = List()

    /**
     * Adds a string fragment to our accumulator. If this string fragment causes the accumulator
     * to grow longer than our max fragment size, we split the accumulator and add it to the end
     * of our list of fragments.
     *
     * @param seq Fragment string to add.
     */
    def addFragment(seq: String) {
      sequence += seq

      if (sequence.length > fragmentLength) {
        sequenceSeq ::= sequence.take(fragmentLength.toInt)
        sequence = sequence.drop(fragmentLength.toInt)
      }
    }

    // run addFragment on all fragments
    sequences.foreach(addFragment(_))

    // if we still have a remaining sequence that is not part of a fragment, add it
    if (sequence.length != 0) {
      sequenceSeq ::= sequence
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

        val builder = ADAMNucleotideContigFragment.newBuilder()
          .setContigId(id)
          .setFragmentSequence(bases)
          .setContigLength(sequenceLength)
          .setFragmentNumber(index)
          .setFragmentStartPosition(index * fragmentLength)
          .setNumberOfFragmentsInContig(fragmentCount)

        // map over optional fields
        name.foreach(builder.setContigName(_))
        description.foreach(builder.setDescription(_))

        // build and return
        builder.build()
      })

    fragments
  }
}
