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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{ Contig, NucleotideContigFragment }
import scala.collection.mutable

/**
 * Object for converting an RDD containing FASTA sequence data into ADAM FASTA data.
 */
private[adam] object FastaConverter {

  /**
   * Case class that describes a line in FASTA that begins with a ">".
   *
   * In FASTA, a sequence starts with a line that begins with a ">" and that
   * gives the sequence name, and optionally, miscellaneous information about
   * the sequence. If the file contains a single line, this description line
   * can be omitted.
   *
   * @param fileIndex The line number where this line was seen in the file.
   * @param seqId The index of this sequence in the file.
   * @param descriptionLine An optional string that describes the FASTA line.
   */
  case class FastaDescriptionLine(fileIndex: Long = -1L,
                                  seqId: Int = 0,
                                  descriptionLine: Option[String] = None) {
    /**
     * The contig name and description that was parsed out of this description line.
     */
    val (contigName, contigDescription) = parseDescriptionLine(descriptionLine, fileIndex)

    /**
     * Parses the text of a given line.
     *
     * Assumes that the line contains the contig name followed by an optional
     * description of the contig, with the two separated by a space.
     *
     * @throws IllegalArgumentException if there is no name in the line and the
     *   line is not the only record in a file (i.e., the file contains multiple
     *   contigs).
     *
     * @param descriptionLine The optional string describing the contig. If this
     *   is not set and this isn't the only line in the file, we throw.
     * @param id The index of this contig in the file.
     * @return Returns a tuple containing (the optional contig name, and the
     *   optional contig description).
     */
    private def parseDescriptionLine(descriptionLine: Option[String],
                                     id: Long): (Option[String], Option[String]) = {
      descriptionLine.fold {
        require(id == -1L, "Cannot have a headerless line in a file with more than one fragment.")
        (None: Option[String], None: Option[String])
      } { (dL) =>
        val splitIndex = dL.indexOf(' ')
        if (splitIndex >= 0) {
          val split = dL.splitAt(splitIndex)

          val contigName: String = split._1.stripPrefix(">").trim
          val contigDescription: String = split._2.trim

          (Some(contigName), Some(contigDescription))

        } else {
          (Some(dL.stripPrefix(">").trim), None)
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
   * @throws IllegalArgumentException Thrown if a sequence does not have sequence data.
   *
   * @param rdd RDD containing Long,String tuples, where the Long corresponds to the number
   * of the file line, and the String is the line of the file.
   * @param maxFragmentLength The maximum length of fragments in the contig.
   * @return An RDD of ADAM FASTA data.
   */
  def apply(
    rdd: RDD[(Long, String)],
    maxFragmentLength: Long = 10000L): RDD[NucleotideContigFragment] = {
    val filtered = rdd.map(kv => (kv._1, kv._2.trim()))
      .filter((kv: (Long, String)) => !kv._2.startsWith(";"))

    val descriptionLines: Map[Long, FastaDescriptionLine] = getDescriptionLines(filtered)
    val indexToContigDescription = rdd.context.broadcast(descriptionLines)

    val sequenceLines = filtered.filter(kv => !isDescriptionLine(kv._2))

    val keyedSequences =
      if (indexToContigDescription.value.isEmpty) {
        sequenceLines.keyBy(kv => -1L)
      } else {
        sequenceLines.keyBy(row => findContigIndex(row._1, indexToContigDescription.value.keys.toList))
      }

    val groupedContigs = keyedSequences.groupByKey()

    val converter = new FastaConverter(maxFragmentLength)

    groupedContigs.flatMap {
      case (id, lines) =>

        val descriptionLine = indexToContigDescription.value.getOrElse(id, FastaDescriptionLine())
        assert(lines.nonEmpty, s"Sequence ${descriptionLine.seqId} has no sequence data.")

        val sequence: Seq[String] = lines.toSeq.sortBy(_._1).map(kv => cleanSequence(kv._2))
        converter.convert(
          descriptionLine.contigName,
          descriptionLine.seqId,
          sequence,
          descriptionLine.contigDescription
        )
    }
  }

  /**
   * Cleans up a sequence by stripping asterisks at the end of the sequence.
   *
   * To be consistent with a legacy database, some FASTA sequences end in a "*"
   * suffix. This method strips that suffix from the end of the sequence.
   *
   * @param sequence The sequence to clean.
   * @return Sequence minus "*" suffix.
   */
  private def cleanSequence(sequence: String): String = {
    sequence.stripSuffix("*")
  }

  /**
   * A FASTA line starting with ">" is a description line.
   *
   * @param line The line to check.
   * @return True if the line starts with ">" and is thus a description line.
   */
  private def isDescriptionLine(line: String): Boolean = {
    line.startsWith(">")
  }

  /**
   * Gets the description lines in a FASTA file.
   *
   * Filters an input RDD that contains (line number, line) pairs and returns
   * all lines that are descriptions of a sequence.
   *
   * @param rdd RDD of (line number, line string) pairs to filter.
   * @return Returns a map that maps sequence IDs to description lines.
   */
  private[converters] def getDescriptionLines(rdd: RDD[(Long, String)]): Map[Long, FastaDescriptionLine] = {

    rdd.filter(kv => isDescriptionLine(kv._2))
      .collect()
      .zipWithIndex
      .map(kv => (kv._1._1, FastaDescriptionLine(kv._1._1, kv._2, Some(kv._1._2))))
      .toMap
  }

  /**
   * Finds the index of a contig.
   *
   * The index of a contig is the highest index below the index of our row.
   * Here, we define the index as the row number of the description line that
   * describes this contig.
   *
   * @param rowIdx The row number of the contig row to check.
   * @param indices A list containing the row numbers of all description lines.
   * @return Returns the row index of the description line that describes this
   *   sequence line.
   */
  private[converters] def findContigIndex(rowIdx: Long, indices: List[Long]): Long = {
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
    if (sequence.nonEmpty) {
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
  def convert(
    name: Option[String],
    id: Int,
    sequence: Seq[String],
    description: Option[String]): Seq[NucleotideContigFragment] = {

    // get sequence length
    val sequenceLength = sequence.map(_.length).sum

    // map sequences into fragments
    val sequencesAsFragments = mapFragments(sequence)

    // get number of fragments
    val fragmentCount = sequencesAsFragments.length

    // make new builder and set up non-optional fields
    val fragments = sequencesAsFragments.zipWithIndex
      .map(si => {
        val (bases, index) = si

        val contig = Contig.newBuilder
          .setContigLength(sequenceLength)

        val builder = NucleotideContigFragment.newBuilder()
          .setFragmentSequence(bases)
          .setFragmentNumber(index)
          .setFragmentStartPosition(index * fragmentLength)
          .setFragmentEndPosition(index * fragmentLength + bases.length - 1L)
          .setNumberOfFragmentsInContig(fragmentCount)
          .setFragmentLength(bases.length)

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
