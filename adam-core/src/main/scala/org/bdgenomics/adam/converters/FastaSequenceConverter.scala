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
import org.bdgenomics.formats.avro.{ Alphabet, Sequence }

/**
 * Object for converting an RDD containing FASTA sequence data into ADAM FASTA data.
 */
private[adam] object FastaSequenceConverter {

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
     * The reference name and description that was parsed out of this description line.
     */
    val (referenceName, referenceDescription) = parseDescriptionLine(descriptionLine, fileIndex)

    /**
     * Parses the text of a given line.
     *
     * Assumes that the line contains the reference name followed by an optional
     * description of the reference, with the two separated by a space.
     *
     * @throws IllegalArgumentException if there is no name in the line and the
     *   line is not the only record in a file (i.e., the file contains multiple
     *   references).
     * @param descriptionLine The optional string describing the reference. If this
     *   is not set and this isn't the only line in the file, we throw.
     * @param id The index of this reference in the file.
     * @return Returns a tuple containing (the optional reference name, and the
     *   optional reference description).
     */
    private def parseDescriptionLine(descriptionLine: Option[String],
                                     id: Long): (Option[String], Option[String]) = {
      descriptionLine.fold {
        require(id == -1L, "Cannot have a headerless line in a file with more than one sequence.")
        (None: Option[String], None: Option[String])
      } { (dL) =>
        // fasta description line splits on whitespace
        val splitIndex = dL.indexWhere(c => c.isWhitespace)
        if (splitIndex >= 0) {
          val split = dL.splitAt(splitIndex)

          // is this description metadata or not? if it is metadata, it will contain "|"
          if (split._1.contains('|')) {
            (None, Some(dL.stripPrefix(">").trim))
          } else {
            val referenceName: String = split._1.stripPrefix(">").trim
            val referenceDescription: String = split._2.trim

            (Some(referenceName), Some(referenceDescription))
          }
        } else {
          (Some(dL.stripPrefix(">").trim), None)
        }
      }
    }
  }

  /**
   * Converts an RDD containing ints and strings into an RDD containing ADAM sequences.
   *
   * @note Input dataset is assumed to have come in from a Hadoop TextInputFormat reader. This sets
   *   a specific format for the RDD's Key-Value pairs.
   * @throws AssertionError Thrown if there appear to be multiple sequences in a single file
   *   that do not have descriptions.
   * @throws IllegalArgumentException Thrown if a sequence does not have sequence data.
   * @param alphabet Alphabet in which to interpret the sequences to convert.
   * @param rdd RDD containing Long,String tuples, where the Long corresponds to the number
   *   of the file line, and the String is the line of the file.
   * @return An RDD of ADAM FASTA data.
   */
  def apply(alphabet: Alphabet, rdd: RDD[(Long, String)]): RDD[Sequence] = {
    val filtered = rdd.map(kv => (kv._1, kv._2.trim()))
      .filter((kv: (Long, String)) => !kv._2.startsWith(";"))

    val descriptionLines: Map[Long, FastaDescriptionLine] = getDescriptionLines(filtered)
    val indexToReferenceDescription = rdd.context.broadcast(descriptionLines)

    val sequenceLines = filtered.filter(kv => !isDescriptionLine(kv._2))

    val keyedSequences =
      if (indexToReferenceDescription.value.isEmpty) {
        sequenceLines.keyBy(kv => -1L)
      } else {
        sequenceLines.keyBy(row => findReferenceIndex(row._1, indexToReferenceDescription.value.keys.toList))
      }

    val groupedReferences = keyedSequences.groupByKey()

    val converter = new FastaSequenceConverter(alphabet)

    groupedReferences.flatMap {
      case (id, lines) =>

        val descriptionLine = indexToReferenceDescription.value.getOrElse(id, FastaDescriptionLine())
        assert(lines.nonEmpty, s"Sequence ${descriptionLine.seqId} has no sequence data.")

        val sequence: Seq[String] = lines.toSeq.sortBy(_._1).map(kv => cleanSequence(kv._2))
        converter.convert(
          descriptionLine.referenceName,
          descriptionLine.seqId,
          sequence,
          descriptionLine.referenceDescription
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
   * Finds the index of a reference.
   *
   * The index of a reference is the highest index below the index of our row.
   * Here, we define the index as the row number of the description line that
   * describes this reference.
   *
   * @param rowIdx The row number of the reference row to check.
   * @param indices A list containing the row numbers of all description lines.
   * @return Returns the row index of the description line that describes this
   *   sequence line.
   */
  private[converters] def findReferenceIndex(rowIdx: Long, indices: List[Long]): Long = {
    val idx = indices.filter(_ <= rowIdx)
    idx.max
  }
}

/**
 * Conversion methods for single FASTA sequences into ADAM FASTA data.
 *
 * @param alphabet Alphabet in which to interpret the sequences to convert.
 */
private[converters] class FastaSequenceConverter(alphabet: Alphabet) extends Serializable {

  /**
   * Converts a single FASTA sequence into an ADAM sequence.
   *
   * @throws IllegalArgumentException Thrown if sequence contains an illegal character.
   * @param name String option for the sequence name.
   * @param id Numerical identifier for the sequence.
   * @param sequenceLines Nucleotide sequence lines.
   * @param description Optional description of the sequence.
   * @return The converted ADAM FASTA sequence.
   */
  def convert(
    name: Option[String],
    id: Int,
    sequenceLines: Seq[String],
    description: Option[String]): Seq[Sequence] = {

    val sequence = sequenceLines.mkString
    val length = sequence.length().toLong

    val builder = Sequence.newBuilder()
      .setAlphabet(alphabet)
      .setSequence(sequence)
      .setLength(length)

    // map over optional fields
    name.foreach(builder.setName(_))
    description.foreach(builder.setDescription(_))

    Seq(builder.build())
  }
}
