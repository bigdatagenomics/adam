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
import org.apache.spark.broadcast.Broadcast
import org.bdgenomics.formats.avro.{ Alphabet, Sequence, Slice }
import scala.collection.immutable.SortedMap
import scala.collection.mutable.MutableList

/**
 * FASTA format description line.
 *
 * @param index Index of the description line.
 * @param name Name parsed from the description line.
 * @param description Description parsed from the description line.
 */
private[adam] case class FastaDescriptionLine(
    index: Long = -1L,
    name: Option[String] = None,
    description: Option[String] = None) {
}

/**
 * FASTA format converters.
 */
private[converters] object FastaConverters {
  /**
   * Regular expression to identify valid FASTA format lines, primarly of use when
   * reading FASTA formatted sequences from GFF3 files.  Nucleotide symbols, amino acid
   * symbols, and ambiguity code symbols cover a-z. Also valid are whitespace, gap
   * symbol -, and translation stop *.
   */
  private val fastaRegex = "^[a-zA-Z-*\\s]+$".r

  /**
   * Return true if the specified line is a description line.
   *
   * @param line FASTA format line.
   * @return Return true if the specified line is a description line.
   */
  def isDescriptionLine(line: String): Boolean = {
    line.startsWith(">")
  }

  /**
   * Return true if the specified line is valid FASTA format.
   *
   * @param line FASTA format line.
   * @return Return true if the specified line is valid FASTA format.
   */
  def isFastaLine(line: String): Boolean = {
    isDescriptionLine(line) || fastaRegex.pattern.matcher(line).matches()
  }

  /**
   * Clean up a sequence by stripping asterisks at the end of the sequence.
   *
   * To be consistent with a legacy database, some FASTA sequences end in a "*"
   * suffix. This method strips that suffix from the end of the sequence.
   *
   * @param sequence The sequence to clean.
   * @return Sequence minus "*" suffix.
   */
  def cleanSequence(sequence: String): String = {
    sequence.stripSuffix("*")
  }

  /**
   * Parse the specified line into a description line.
   *
   * @param index Index of the line to parse.
   * @param optLine Line to parse, if any.
   * @return Return the specified line parsed into a description line.
   */
  def parseDescriptionLine(index: Long, optLine: Option[String]): FastaDescriptionLine = {
    optLine.fold {
      require(index == -1L, "Cannot have a headerless line in a file with more than one sequence.")
      FastaDescriptionLine(index, None, None)
    } {
      (line) =>

        // fasta description line splits on whitespace
        val splitIndex = line.indexWhere(c => c.isWhitespace)
        if (splitIndex >= 0) {
          val split = line.splitAt(splitIndex)

          // is this description metadata or not? if it is metadata, it will contain "|"
          if (split._1.contains('|')) {
            FastaDescriptionLine(index, None, Some(line.stripPrefix(">").trim))
          } else {
            val name: String = split._1.stripPrefix(">").trim
            val description: String = split._2.trim

            FastaDescriptionLine(index, Some(name), Some(description))
          }
        } else {
          FastaDescriptionLine(index, Some(line.stripPrefix(">").trim), None)
        }
    }
  }

  /**
   * Filter the specified lines to FASTA format lines.
   *
   * @param rdd Lines keyed by line number.
   * @return Return the specified lines filtered to FASTA format lines.
   */
  def filterToFastaLines(rdd: RDD[(Long, String)]): RDD[(Long, String)] = {
    rdd
      .map(kv => (kv._1, kv._2.trim()))
      .filter((kv: (Long, String)) => isFastaLine(kv._2))
  }

  /**
   * Collect description lines from the specified lines into a sorted map suitable
   * for broadcast.
   *
   * @param rdd Lines keyed by line number.
   * @return Return description lines from the specified lines collected into a sorted
   *    map suitable for broadcast.
   */
  def collectDescriptionLines(rdd: RDD[(Long, String)]): SortedMap[Long, FastaDescriptionLine] = {
    val descriptionLines: SortedMap[Long, FastaDescriptionLine] = SortedMap()

    descriptionLines ++ rdd // Scala 2.13+ only  SortedMap.from(
      .filter(kv => isDescriptionLine(kv._2))
      .map(kv => (kv._1, parseDescriptionLine(kv._1, Some(kv._2))))
      .sortByKey() // not strictly necessary, slightly slower without
      .collect()
  }

  /**
   * Group sequence lines by description line index.
   *
   * @param rdd FASTA format sequence lines keyed by line number.
   * @param broadcast Sorted map of description lines keyed by line number.
   * @return Return sequence lines grouped by description line index.
   */
  def groupByDescriptionLineIndex(rdd: RDD[(Long, String)], broadcast: Broadcast[SortedMap[Long, FastaDescriptionLine]]): RDD[(Long, Iterable[(Long, String)])] = {
    rdd
      .filter(kv => !isDescriptionLine(kv._2))
      //.keyBy(kv => broadcast.value.maxBefore(kv._1).getOrElse(-1L))  Scala 2.13+ only
      .keyBy(kv => broadcast.value.until(kv._1).lastOption.getOrElse((-1L, Nil))._1)
      .groupByKey()
  }
}

/**
 * Convert FASTA format into Sequences.
 */
private[adam] object FastaSequenceConverter {
  import FastaConverters._

  /**
   * Convert FASTA format into Sequences.
   *
   * @param alphabet Alphabet for the sequences.
   * @param rdd RDD of FASTA format as (Long, String) tuples.
   * @return Return an RDD of Sequences converted from FASTA format.
   */
  def apply(alphabet: Alphabet, rdd: RDD[(Long, String)]): RDD[Sequence] = {

    val filtered = filterToFastaLines(rdd)
    val descriptionLines = collectDescriptionLines(filtered)
    val broadcast = rdd.context.broadcast(descriptionLines)
    val sequenceLines = groupByDescriptionLineIndex(filtered, broadcast)

    sequenceLines.flatMap {
      case (index: Long, lines) =>

        val descriptionLine = broadcast.value.getOrElse(index, FastaDescriptionLine())

        val sequence = lines.toSeq.sortBy(_._1).map(kv => cleanSequence(kv._2)).mkString
        val length = sequence.length().toLong

        val builder = Sequence.newBuilder()
          .setAlphabet(alphabet)
          .setSequence(sequence)
          .setLength(length)

        // map over optional fields
        descriptionLine.name.foreach(builder.setName(_))
        descriptionLine.description.foreach(builder.setDescription(_))

        Seq(builder.build)
    }
  }
}

/**
 * Convert FASTA format into Slices.
 */
private[adam] object FastaSliceConverter {
  import FastaConverters._

  /**
   * Convert FASTA format into Slices.
   *
   * @param rdd RDD of FASTA format as (Long, String) tuples.
   * @param maximumLength Maximum length in bases for each Slice.
   * @return Return an RDD of Slices converted from FASTA format.
   */
  def apply(rdd: RDD[(Long, String)], maximumLength: Long): RDD[Slice] = {

    val filtered = filterToFastaLines(rdd)
    val descriptionLines = collectDescriptionLines(filtered)
    val broadcast = rdd.context.broadcast(descriptionLines)
    val sequenceLines = groupByDescriptionLineIndex(filtered, broadcast)

    sequenceLines.flatMap {
      case (index: Long, lines) =>

        val descriptionLine = broadcast.value.getOrElse(index, FastaDescriptionLine())

        val sequences = lines.toSeq.sortBy(_._1).map(kv => cleanSequence(kv._2))
        val length = sequences.map(_.length).sum

        val sequenceSlices = slice(sequences, maximumLength)
        val sliceCount = sequenceSlices.length

        val slices = sequenceSlices.zipWithIndex
          .map(si => {
            val (bases, index) = si

            val builder = Slice.newBuilder()
              .setAlphabet(Alphabet.DNA)
              .setSequence(bases)
              .setIndex(index)
              .setStart(index * maximumLength)
              .setEnd(index * maximumLength + bases.length)
              .setSlices(sliceCount)
              .setLength(bases.length.toLong)
              .setTotalLength(length.toLong)

            // map over optional fields
            descriptionLine.name.foreach(builder.setName(_))
            descriptionLine.description.foreach(builder.setDescription(_))

            builder.build
          })

        slices
    }
  }

  /**
   * Slice the specified sequence strings per the specified maximum length.
   *
   * @param sequences Sequence strings to slice.
   * @param maximumLength Maximum length in bases for each slice.
   * @return Return the specified sequence strings sliced per the specified maximum length.
   */
  private def slice(sequences: Seq[String], maximumLength: Long): Seq[String] = {
    // internal "fsm" variables
    var sequence: StringBuilder = new StringBuilder
    var sequenceSeq: MutableList[String] = MutableList()

    /**
     * Adds a string slice to our accumulator. If this string slice causes the accumulator
     * to grow longer than our max slice size, we split the accumulator and add it to the end
     * of our list of slice.
     *
     * @param seq Slice string to add.
     */
    def addSlice(seq: String) {
      sequence.append(seq)

      while (sequence.length > maximumLength) {
        sequenceSeq += sequence.take(maximumLength.toInt).toString()
        sequence = sequence.drop(maximumLength.toInt)
      }
    }

    // run addFragment on all slices
    sequences.foreach(addSlice)

    // if we still have a remaining sequence that is not part of a slice, add it
    if (sequence.nonEmpty) {
      sequenceSeq += sequence.toString()
    }

    // return our slices
    sequenceSeq
  }
}
