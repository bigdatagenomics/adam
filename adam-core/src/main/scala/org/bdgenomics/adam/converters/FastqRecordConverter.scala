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

import grizzled.slf4j.Logging
import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.Text
import org.bdgenomics.formats.avro.{
  Alignment,
  Fragment
}
import scala.collection.JavaConversions._

/**
 * Utility class for converting FASTQ formatted data.
 *
 * FASTQ format is:
 *
 * {{{
 * @readName<optional whitespace read:is filtered:control number:sample number>
 * sequence
 * +<optional readname>
 * ASCII quality scores
 * }}}
 */
private[adam] class FastqRecordConverter extends Serializable with Logging {

  private val firstReadSuffix = """[/ +_]1$"""
  private val secondReadSuffix = """[/ +_]2$"""
  private val illuminaMetadata = """ [12]:[YN]:[02468]+:[0-9ACTNG+]+$"""
  private val firstReadRegex = firstReadSuffix.r
  private val secondReadRegex = secondReadSuffix.r
  private val suffixRegex = "%s|%s|%s".format(firstReadSuffix,
    secondReadSuffix,
    illuminaMetadata).r

  /**
   * @param readName The name of the read.
   * @param isFirstOfPair True if this read is the first read in a paired sequencing fragment.
   * @throws IllegalArgumentException if the read name suffix and flags match.
   */
  private[converters] def readNameSuffixAndIndexOfPairMustMatch(readName: String,
                                                                isFirstOfPair: Boolean) {
    val isSecondOfPair = !isFirstOfPair

    val match1 = firstReadRegex.findAllIn(readName)
    val match2 = secondReadRegex.findAllIn(readName)

    if (match1.nonEmpty && isSecondOfPair)
      throw new IllegalArgumentException(
        s"Found read name $readName ending in ${match1.next} despite first-of-pair flag being set")
    else if (match2.nonEmpty && isFirstOfPair)
      throw new IllegalArgumentException(
        s"Found read name $readName ending in ${match2.next} despite second-of-pair flag being set")
    // else, readName doesn't really tell whether it's first or second of pair, assumed to match
  }

  private[converters] def parseReadInFastq(input: String,
                                           setFirstOfPair: Boolean = false,
                                           setSecondOfPair: Boolean = false,
                                           stringency: ValidationStringency = ValidationStringency.STRICT): (String, String, String) = {
    // since it's a private method, simple require call is ok without detailed error message
    require(!(setFirstOfPair && setSecondOfPair))

    val lines = input.split('\n')
    require(lines.length == 4 || (lines.length == 3 && input.endsWith("\n\n")),
      s"Input must have 4 lines (${lines.length.toString} found):\n${input}")

    val readName = lines(0).drop(1)
    if (setFirstOfPair || setSecondOfPair) {
      try {
        readNameSuffixAndIndexOfPairMustMatch(readName, setFirstOfPair)
      } catch {
        case e: IllegalArgumentException => {
          // if we are lenient we log, strict we rethrow, silent we ignore 
          if (stringency == ValidationStringency.STRICT) {
            throw e
          } else if (stringency == ValidationStringency.LENIENT) {
            warn("Read had improper pair suffix: %s".format(e.getMessage))
          }
        }
      }
    }

    val readNameNoSuffix = suffixRegex.replaceAllIn(readName, "")

    val readSequence = lines(1)
    val readQualityScoresRaw = if (lines.length == 4) {
      lines(3)
    } else {
      ""
    }

    val readQualityScores =
      if (stringency == ValidationStringency.STRICT) readQualityScoresRaw
      else {
        if (readQualityScoresRaw == "*") "B" * readSequence.length
        else if (readQualityScoresRaw.length < readSequence.length) {
          readQualityScoresRaw + ("B" * (readSequence.length - readQualityScoresRaw.length))
        } else if (readQualityScoresRaw.length > readSequence.length) {
          throw new IllegalArgumentException("Quality scores length must not be longer than read length")
        } else readQualityScoresRaw
      }

    if (stringency == ValidationStringency.STRICT) {
      if (readQualityScoresRaw == "*" && readSequence.length > 1)
        throw new IllegalArgumentException(s"Fastq quality must be defined for\n $input")
    }

    require(
      readSequence.length == readQualityScores.length,
      s"The first read: ${readName}, has different sequence and qual length."
    )

    (readNameNoSuffix, readSequence, readQualityScores)
  }

  private[converters] def parseReadPairInFastq(input: String): (String, String, String, String, String, String) = {
    val lines = input.toString.split('\n')
    require(lines.length == 8,
      s"Record must have 8 lines (${lines.length.toString} found):\n${input}")

    val (firstReadName, firstReadSequence, firstReadQualityScores) =
      parseReadInFastq(lines.take(4).mkString("\n"), setFirstOfPair = true, setSecondOfPair = false)

    val (secondReadName, secondReadSequence, secondReadQualityScores) =
      parseReadInFastq(lines.drop(4).mkString("\n"), setFirstOfPair = false, setSecondOfPair = true)

    (
      firstReadName,
      firstReadSequence,
      firstReadQualityScores,
      secondReadName,
      secondReadSequence,
      secondReadQualityScores
    )
  }

  private[converters] def makeAlignment(readName: String,
                                        sequence: String,
                                        qualityScores: String,
                                        readInFragment: Int,
                                        readPaired: Boolean = true,
                                        optReadGroup: Option[String] = None): Alignment = {
    val builder = Alignment.newBuilder
      .setReadName(readName)
      .setSequence(sequence)
      .setQualityScores(qualityScores)
      .setReadPaired(readPaired)
      .setReadInFragment(readInFragment)

    optReadGroup.foreach(builder.setReadGroupId)

    builder.build
  }

  /**
   * @param readName The read name to possibly trim.
   * @return If the read name ends in /1 or /2, this suffix is trimmed.
   */
  def maybeTrimSuffix(readName: String): String = {
    if (readName.endsWith("/1") || readName.endsWith("/2")) {
      readName.dropRight(2)
    } else {
      readName
    }
  }

  /**
   * Converts a read pair in FASTQ format into two Alignments.
   *
   * Used for processing a single fragment of paired end sequencing data stored
   * in interleaved FASTQ. While interleaved FASTQ is not an "official" format,
   * it is relatively common in the wild. As the name implies, the reads from
   * a single sequencing fragment are interleaved in a single file, and are not
   * split across two files.
   *
   * @param element Key-value pair of (void, and the FASTQ text). The text
   *   should correspond to exactly two records.
   * @return Returns a length = 2 iterable of Alignments.
   *
   * @throws IllegalArgumentException Throws if records are misformatted. Each
   *   record must be 4 lines, and sequence and quality must be the same length.
   *
   * @see convertFragment
   */
  def convertPair(element: (Void, Text)): Iterable[Alignment] = {
    val (
      firstReadName,
      firstReadSequence,
      firstReadQualityScores,
      secondReadName,
      secondReadSequence,
      secondReadQualityScores
      ) = parseReadPairInFastq(element._2.toString)

    // build and return iterators
    Iterable(
      makeAlignment(firstReadName, firstReadSequence, firstReadQualityScores, 0),
      makeAlignment(secondReadName, secondReadSequence, secondReadQualityScores, 1)
    )
  }

  /**
   * Converts a read pair in FASTQ format into a Fragment.
   *
   * @param element Key-value pair of (void, and the FASTQ text). The text
   *   should correspond to exactly two records.
   * @return Returns a single Fragment containing two reads..
   *
   * @throws IllegalArgumentException Throws if records are misformatted. Each
   *   record must be 4 lines, and sequence and quality must be the same length.
   *
   * @see convertPair
   */
  def convertFragment(element: (Void, Text)): Fragment = {
    val (
      firstReadName,
      firstReadSequence,
      firstReadQualityScores,
      secondReadName,
      secondReadSequence,
      secondReadQualityScores
      ) = parseReadPairInFastq(element._2.toString)

    require(
      firstReadName == secondReadName,
      "Reads %s and %s in Fragment have different names.".format(
        firstReadName,
        secondReadName
      )
    )

    val alignments = List(
      makeAlignment(firstReadName, firstReadSequence, firstReadQualityScores, 0),
      makeAlignment(secondReadName, secondReadSequence, secondReadQualityScores, 1)
    )

    // build and return record
    Fragment.newBuilder
      .setName(firstReadName)
      .setAlignments(alignments)
      .build
  }

  /**
   * Converts a single FASTQ read into ADAM format.
   *
   * Used for processing a single fragment of paired end sequencing data stored
   * in interleaved FASTQ. While interleaved FASTQ is not an "official" format,
   * it is relatively common in the wild. As the name implies, the reads from
   * a single sequencing fragment are interleaved in a single file, and are not
   * split across two files.
   *
   * @param element Key-value pair of (void, and the FASTQ text). The text
   *   should correspond to exactly two records.
   * @return Returns a length = 2 iterable of Alignments.
   *
   * @throws IllegalArgumentException Throws if records are misformatted. Each
   *   record must be 4 lines, and sequence and quality must be the same length.
   *
   * @see convertFragment
   */
  def convertRead(
    element: (Void, Text),
    optReadGroup: Option[String] = None,
    setFirstOfPair: Boolean = false,
    setSecondOfPair: Boolean = false,
    stringency: ValidationStringency = ValidationStringency.STRICT): Alignment = {
    if (setFirstOfPair && setSecondOfPair)
      throw new IllegalArgumentException("setFirstOfPair and setSecondOfPair cannot be true at the same time")

    val (readName, readSequence, readQualityScores) =
      parseReadInFastq(element._2.toString, setFirstOfPair, setSecondOfPair, stringency)

    // default to 0
    val readInFragment =
      if (setSecondOfPair) 1
      else 0

    val readPaired = setFirstOfPair || setSecondOfPair

    makeAlignment(
      readName, readSequence, readQualityScores,
      readInFragment, readPaired, optReadGroup)
  }
}
