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

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.Text
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  Fragment
}
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._

/**
 * Utility class for converting FASTQ formatted data.
 *
 * FASTQ format is:
 *
 * {{{
 * @readName
 * sequence
 * +<optional readname>
 * ASCII quality scores
 * }}}
 */
private[adam] class FastqRecordConverter extends Serializable with Logging {

  /**
   * Converts a read pair in FASTQ format into two AlignmentRecords.
   *
   * Used for processing a single fragment of paired end sequencing data stored
   * in interleaved FASTQ. While interleaved FASTQ is not an "official" format,
   * it is relatively common in the wild. As the name implies, the reads from
   * a single sequencing fragment are interleaved in a single file, and are not
   * split across two files.
   *
   * @param element Key-value pair of (void, and the FASTQ text). The text
   *   should correspond to exactly two records.
   * @return Returns a length = 2 iterable of AlignmentRecords.
   *
   * @throws IllegalArgumentException Throws if records are misformatted. Each
   *   record must be 4 lines, and sequence and quality must be the same length.
   *
   * @see convertFragment
   */
  def convertPair(element: (Void, Text)): Iterable[AlignmentRecord] = {
    val lines = element._2.toString.split('\n')
    require(lines.length == 8, "Record has wrong format:\n" + element._2.toString)

    // get fields for first read in pair
    val firstReadName = lines(0).drop(1)
    val firstReadSequence = lines(1)
    val firstReadQualities = lines(3)

    require(
      firstReadSequence.length == firstReadQualities.length,
      "Read " + firstReadName + " has different sequence and qual length."
    )

    // get fields for second read in pair
    val secondReadName = lines(4).drop(1)
    val secondReadSequence = lines(5)
    val secondReadQualities = lines(7)

    require(
      secondReadSequence.length == secondReadQualities.length,
      "Read " + secondReadName + " has different sequence and qual length."
    )

    // build and return iterators
    Iterable(
      AlignmentRecord.newBuilder()
        .setReadName(firstReadName)
        .setSequence(firstReadSequence)
        .setQual(firstReadQualities)
        .setReadPaired(true)
        .setProperPair(true)
        .setReadInFragment(0)
        .setReadNegativeStrand(null)
        .setMateNegativeStrand(null)
        .setPrimaryAlignment(null)
        .setSecondaryAlignment(null)
        .setSupplementaryAlignment(null)
        .build(),
      AlignmentRecord.newBuilder()
        .setReadName(secondReadName)
        .setSequence(secondReadSequence)
        .setQual(secondReadQualities)
        .setReadPaired(true)
        .setProperPair(true)
        .setReadInFragment(1)
        .setReadNegativeStrand(null)
        .setMateNegativeStrand(null)
        .setPrimaryAlignment(null)
        .setSecondaryAlignment(null)
        .setSupplementaryAlignment(null)
        .build()
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
    val lines = element._2.toString.split('\n')
    require(lines.length == 8, "Record has wrong format:\n" + element._2.toString)

    // get fields for first read in pair
    val firstReadName = lines(0).drop(1)
    val firstReadSequence = lines(1)
    val firstReadQualities = lines(3)

    require(
      firstReadSequence.length == firstReadQualities.length,
      "Read " + firstReadName + " has different sequence and qual length."
    )

    // get fields for second read in pair
    val secondReadName = lines(4).drop(1)
    val secondReadSequence = lines(5)
    val secondReadQualities = lines(7)

    require(
      secondReadSequence.length == secondReadQualities.length,
      "Read " + secondReadName + " has different sequence and qual length."
    )
    require(
      firstReadName == secondReadName,
      "Reads %s and %s in Fragment have different names.".format(
        firstReadName,
        secondReadName
      )
    )

    // build and return record
    Fragment.newBuilder()
      .setReadName(firstReadName)
      .setAlignments(List(AlignmentRecord.newBuilder()
        .setSequence(firstReadSequence)
        .setQual(firstReadQualities)
        .build(), AlignmentRecord.newBuilder()
        .setSequence(secondReadSequence)
        .setQual(secondReadQualities)
        .build()))
      .build()
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
   * @return Returns a length = 2 iterable of AlignmentRecords.
   *
   * @throws IllegalArgumentException Throws if records are misformatted. Each
   *   record must be 4 lines, and sequence and quality must be the same length.
   *
   * @see convertFragment
   */
  def convertRead(
    element: (Void, Text),
    recordGroupOpt: Option[String] = None,
    setFirstOfPair: Boolean = false,
    setSecondOfPair: Boolean = false,
    stringency: ValidationStringency = ValidationStringency.STRICT): AlignmentRecord = {
    val lines = element._2.toString.split('\n')
    require(lines.length == 4, "Record has wrong format:\n" + element._2.toString)

    def trimTrailingReadNumber(readName: String): String = {
      if (readName.endsWith("/1")) {
        if (setSecondOfPair) {
          throw new Exception(
            s"Found read name $readName ending in '/1' despite second-of-pair flag being set"
          )
        }
        readName.dropRight(2)
      } else if (readName.endsWith("/2")) {
        if (setFirstOfPair) {
          throw new Exception(
            s"Found read name $readName ending in '/2' despite first-of-pair flag being set"
          )
        }
        readName.dropRight(2)
      } else {
        readName
      }
    }

    // get fields for first read in pair
    val readName = trimTrailingReadNumber(lines(0).drop(1))
    val readSequence = lines(1)

    lazy val suffix = s"\n=== printing received Fastq record for debugging ===\n${lines.mkString("\n")}\n=== End of debug output for Fastq record ==="
    if (stringency == ValidationStringency.STRICT && lines(3) == "*" && readSequence.length > 1)
      throw new IllegalArgumentException(s"Fastq quality must be defined. $suffix")
    else if (stringency == ValidationStringency.STRICT && lines(3).length != readSequence.length)
      throw new IllegalArgumentException(s"Fastq sequence and quality strings must have the same length.\n Fastq quality string of length ${lines(3).length}, expected ${readSequence.length} from the sequence length. $suffix")

    val readQualities =
      if (lines(3) == "*")
        "B" * readSequence.length
      else if (lines(3).length < lines(1).length)
        lines(3) + ("B" * (lines(1).length - lines(3).length))
      else if (lines(3).length > lines(1).length)
        throw new NotImplementedError("Not implemented")
      else
        lines(3)

    require(
      readSequence.length == readQualities.length,
      List(
        s"Read $readName has different sequence and qual length:",
        s"sequence=$readSequence",
        s"qual=$readQualities"
      ).mkString("\n\t")
    )

    val builder = AlignmentRecord.newBuilder()
      .setReadName(readName)
      .setSequence(readSequence)
      .setQual(readQualities)
      .setReadPaired(setFirstOfPair || setSecondOfPair)
      .setProperPair(null)
      .setReadInFragment(
        if (setFirstOfPair) 0
        else if (setSecondOfPair) 1
        else null
      )
      .setReadNegativeStrand(false)
      .setMateNegativeStrand(null)
      .setPrimaryAlignment(null)
      .setSecondaryAlignment(null)
      .setSupplementaryAlignment(null)

    recordGroupOpt.foreach(builder.setRecordGroupName)

    builder.build()
  }
}
