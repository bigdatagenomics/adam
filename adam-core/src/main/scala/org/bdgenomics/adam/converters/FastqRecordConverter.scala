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

import org.apache.hadoop.io.Text
import org.apache.spark.Logging
import org.bdgenomics.formats.avro.AlignmentRecord
import java.util.regex._

class FastqRecordConverter extends Serializable with Logging {

  def convertPair(element: (Void, Text)): Iterable[AlignmentRecord] = {
    val temp = element._2.toString.split('\n')
    var lines = Array[String]("")
    //true if read is multiline
    if (temp.length > 8) {
      lines = parseMultiLine(element._2.toString)
    } else {
      lines = temp
    }
    assert(lines.length == 8, "Record has wrong format:\n" + lines(0) + " " + lines(1) + " " + lines.length)

    // get fields for first read in pair
    val firstReadName = lines(0).drop(1)
    val firstReadSequence = lines(1)
    val firstReadQualities = lines(3)

    assert(firstReadSequence.length == firstReadQualities.length,
      "Read " + firstReadName + " has different sequence and qual length.")

    // get fields for second read in pair
    val secondReadName = lines(4).drop(1)
    val secondReadSequence = lines(5)
    val secondReadQualities = lines(7)

    assert(secondReadSequence.length == secondReadQualities.length,
      "Read " + secondReadName + " has different sequence and qual length.")

    // build and return iterators
    Iterable(AlignmentRecord.newBuilder()
      .setReadName(firstReadName)
      .setSequence(firstReadSequence)
      .setQual(firstReadQualities)
      .setReadPaired(true)
      .setProperPair(true)
      .setFirstOfPair(true)
      .setSecondOfPair(false)
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
        .setFirstOfPair(false)
        .setSecondOfPair(true)
        .setReadNegativeStrand(null)
        .setMateNegativeStrand(null)
        .setPrimaryAlignment(null)
        .setSecondaryAlignment(null)
        .setSupplementaryAlignment(null)
        .build())
  }

  def parseMultiLine(element: String): Array[String] = {
    //regex string for a new line that is followed by a +
    val fastqRegex = "\\n(?=\\+)"

    //The read can be split at the fist new line, since the @title can only be one line long
    val splitOne = element.split("\\n", 2)

    //We don't know how many lines make up the sequence data, but we know it is ended by a new line followed by a +
    //This pattern will not show up in the sequence data itself, since it is limited to IUPAC codes
    val splitTwo = splitOne(1).split(fastqRegex, 2)

    //The read can again be split at the new line, since the +title can only be one line
    val splitThree = splitTwo(1).split("\\n", 2)

    //get the sequence data's length
    val count = splitTwo(0).length

    //The sequence and quality lines have the same length, so 'count' tells us 
    //where the quality line ends. 'count +1' includes the newline in 'qual'
    val (qual, rest) = splitThree(1).splitAt(count + 1)

    //create the full read, and remove newline characters
    val read = Array(splitOne(0), splitTwo(0).filterNot(_ == '\n'), splitThree(0), qual.filterNot(_ == '\n'))

    //test if there is more data to be read (i.e. the second half of an interleaved read)
    if (rest.filterNot(_ == '\n').isEmpty) {
      //there is none, so the read is returned
      return read
    } else {
      //interleaved read. Process the rest of it
      val list = parseMultiLine(rest.toString)
      return Array(read, list).flatten
    }

  }

  def convertRead(element: (Void, Text)): AlignmentRecord = {
    val temp = element._2.toString.split('\n')
    var lines = Array[String]("")
    //true if read is multiline
    if (temp.length > 4) {
      lines = parseMultiLine(element._2.toString)
    } else {
      lines = temp
    }

    assert(lines.length == 4, "Record has wrong format:\n" + element._2.toString)

    // get fields for first read in pair
    val readName = lines(0).drop(1)
    val readSequence = lines(1)
    val readQualities = lines(3)

    assert(readSequence.length == readQualities.length,
      "Read " + readName + " has different sequence and qual length.")

    AlignmentRecord.newBuilder()
      .setReadName(readName)
      .setSequence(readSequence)
      .setQual(readQualities)
      .setReadPaired(false)
      .setProperPair(null)
      .setFirstOfPair(null)
      .setSecondOfPair(null)
      .setReadNegativeStrand(null)
      .setMateNegativeStrand(null)
      .setPrimaryAlignment(null)
      .setSecondaryAlignment(null)
      .setSupplementaryAlignment(null)
      .build()
  }
}
