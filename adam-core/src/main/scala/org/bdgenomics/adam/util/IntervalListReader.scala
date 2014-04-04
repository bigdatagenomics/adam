/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.util

import java.io.File
import scala.io._
import scala.collection._
import java.util.regex._
import org.bdgenomics.adam.models.{ SequenceDictionary, SequenceRecord, ReferenceRegion }

/**
 * Reads GATK-style interval list files
 * e.g. example file taken from this page:
 * http://www.broadinstitute.org/gatk/guide/article?id=1204
 *
 * @param file a File whose contents are a (line-based tab-separated) interval file in UTF-8 encoding
 */
class IntervalListReader(file: File) extends Traversable[(ReferenceRegion, String)] {

  val encoding = "UTF-8"
  private var seqDict: SequenceDictionary = null

  /**
   * The interval list file contains a SAM-style sequence dictionary as a header --
   * this value holds that dictionary, reading it if necessary (if the file hasn't been
   * opened before).
   */
  lazy val sequenceDictionary = loadSequenceDictionary()

  private def loadSequenceDictionary(): SequenceDictionary = {
    val headerLines = Source.fromFile(file, encoding).getLines().filter(_.startsWith("@SQ"))
    val headerReader = new HeaderReader()
    headerLines.foreach(headerReader.takeLine)

    headerReader.sequenceDictionary
  }

  def foreach[U](f: ((ReferenceRegion, String)) => U) {
    val lines: Iterator[String] = Source.fromFile(file, encoding).getLines()

    lines.filter(!_.startsWith("@")).foreach {
      line =>
        val array = line.split("\t")
        val refId = array(0).toInt
        val start = array(1).toLong
        val end = array(2).toLong
        val strand = array(3)
        val name = array(4)

        assert(strand == "+")

        f((ReferenceRegion(refId, start, end), name))
    }
  }

  class HeaderReader {

    val sequenceRecords = mutable.ListBuffer[SequenceRecord]()
    val regex = Pattern.compile("(\\w{2}):(.*)")
    var nextSequenceId = 0

    def sequenceDictionary: SequenceDictionary = SequenceDictionary(sequenceRecords: _*)

    def takeLine(line: String) {
      if (line.startsWith("@SQ")) {
        val array: Array[String] = line.split("\t")

        var name: String = null
        var length: Long = null.asInstanceOf[Long]
        var url: String = null

        (1 until array.length).foreach {
          idx =>
            val matcher = regex.matcher(array(idx))
            if (matcher.matches()) {
              val tag = matcher.group(1)
              val arg = matcher.group(2)

              tag match {
                case "SN" => name = arg
                case "UR" => url = arg
                case "LN" => length = arg.toLong
                case _ => None
              }
            }
        }

        sequenceRecords ++= List(SequenceRecord(nextSequenceId, name, length, url))
        nextSequenceId += 1
      }

    }
  }

}
