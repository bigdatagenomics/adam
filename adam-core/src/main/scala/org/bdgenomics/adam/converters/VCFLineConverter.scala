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

import java.io._
import org.bdgenomics.formats.avro.FlatGenotype
import scala.collection.JavaConversions._

/**
 * VCFLineParser is a line-by-line (streaming) parser for VCF files, written specifically to support
 * light-weight creation of FlatGenotype records from large (multi-Gb, gzipped) VCF files.
 * @param inputStream An input stream containing the VCF contents
 * @param sampleSubset An (optional) set of Strings, containing only those samples that should be parsed
 *                     from the file.  If this parameter is non-None, then only those samples (column
 *                     names) in the set will be contained in the VCFLine values produced by this parser.
 */
class VCFLineParser(inputStream: InputStream, sampleSubset: Option[Set[String]] = None)
    extends Iterator[VCFLine] {

  val reader = new BufferedReader(new InputStreamReader(inputStream))
  var nextLine: VCFLine = null
  var samples: Array[String] = null
  var sampleIndices: Seq[Int] = null

  findNextLine()

  private def findNextLine() {
    nextLine = null

    var readLine: String = reader.readLine()
    while (readLine != null && readLine.startsWith("#")) {
      if (readLine.startsWith("#CHROM")) {
        val array = readLine.split("\t")
        val allSamples = array.slice(9, array.length)

        samples = sampleSubset match {
          case Some(sampleSet) => allSamples.filter(sampleSet.contains)
          case None            => allSamples
        }

        sampleIndices = sampleSubset match {
          case Some(sampleSet) => (0 until samples.length).filter(i => sampleSet.contains(samples(i)))
          case None            => 0 until samples.length
        }
      }

      readLine = reader.readLine()
    }

    if (readLine != null) {
      nextLine = new VCFLine(readLine, samples, sampleIndices)
    }
  }

  def next(): VCFLine = {
    val retLine = nextLine
    findNextLine()
    retLine
  }

  def hasNext: Boolean = nextLine != null

  def close() { reader.close() }
}

class VCFLine(vcfLine: String, val samples: Array[String], sampleIndices: Seq[Int]) {

  private val array = vcfLine.split("\t")

  // CHROM POS ID REF ALT QUAL FILTER INFO FORMAT SAMPLES*

  val referenceName = array(0)
  val position = array(1).toInt
  val id = array(2)
  val ref = array(3)
  val alts: List[String] = array(4).split(",").toList
  val qual = array(5) match {
    case "."       => null
    case x: String => x.toDouble
  }
  val filter = array(6)

  val info: Map[String, String] = array(7).split(";").map {
    keyValue =>
      {
        if (keyValue.indexOf("=") != -1) {
          val kvArr = keyValue.split("=")
          kvArr(0) -> kvArr(1)
        } else {
          keyValue -> null
        }
      }
  }.toMap

  val format = array(8).split(":")
  val alleleArray = ref :: alts

  val sampleFields =
    for (i <- sampleIndices)
      yield format.zip(array(9 + i).split(":")).toMap

}

object VCFLineConverter {

  def convert(line: VCFLine): Seq[FlatGenotype] = {

    def buildGenotype(i: Int): Option[FlatGenotype] = {
      val sampleFieldMap = line.sampleFields(i)
      val gtField = sampleFieldMap("GT")

      if (gtField == "./.") {
        None
      } else {
        val gts = sampleFieldMap("GT").split("\\||/").map(_.toInt)
        val genotypes: Seq[String] = gts.map(idx => line.alleleArray(idx))
        val sampleId = line.samples(i)

        val flatGenotype = FlatGenotype.newBuilder()
          .setReferenceName(line.referenceName)
          .setPosition(line.position)
          .setReferenceAllele(line.ref)
          .setSampleId(sampleId)
          .setAlleles(genotypes)
          .build()

        Some(flatGenotype)
      }
    }

    line.samples.zipWithIndex.flatMap {
      case (sample, i) => buildGenotype(i)
    }
  }

}
