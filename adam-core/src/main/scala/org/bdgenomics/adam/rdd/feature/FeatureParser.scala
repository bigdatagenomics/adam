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
package org.bdgenomics.adam.rdd.feature

import htsjdk.samtools.ValidationStringency
import org.bdgenomics.adam.models.SequenceRecord
import org.bdgenomics.formats.avro.Feature
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

private[rdd] sealed trait FeatureParser extends Serializable with Logging {

  /**
   * If the specified validation strategy is STRICT, throw an exception,
   * if LENIENT, log a warning, otherwise return None.
   *
   * @param message error or warning message
   * @param line line
   * @param stringency validation stringency
   * @return None, if stringency is not STRICT or LENIENT
   */
  def throwWarnOrNone(message: String,
                      line: String,
                      stringency: ValidationStringency): Option[Feature] = {

    if (stringency == ValidationStringency.STRICT) {
      throw new IllegalArgumentException(message.format(line))
    } else if (stringency == ValidationStringency.LENIENT) {
      log.warn(message.format(line))
    }
    None
  }

  /**
   * Parse the specified line and return a feature, if possible.
   *
   * @param line line to parse
   * @param stringency validation stringency
   * @return the specified line parsed into a feature, if possible
   */
  def parse(line: String, stringency: ValidationStringency): Option[Feature]
}

private[rdd] object GTFParser {
  private val PATTERN = "\\s*([^\\s]+)\\s\"([^\"]+)\"".r

  /**
   * Split the GTF/GFF2 attributes column by <code>;</code> and whitespace.
   *
   * @param attributes attributes column to split
   * @return the GTF/GFF2 attributes column split by <code>;</code> and whitespace
   */
  def parseAttributes(attributes: String): Seq[(String, String)] =
    attributes.split(";").flatMap {
      case token: String =>
        PATTERN.findFirstMatchIn(token).map(m => (m.group(1), m.group(2)))
    }
}

/**
 * Parser for GTF/GFF2 format.
 *
 * Specification:
 * http://www.ensembl.org/info/website/upload/gff.html
 */
private[rdd] class GTFParser extends FeatureParser {

  def isHeader(line: String): Boolean = {
    line.startsWith("#") || line.isEmpty
  }

  override def parse(line: String,
                     stringency: ValidationStringency): Option[Feature] = {

    // skip header lines
    if (isHeader(line)) {
      return None
    }

    val fields = line.split("\t")

    // check for invalid lines
    if (fields.length < 8 || fields.length > 9) {
      throwWarnOrNone("Invalid GTF/GFF2 line: %s", line, stringency)
    }
    val (seqname, source, featureType, start, end, score, strand, frame) =
      (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7))
    val attributes = if (fields.length == 9) {
      fields(8)
    } else {
      ""
    }

    val f = Feature.newBuilder()
      .setSource(source)
      .setFeatureType(featureType)
      .setReferenceName(seqname)
      .setStart(start.toLong - 1L) // GTF/GFF2 coordinate system is 1-based
      .setEnd(end.toLong) // GTF/GFF2 ranges are closed

    // set score if specified
    if (score != ".") f.setScore(score.toDouble)

    // set frame if specified
    if (frame != ".") f.setFrame(frame.toInt)

    // set strand if specified
    Features.toStrand(strand).foreach(f.setStrand(_))

    // assign values for various feature fields from attributes
    Features.assignAttributes(GTFParser.parseAttributes(attributes), f)

    Some(f.build())
  }
}

private[rdd] object GFF3Parser {

  /**
   * Split the GFF3 attributes column by <code>;</code> and <code>=</code>s.
   *
   * @param attributes attributes column to split
   * @return the GFF3 attributes column split by <code>;</code> and <code>=</code>s
   */
  def parseAttributes(attributes: String): Seq[(String, String)] =
    attributes.split(";")
      .map(s => {
        val eqIdx = s.indexOf("=")
        (s.take(eqIdx), s.drop(eqIdx + 1))
      }).toSeq
}

/**
 * Parser for GFF3 format.
 *
 * Specification:
 * http://www.sequenceontology.org/gff3.shtml
 */
private[rdd] class GFF3Parser extends FeatureParser {

  // nucleotide + amino acid + ambiguity codes covers a-z
  private val fastaRegex = "^[a-zA-Z]+$".r

  def isHeader(line: String): Boolean = {
    line.isEmpty || line.startsWith("#") || line.startsWith(">")
  }

  def isFasta(line: String): Boolean = {
    fastaRegex.pattern.matcher(line).matches()
  }

  override def parse(line: String,
                     stringency: ValidationStringency): Option[Feature] = {

    // skip header lines
    if (isHeader(line)) {
      return None
    }

    val fields = line.split("\t")

    // check for invalid lines
    if (fields.length < 8 || fields.length > 9) {

      // skip FASTA-formatted sequence
      if (isFasta(line)) {
        return None
      }
      throwWarnOrNone("Invalid GFF3 line: %s", line, stringency)
    }
    val (seqid, source, featureType, start, end, score, strand, phase) =
      (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7))
    val attributes = if (fields.length == 9) {
      fields(8)
    } else {
      ""
    }

    val f = Feature.newBuilder()
      .setSource(source)
      .setFeatureType(featureType)
      .setReferenceName(seqid)
      .setStart(start.toLong - 1L) // GFF3 coordinate system is 1-based
      .setEnd(end.toLong) // GFF3 ranges are closed

    // set score if specified
    if (score != ".") f.setScore(score.toDouble)

    // set phase if specified
    if (phase != ".") f.setPhase(phase.toInt)

    // set strand if specified
    Features.toStrand(strand).foreach(f.setStrand(_))

    // assign values for various feature fields from attributes
    Features.assignAttributes(GFF3Parser.parseAttributes(attributes), f)

    Some(f.build())
  }
}

/**
 * Parser for IntervalList format.
 *
 * In lieu of a specification, see:
 * https://samtools.github.io/htsjdk/javadoc/htsjdk/htsjdk/samtools/util/IntervalList.html
 */
private[rdd] class IntervalListParser extends FeatureParser {

  def isHeader(line: String): Boolean = {
    line.startsWith("@")
  }

  def parseWithHeader(line: String,
                      stringency: ValidationStringency): (Option[SequenceRecord], Option[Feature]) = {

    if (isHeader(line)) {
      (parseHeader(line, stringency), None)
    } else {
      (None, parse(line, stringency))
    }
  }

  def parseHeader(line: String,
                  stringency: ValidationStringency): Option[SequenceRecord] = {

    val fields = line.split("[ \t]+")

    if (fields(0).startsWith("@SQ")) {
      val (name, length, url, md5) = {
        val attrs = fields.drop(1).flatMap(field => field.split(":", 2) match {
          case Array(key, value) => Some((key -> value))
          case x => {
            if (stringency == ValidationStringency.STRICT) {
              throw new Exception(s"Expected fields of the form 'key:value' in field $field but got: $x. Line:\n$line")
            } else {
              if (stringency == ValidationStringency.LENIENT) {
                log.warn(s"Expected fields of the form 'key:value' in field $field but got: $x. Line:\n$line")
              }
              None
            }
          }
        }).toMap

        // Require that all @SQ lines have name, length, url, md5.
        (attrs("SN"), attrs("LN").toLong, attrs("UR"), attrs("M5"))
      }
      Some(SequenceRecord(name, length, md5, url))
    } else {
      None
    }
  }

  override def parse(line: String,
                     stringency: ValidationStringency): Option[Feature] = {

    // skip header lines
    if (isHeader(line)) {
      return None
    }

    val fields = line.split("\t")

    // check for empty or invalid lines
    if (fields.length != 5) {
      throwWarnOrNone("Invalid IntervalList line: %s", line, stringency)
    }
    val (referenceName, start, end, strand, featureName) =
      (fields(0), fields(1), fields(2), fields(3), fields(4))

    val f = Feature.newBuilder()
      .setReferenceName(referenceName)
      .setStart(start.toLong - 1) // IntervalList ranges are 1-based
      .setEnd(end.toLong) // IntervalList ranges are closed
      .setName(featureName)

    // set strand if specified
    Features.toStrand(strand).foreach(f.setStrand(_))

    Some(f.build())
  }
}

/**
 * Parser for BED format.
 *
 * Specification:
 * http://www.genome.ucsc.edu/FAQ/FAQformat.html#format1
 */
private[rdd] class BEDParser extends FeatureParser {

  def isHeader(line: String): Boolean = {
    line.startsWith("#") || line.startsWith("browser") || line.startsWith("track") || line.isEmpty
  }

  override def parse(line: String,
                     stringency: ValidationStringency): Option[Feature] = {

    // skip header lines
    if (isHeader(line)) {
      return None
    }

    val fields = line.split("\t")

    // check for invalid lines
    if (fields.length < 3) {
      throwWarnOrNone("Invalid BED line: %s", line, stringency)
    }

    def hasColumn(n: Int): Boolean = {
      fields.length > n && fields(n) != "."
    }

    val f = Feature.newBuilder()
      .setReferenceName(fields(0))
      .setStart(fields(1).toLong) // BED ranges are 0-based
      .setEnd(fields(2).toLong) // BED ranges are closed-open

    if (hasColumn(3)) f.setName(fields(3))
    if (hasColumn(4)) f.setScore(fields(4).toDouble)
    if (fields.length > 5) Features.toStrand(fields(5)).foreach(f.setStrand(_))

    val attributes = new ArrayBuffer[(String, String)]()
    if (hasColumn(6)) attributes += ("thickStart" -> fields(6))
    if (hasColumn(7)) attributes += ("thickEnd" -> fields(7))
    if (hasColumn(8)) attributes += ("itemRgb" -> fields(8))
    if (hasColumn(9)) attributes += ("blockCount" -> fields(9))
    if (hasColumn(10)) attributes += ("blockSizes" -> fields(10))
    if (hasColumn(11)) attributes += ("blockStarts" -> fields(11))

    val attrMap = attributes.toMap
    f.setAttributes(attrMap)

    Some(f.build())
  }
}

/**
 * Parser for NarrowPeak format.
 *
 * Specification:
 * http://www.genome.ucsc.edu/FAQ/FAQformat.html#format12
 */
private[rdd] class NarrowPeakParser extends FeatureParser {

  def isHeader(line: String): Boolean = {
    line.startsWith("#") || line.startsWith("browser") || line.startsWith("track") || line.isEmpty
  }

  override def parse(line: String,
                     stringency: ValidationStringency): Option[Feature] = {

    // skip header lines
    if (isHeader(line)) {
      return None
    }

    val fields = line.split("\t")

    // check for invalid lines
    if (fields.length < 3) {
      throwWarnOrNone("Invalid NarrowPeak line: %s", line, stringency)
    }

    def hasColumn(n: Int): Boolean = {
      fields.length > n && fields(n) != "."
    }

    val f = Feature.newBuilder()
      .setReferenceName(fields(0))
      .setStart(fields(1).toLong) // NarrowPeak ranges are 0-based
      .setEnd(fields(2).toLong) // NarrowPeak ranges are closed-open

    if (hasColumn(3)) f.setName(fields(3))
    if (hasColumn(4)) f.setScore(fields(4).toDouble)
    if (fields.length > 5) Features.toStrand(fields(5)).foreach(f.setStrand(_))

    val attributes = new ArrayBuffer[(String, String)]()
    if (hasColumn(6)) attributes += ("signalValue" -> fields(6))
    if (hasColumn(7)) attributes += ("pValue" -> fields(7))
    if (hasColumn(8)) attributes += ("qValue" -> fields(8))
    if (hasColumn(9)) attributes += ("peak" -> fields(9))

    val attrMap = attributes.toMap
    f.setAttributes(attrMap)

    Some(f.build())
  }
}
