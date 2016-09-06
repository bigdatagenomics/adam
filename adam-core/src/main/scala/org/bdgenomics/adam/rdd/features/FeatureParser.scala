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
package org.bdgenomics.adam.rdd.features

import htsjdk.samtools.ValidationStringency
import java.io.File
import org.bdgenomics.adam.models.SequenceRecord
import org.bdgenomics.formats.avro.{ Dbxref, Feature, Strand }
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

private[rdd] sealed trait FeatureParser extends Serializable with Logging {
  def parse(line: String): Seq[Feature]
}

private[rdd] class FeatureFile(parser: FeatureParser) extends Serializable {
  def parse(file: File): Iterator[Feature] = {
    val src = Source.fromFile(file)
    try {
      src.getLines().flatMap { line =>
        parser.parse(line)
      }
    } finally {
      src.close()
    }
  }
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

  override def parse(line: String): Seq[Feature] = {
    // skip header and comment lines
    if (line.startsWith("#")) return Seq()

    val fields = line.split("\t")
    // skip empty or invalid lines
    if (fields.length < 8 || fields.length > 9) {
      log.warn("Empty or invalid GTF/GFF2 line: {}", line)
      return Seq()
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
      .setContigName(seqname)
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

    Seq(f.build())
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

  override def parse(line: String): Seq[Feature] = {
    // skip header and comment lines
    if (line.startsWith("#")) return Seq()

    val fields = line.split("\t")
    // skip empty or invalid lines
    if (fields.length < 8 || fields.length > 9) {
      log.warn("Empty or invalid GFF3 line: {}", line)
      return Seq()
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
      .setContigName(seqid)
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

    Seq(f.build())
  }
}

/**
 * Parser for IntervalList format.
 *
 * In lieu of a specification, see:
 * https://samtools.github.io/htsjdk/javadoc/htsjdk/htsjdk/samtools/util/IntervalList.html
 */
private[rdd] class IntervalListParser extends Serializable with Logging {
  def parse(line: String,
            stringency: ValidationStringency): (Option[SequenceRecord], Option[Feature]) = {
    val fields = line.split("[ \t]+")
    if (fields.length < 2) {
      (None, None)
    } else {
      if (fields(0).startsWith("@")) {
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

          (Some(SequenceRecord(name, length, md5, url)), None)
        } else {
          (None, None)
        }
      } else {
        if (fields.length < 4) {
          throw new Exception(s"Invalid line: $line")
        }

        val (dbxrfs, attrs: Map[String, String]) =
          (if (fields.length < 5 || fields(4) == "." || fields(4) == "-") {
            (Nil, Map())
          } else {
            val a = fields(4).split(Array(';', ',')).flatMap(field => field.split('|') match {
              case Array(key, value) =>
                key match {
                  case "gn" | "ens" | "vega" | "ccds" =>
                    Some((
                      Some(Dbxref.newBuilder().setDb(key).setAccession(value).build()),
                      None
                    ))
                  case _ => Some((None, Some(key -> value)))
                }
              case x => {
                if (stringency == ValidationStringency.STRICT) {
                  throw new Exception(s"Expected fields of the form 'key|value;' but got: $field. Line:\n$line")
                } else {
                  if (stringency == ValidationStringency.LENIENT) {
                    log.warn(s"Expected fields of the form 'key|value;' but got: $field. Line:\n$line")
                  }
                  None
                }
              }
            })

            (a.flatMap(_._1).toList, a.flatMap(_._2).toMap)
          })

        (
          None,
          Some(
            Feature.newBuilder()
              .setContigName(fields(0))
              .setStart(fields(1).toLong - 1) // IntervalList ranges are 1-based
              .setEnd(fields(2).toLong) // IntervalList ranges are closed
              .setStrand(fields(3) match {
                case "+" => Strand.FORWARD
                case "-" => Strand.REVERSE
                case _   => Strand.INDEPENDENT
              })
              .setAttributes(attrs)
              .setDbxrefs(dbxrfs)
              .build()
          )
        )
      }
    }
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
    line.startsWith("#") || line.startsWith("browser") || line.startsWith("track")
  }

  override def parse(line: String): Seq[Feature] = {
    // skip header lines
    if (isHeader(line)) return Seq()

    val fields = line.split("\t")
    def hasColumn(n: Int): Boolean = {
      fields.length > n && fields(n) != "."
    }

    // skip empty or invalid lines
    if (fields.length < 3) {
      if (log.isDebugEnabled) log.debug("Empty or invalid BED line: {}", line)
      return Seq()
    }
    val fb = Feature.newBuilder()
    fb.setContigName(fields(0))

    // BED files are 0-based space-coordinates, so conversion to
    // our coordinate space should mean that the values are unchanged.
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)

    if (hasColumn(3)) fb.setName(fields(3))
    if (hasColumn(4)) fb.setScore(fields(4).toDouble)
    if (hasColumn(5)) Features.toStrand(fields(5)).foreach(fb.setStrand(_))

    val attributes = new ArrayBuffer[(String, String)]()
    if (hasColumn(6)) attributes += ("thickStart" -> fields(6))
    if (hasColumn(7)) attributes += ("thickEnd" -> fields(7))
    if (hasColumn(8)) attributes += ("itemRgb" -> fields(8))
    if (hasColumn(9)) attributes += ("blockCount" -> fields(9))
    if (hasColumn(10)) attributes += ("blockSizes" -> fields(10))
    if (hasColumn(11)) attributes += ("blockStarts" -> fields(11))

    val attrMap = attributes.toMap
    fb.setAttributes(attrMap)

    val feature: Feature = fb.build()
    Seq(feature)
  }
}

/**
 * Parser for NarrowPeak format.
 *
 * Specification:
 * http://www.genome.ucsc.edu/FAQ/FAQformat.html#format12
 */
private[rdd] class NarrowPeakParser extends Serializable with Logging {

  def parse(line: String,
            stringency: ValidationStringency): Seq[Feature] = {
    val fields = line.split("\t")
    def hasColumn(n: Int): Boolean = {
      fields.length > n && fields(n) != "."
    }

    // skip empty or invalid lines
    if (fields.length < 3) {
      if (stringency == ValidationStringency.STRICT) {
        throw new IllegalArgumentException("Empty or invalid NarrowPeak line: %s".format(line))
      } else if (stringency == ValidationStringency.LENIENT) {
        log.warn("Empty or invalid NarrowPeak line: {}", line)
      }
      return Seq()
    }
    val fb = Feature.newBuilder()
    fb.setContigName(fields(0))

    // Peak files are 0-based space-coordinates, so conversion to
    // our coordinate space should mean that the values are unchanged.
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)

    if (hasColumn(3)) fb.setName(fields(3))
    if (hasColumn(4)) fb.setScore(fields(4).toDouble)
    if (fields.length > 5) Features.toStrand(fields(5)).foreach(fb.setStrand(_))

    val attributes = new ArrayBuffer[(String, String)]()
    if (hasColumn(6)) attributes += ("signalValue" -> fields(6))
    if (hasColumn(7)) attributes += ("pValue" -> fields(7))
    if (hasColumn(8)) attributes += ("qValue" -> fields(8))
    if (hasColumn(9)) attributes += ("peak" -> fields(9))

    val attrMap = attributes.toMap
    fb.setAttributes(attrMap)
    Seq(fb.build())
  }
}
