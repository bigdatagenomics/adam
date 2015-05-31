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

import java.io.File
import java.util.UUID
import org.bdgenomics.adam.models.SequenceRecord
import org.bdgenomics.formats.avro.{ Dbxref, Contig, Strand, Feature }
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import scala.io.Source

trait FeatureParser extends Serializable {
  def parse(line: String): Seq[Feature]
}

class FeatureFile(parser: FeatureParser) extends Serializable {
  def parse(file: File): Iterator[Feature] =
    Source.fromFile(file).getLines().flatMap { line =>
      parser.parse(line)
    }
}

object GTFParser {

  private val attr_regex = "\\s*([^\\s]+)\\s\"([^\"]+)\"".r

  /**
   * Parses a string of format
   *   token; token; token ...
   *
   * where each 'token' is of the form
   *   key "value"
   *
   * and turns it into a Map
   *
   * @param attributeField The original string of tokens
   * @return The Map of attributes
   */
  def parseAttrs(attributeField: String): Map[String, String] =
    attributeField.split(";").flatMap {
      case token: String =>
        attr_regex.findFirstMatchIn(token).map(m => (m.group(1), m.group(2)))
    }.toMap
}

/**
 * GTF is a line-based GFF variant.
 *
 * Details of the GTF/GFF format here:
 * http://www.ensembl.org/info/website/upload/gff.html
 */
class GTFParser extends FeatureParser {

  override def parse(line: String): Seq[Feature] = {
    // Just skip the '#' prefixed lines, these are comments in the
    // GTF file format.
    if (line.startsWith("#")) {
      return Seq()
    }

    val fields = line.split("\t")

    val (seqname, source, feature, start, end, score, strand, frame, attribute) =
      (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8))

    lazy val attrs = GTFParser.parseAttrs(attribute)

    val contig = Contig.newBuilder().setContigName(seqname).build()
    val f = Feature.newBuilder()
      .setContig(contig)
      .setStart(start.toLong - 1) // GTF/GFF ranges are 1-based
      .setEnd(end.toLong) // GTF/GFF ranges are closed
      .setFeatureType(feature)
      .setSource(source)

    val _strand = strand match {
      case "+" => Strand.Forward
      case "-" => Strand.Reverse
      case _   => Strand.Independent
    }
    f.setStrand(_strand)

    val exonId: Option[String] = attrs.get("exon_id").orElse {
      attrs.get("transcript_id").flatMap(t => attrs.get("exon_number").map(e => t + "_" + e))
    }

    val (_id, _parentId) =
      feature match {
        case "gene"       => (attrs.get("gene_id"), None)
        case "transcript" => (attrs.get("transcript_id"), attrs.get("gene_id"))
        case "exon"       => (exonId, attrs.get("transcript_id"))
        case "CDS"        => (attrs.get("id"), attrs.get("transcript_id"))
        case "UTR"        => (attrs.get("id"), attrs.get("transcript_id"))
        case _            => (attrs.get("id"), None)
      }
    _id.foreach(f.setFeatureId)
    _parentId.foreach(parentId => f.setParentIds(List[String](parentId)))

    f.setAttributes(attrs)

    Seq(f.build())
  }
}

class IntervalListParser extends Serializable {
  def parse(line: String): (Option[SequenceRecord], Option[Feature]) = {
    val fields = line.split("[ \t]+")
    if (fields.length < 2) {
      (None, None)
    } else {
      if (fields(0).startsWith("@")) {
        if (fields(0).startsWith("@SQ")) {
          val (name, length, url, md5) = {
            val attrs = fields.drop(1).map(field => field.split(":", 2) match {
              case Array(key, value) => key -> value
              case x                 => throw new Exception(s"Expected fields of the form 'key:value' in field $field but got: $x. Line:\n$line")
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
            val a = fields(4).split(Array(';', ',')).map(field => field.split('|') match {
              case Array(key, value) =>
                key match {
                  case "gn" | "ens" | "vega" | "ccds" => (Some(Dbxref.newBuilder().setDb(key).setAccession(value).build()), None)
                  case _                              => (None, Some(key -> value))
                }
              case x => throw new Exception(s"Expected fields of the form 'key|value;' but got: $field. Line:\n$line")
            })

            (a.flatMap(_._1).toList, a.flatMap(_._2).toMap)
          })

        (
          None,
          Some(
            Feature.newBuilder()
              .setContig(Contig.newBuilder().setContigName(fields(0)).build())
              .setStart(fields(1).toLong)
              .setEnd(fields(2).toLong)
              .setStrand(fields(3) match {
                case "+" => Strand.Forward
                case "-" => Strand.Reverse
                case _   => Strand.Independent
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

class BEDParser extends FeatureParser {

  override def parse(line: String): Seq[Feature] = {

    val fields = line.split("\t")
    if (fields.length < 3) {
      return Seq()
    }
    val fb = Feature.newBuilder()
    val cb = Contig.newBuilder()
    cb.setContigName(fields(0))
    fb.setContig(cb.build())
    fb.setFeatureId(UUID.randomUUID().toString)

    // BED files are 0-based space-coordinates, so conversion to
    // our coordinate space should mean that the values are unchanged.
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)

    if (fields.length > 3) {
      fb.setFeatureType(fields(3))
    }
    if (fields.length > 4) {
      fb.setValue(fields(4) match {
        case "." => null
        case _   => fields(4).toDouble
      })
    }
    if (fields.length > 5) {
      fb.setStrand(fields(5) match {
        case "+" => Strand.Forward
        case "-" => Strand.Reverse
        case _   => Strand.Independent
      })
    }
    val attributes = new ArrayBuffer[(String, String)]()
    if (fields.length > 6) {
      attributes += ("thickStart" -> fields(6))
    }
    if (fields.length > 7) {
      attributes += ("thickEnd" -> fields(7))
    }
    if (fields.length > 8) {
      attributes += ("itemRgb" -> fields(8))
    }
    if (fields.length > 9) {
      attributes += ("blockCount" -> fields(9))
    }
    if (fields.length > 10) {
      attributes += ("blockSizes" -> fields(10))
    }
    if (fields.length > 11) {
      attributes += ("blockStarts" -> fields(11))
    }
    val attrMap = attributes.toMap
    fb.setAttributes(attrMap)

    val feature: Feature = fb.build()
    Seq(feature)
  }
}

class NarrowPeakParser extends FeatureParser {

  override def parse(line: String): Seq[Feature] = {
    val fields = line.split("\t")
    if (fields.length < 3) {
      return Seq()
    }
    val fb = Feature.newBuilder()
    val cb = Contig.newBuilder()
    cb.setContigName(fields(0))
    fb.setContig(cb.build())
    fb.setFeatureId(UUID.randomUUID().toString)

    // Peak files are 0-based space-coordinates, so conversion to
    // our coordinate space should mean that the values are unchanged.
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)

    if (fields.length > 3) {
      fb.setFeatureType(fields(3))
    }
    if (fields.length > 4) {
      fb.setValue(fields(4) match {
        case "." => null
        case _   => fields(4).toDouble
      })
    }
    if (fields.length > 5) {
      fb.setStrand(fields(5) match {
        case "+" => Strand.Forward
        case "-" => Strand.Reverse
        case _   => Strand.Independent
      })
    }
    val attributes = new ArrayBuffer[(String, String)]()
    if (fields.length > 6) {
      attributes += ("signalValue" -> fields(6))
    }
    if (fields.length > 7) {
      attributes += ("pValue" -> fields(7))
    }
    if (fields.length > 8) {
      attributes += ("qValue" -> fields(8))
    }
    if (fields.length > 9) {
      attributes += ("peak" -> fields(9))
    }
    val attrMap = attributes.toMap
    fb.setAttributes(attrMap)
    Seq(fb.build())
  }
}

