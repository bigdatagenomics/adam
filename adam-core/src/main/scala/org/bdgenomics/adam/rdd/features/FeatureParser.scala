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

import org.bdgenomics.formats.avro.{ Contig, Feature, Strand }
import org.bdgenomics.adam.models.{ GTFFeature, BaseFeature, NarrowPeakFeature, BEDFeature }
import scala.collection.JavaConversions._

/**
 * A FeatureParser is a parser which turns lines from a file into
 * values of some feature type.
 *
 * @tparam FT the feature type
 */
trait FeatureParser[FT <: BaseFeature] extends Serializable {

  /**
   * Parse a file line into a feature value.
   * @param line The text line from a file to parse
   * @return Some(feature), or None if the line shouldn't be parsed (e.g. comment lines)
   */
  def parse(line: String): Option[FT]
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
class GTFParser extends FeatureParser[GTFFeature] {

  override def parse(line: String): Option[GTFFeature] = {
    // Just skip the '#' prefixed lines, these are comments in the
    // GTF file format.
    if (line.startsWith("#")) {
      return None
    }

    val fields = line.split("\t")

    /*
    1. seqname - name of the chromosome or scaffold; chromosome names can be given with or without the 'chr' prefix. Important note: the seqname must be one used within Ensembl, i.e. a standard chromosome name or an Ensembl identifier such as a scaffold ID, without any additional content such as species or assembly. See the example GFF output below.
    2. source - name of the program that generated this feature, or the data source (database or project name)
    3. feature - feature type name, e.g. Gene, Variation, Similarity
    4. start - Start position of the feature, with sequence numbering starting at 1.
    5. end - End position of the feature, with sequence numbering starting at 1.
    6. score - A floating point value.
    7. strand - defined as + (forward) or - (reverse).
    8. frame - One of '0', '1' or '2'. '0' indicates that the first base of the feature is the first base of a codon, '1' that the second base is the first base of a codon, and so on..
    9. attribute - A semicolon-separated list of tag-value pairs, providing additional information about each feature.
     */

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
      .setValue(attribute) // TODO(tdanford) this is the wrong place to put 'attributes'

    val _strand = strand match {
      case "+" => Strand.Forward
      case "-" => Strand.Reverse
      case _   => Strand.Independent
    }
    f.setStrand(_strand)

    val (_id, _parentId) =
      feature match {
        case "gene"       => (Option(attrs("gene_id")), None)
        case "transcript" => (Option(attrs("transcript_id")), Option(attrs("gene_id")))
        case "exon"       => (Option(attrs("exon_id")), Option(attrs("transcript_id")))
        case _            => (attrs.get("id"), None)
      }
    _id.foreach(f.setFeatureId)
    _parentId.foreach(parentId => f.setParentIds(List(parentId)))

    Some(new GTFFeature(f.build()))
  }
}

class BEDParser extends FeatureParser[BEDFeature] {
  def parse(line: String): Option[BEDFeature] = {
    val fields = line.split("\t")
    assert(fields.length >= 3, "BED line had less than 3 fields")
    val fb = Feature.newBuilder()
    val cb = Contig.newBuilder()
    cb.setContigName(fields(0))
    fb.setContig(cb.build())
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)
    if (fields.length > 3) fb.setTrackName(fields(3))
    if (fields.length > 4) fb.setValue(fields(4) match {
      case "." => null
      case _   => fields(4).toDouble
    })
    if (fields.length > 5) fb.setStrand(fields(5) match {
      case "+" => Strand.Forward
      case "-" => Strand.Reverse
      case _   => Strand.Independent
    })
    //    if (fields.length > 6) fb.setThickStart(fields(6).toLong)
    //    if (fields.length > 7) fb.setThickEnd(fields(7).toLong)
    //    if (fields.length > 8) fb.setItemRgb(fields(8))
    //    if (fields.length > 9) fb.setBlockCount(fields(9).toLong)
    //    if (fields.length > 10) fb.setBlockSizes(fields(10).split(",").map(new java.lang.Long(_)).toList)
    //    if (fields.length > 11) fb.setBlockStarts(fields(11).split(",").map(new java.lang.Long(_)).toList)

    Some(new BEDFeature(fb.build()))
  }
}

// TODO(laserson): finish narrowPeak parser
class NarrowPeakParser extends FeatureParser[NarrowPeakFeature] {
  def parse(line: String): Option[NarrowPeakFeature] = {
    val fields = line.split("\t")
    assert(fields.length >= 3, "narrowPeak line had less than 3 fields")
    val fb = Feature.newBuilder()
    val cb = Contig.newBuilder()
    cb.setContigName(fields(0))
    fb.setContig(cb.build())
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)
    if (fields.length > 3) fb.setTrackName(fields(3))
    if (fields.length > 4) fb.setValue(fields(4) match {
      case "." => null
      case _   => fields(4).toDouble
    })
    if (fields.length > 5) fb.setStrand(fields(5) match {
      case "+" => Strand.Forward
      case "-" => Strand.Reverse
      case _   => Strand.Independent
    })
    if (fields.length > 6) fb.setSignalValue(fields(6).toDouble)
    if (fields.length > 7) fb.setPValue(fields(7).toDouble)
    if (fields.length > 8) fb.setQValue(fields(8).toDouble)
    if (fields.length > 9) fb.setPeak(fields(9).toLong)

    Some(new NarrowPeakFeature(fb.build()))
  }
}
