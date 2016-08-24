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

import org.bdgenomics.formats.avro.{ Dbxref, Feature, OntologyTerm, Strand }
import scala.collection.JavaConversions._
import scala.collection.mutable.{ ArrayBuffer, HashMap, MutableList }

/**
 * Utility methods on features and related classes.
 */
private[features] object Features {

  /**
   * Parse a strand from the specified string.
   *
   * @param s string to parse
   * @return a strand parsed from the specified string, or None if the stand
   *   is unspecified or incorrectly specified
   */
  def toStrand(s: String): Option[Strand] = {
    s match {
      case "+" => Some(Strand.FORWARD)
      case "-" => Some(Strand.REVERSE)
      case "." => Some(Strand.INDEPENDENT)
      case "?" => Some(Strand.UNKNOWN)
      case _   => None
    }
  }

  /**
   * Convert the specified strand to its string value.
   *
   * @param strand strand to convert
   * @return the specified strand converted to its string value
   */
  def asString(strand: Strand): String = {
    strand match {
      case Strand.FORWARD     => "+"
      case Strand.REVERSE     => "-"
      case Strand.INDEPENDENT => "."
      case Strand.UNKNOWN     => "?"
      case _                  => ""
    }
  }

  /**
   * Parse a database cross reference from the specified string.
   *
   * @param s string to parse
   * @return a database cross reference parsed from the specified string, or None if
   *    the database cross reference is unspecified or incorrectly specified
   */
  def toDbxref(s: String): Option[Dbxref] = {
    val i = s.indexOf(':')
    if (i >= 0) {
      Some(new Dbxref(s.substring(0, i), s.substring(i)))
    } else {
      None
    }
  }

  /**
   * Convert the specified database cross reference to its string value.
   *
   * @param dbxref database cross reference to convert
   * @return the specified database cross reference converted to its string value
   */
  def asString(dbxref: Dbxref): String = {
    dbxref.getDb + ":" + dbxref.getAccession
  }

  /**
   * Parse an ontology term from the specified string.
   *
   * @param s string to parse
   * @return an ontology term parsed from the specified string, or None if
   *    the ontology term is unspecified or incorrectly specified
   */
  def toOntologyTerm(s: String): Option[OntologyTerm] = {
    val i = s.indexOf(':')
    if (i >= 0) {
      Some(new OntologyTerm(s.substring(0, i), s.substring(i)))
    } else {
      None
    }
  }

  /**
   * Convert the specified ontology term to its string value.
   *
   * @param ontologyTerm ontology term to convert
   * @return the specified ontology term converted to its string value
   */
  def asString(ontologyTerm: OntologyTerm): String = {
    ontologyTerm.getDb + ":" + ontologyTerm.getAccession
  }

  /**
   * Assign values for various feature fields from a sequence of attribute key value pairs.
   *
   * @param attributes sequence of attribute key value pairs
   * @param f feature builder
   * @return the specified feature builder
   */
  def assignAttributes(attributes: Seq[(String, String)], f: Feature.Builder): Feature.Builder = {
    // initialize empty builder list fields
    val aliases = new MutableList[String]()
    val notes = new MutableList[String]()
    val parentIds = new MutableList[String]()
    val dbxrefs = new MutableList[Dbxref]()
    val ontologyTerms = new MutableList[OntologyTerm]()

    // set id, name, target, gap, derivesFrom, and isCircular
    // and populate aliases, notes, parentIds, dbxrefs, and ontologyTerms
    // from attributes if specified
    val remaining = new HashMap[String, String]
    attributes.foreach(entry =>
      entry._1 match {
        // reserved keys in GFF3 specification
        case "ID"            => f.setFeatureId(entry._2)
        case "Name"          => f.setName(entry._2)
        case "Target"        => f.setTarget(entry._2)
        case "Gap"           => f.setGap(entry._2)
        case "Derives_from"  => f.setDerivesFrom(entry._2)
        case "Is_circular"   => f.setCircular(entry._2.toBoolean)
        case "Alias"         => aliases += entry._2
        case "Note"          => notes += entry._2
        case "Parent"        => parentIds += entry._2
        case "Dbxref"        => toDbxref(entry._2).foreach(dbxrefs += _)
        case "Ontology_term" => toOntologyTerm(entry._2).foreach(ontologyTerms += _)
        // commonly used keys in GTF/GFF2, e.g. via Ensembl
        case "gene_id"       => f.setGeneId(entry._2)
        case "transcript_id" => f.setTranscriptId(entry._2)
        case "exon_id"       => f.setExonId(entry._2)
        // unrecognized key, save to attributes
        case _               => remaining += entry
      }
    )

    // set list fields if non-empty
    if (!aliases.isEmpty) f.setAliases(aliases)
    if (!notes.isEmpty) f.setNotes(notes)
    if (!parentIds.isEmpty) f.setParentIds(parentIds)
    if (!dbxrefs.isEmpty) f.setDbxrefs(dbxrefs)
    if (!ontologyTerms.isEmpty) f.setOntologyTerms(ontologyTerms)

    // set remaining attributes if non-empty;
    // any duplicate keys are lost at this point, last one in wins
    if (!remaining.isEmpty) f.setAttributes(remaining)

    f
  }

  /**
   * Gather values from various feature fields into a sequence of attribute key value pairs.
   *
   * @param feature feature to gather values from
   * @return values from various feature fields gathered into a sequence of attribute key value pairs
   */
  def gatherAttributes(feature: Feature): Seq[(String, String)] = {
    val attrs = new ArrayBuffer[(String, String)]

    def addBooleanTuple(b: java.lang.Boolean) = {
      attrs += Tuple2("Is_circular", b.toString)
    }

    Option(feature.getFeatureId).foreach(attrs += Tuple2("ID", _))
    Option(feature.getName).foreach(attrs += Tuple2("Name", _))
    Option(feature.getTarget).foreach(attrs += Tuple2("Target", _))
    Option(feature.getGap).foreach(attrs += Tuple2("Gap", _))
    Option(feature.getDerivesFrom).foreach(attrs += Tuple2("Derives_from", _))
    Option(feature.getCircular).foreach(addBooleanTuple)
    Option(feature.getGeneId).foreach(attrs += Tuple2("gene_id", _))
    Option(feature.getTranscriptId).foreach(attrs += Tuple2("transcript_id", _))
    Option(feature.getExonId).foreach(attrs += Tuple2("exon_id", _))
    for (alias <- feature.getAliases) attrs += Tuple2("Alias", alias)
    for (note <- feature.getNotes) attrs += Tuple2("Note", note)
    for (parentId <- feature.getParentIds) attrs += Tuple2("Parent", parentId)
    for (dbxref <- feature.getDbxrefs) attrs += Tuple2("Dbxref", asString(dbxref))
    for (ontologyTerm <- feature.getOntologyTerms) attrs += Tuple2("Ontology_term", asString(ontologyTerm))
    attrs ++= feature.getAttributes.toSeq
  }

  /**
   * Build a name for the specified feature considering its attributes.  Used when
   * writing out to lossy feature formats such as BED, NarrowPeak, and IntervalList.
   *
   * @param feature feature
   * @return a name for the specified feature considering its attributes
   */
  def nameOf(feature: Feature): String = {
    Option(feature.getName).foreach(return _)
    Option(feature.getFeatureId).foreach(return _)

    feature.getFeatureType match {
      case "exon" | "SO:0000147"       => Option(feature.getExonId).foreach(return _)
      case "transcript" | "SO:0000673" => Option(feature.getTranscriptId).foreach(return _)
      case "gene" | "SO:0000704"       => Option(feature.getGeneId).foreach(return _)
      case s: String                   => Option(s).foreach(return _)
      case null                        =>
    }
    "sequence_feature"
  }
}
