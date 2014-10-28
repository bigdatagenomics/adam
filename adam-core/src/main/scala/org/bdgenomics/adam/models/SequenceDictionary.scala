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
package org.bdgenomics.adam.models

import org.bdgenomics.formats.avro.{ AlignmentRecord, NucleotideContigFragment, Contig }
import org.bdgenomics.adam.rdd.ADAMContext._
import htsjdk.samtools.{ SamReader, SAMFileHeader, SAMSequenceRecord, SAMSequenceDictionary }
import org.apache.avro.specific.SpecificRecord
import scala.collection._

/**
 * SequenceDictionary contains the (bijective) map between Ints (the referenceId) and Strings (the referenceName)
 * from the header of a BAM file, or the combined result of multiple such SequenceDictionaries.
 */

object SequenceDictionary {
  def apply(): SequenceDictionary = new SequenceDictionary()
  def apply(records: SequenceRecord*): SequenceDictionary = new SequenceDictionary(records.toVector)
  def apply(dict: SAMSequenceDictionary): SequenceDictionary = {
    new SequenceDictionary(dict.getSequences.map(SequenceRecord.fromSAMSequenceRecord).toVector)
  }
  def apply(header: SAMFileHeader): SequenceDictionary = SequenceDictionary(header.getSequenceDictionary)
  def apply(reader: SamReader): SequenceDictionary = SequenceDictionary(reader.getFileHeader)

  def toSAMSequenceDictionary(dictionary: SequenceDictionary): SAMSequenceDictionary = {
    new SAMSequenceDictionary(dictionary.records.map(SequenceRecord.toSAMSequenceRecord).toList)
  }

  /**
   * Extracts a SAM sequence dictionary from a SAM file header and returns an
   * ADAM sequence dictionary.
   *
   * @see fromSAMSequenceDictionary
   *
   * @param header SAM file header.
   * @return Returns an ADAM style sequence dictionary.
   */
  def fromSAMHeader(header: SAMFileHeader): SequenceDictionary = {
    val samDict = header.getSequenceDictionary

    fromSAMSequenceDictionary(samDict)
  }

  /**
   * Converts a picard/samtools SAMSequenceDictionary into an ADAM sequence dictionary.
   *
   * @see fromSAMHeader
   * @see fromVCFHeader
   *
   * @param samDict SAM style sequence dictionary.
   * @return Returns an ADAM style sequence dictionary.
   */
  def fromSAMSequenceDictionary(samDict: SAMSequenceDictionary): SequenceDictionary = {
    val samDictRecords: List[SAMSequenceRecord] = samDict.getSequences
    new SequenceDictionary(samDictRecords.map(SequenceRecord.fromSAMSequenceRecord).toVector)
  }

  def fromSAMReader(samReader: SamReader): SequenceDictionary =
    fromSAMHeader(samReader.getFileHeader)
}

class SequenceDictionary(val records: Vector[SequenceRecord]) extends Serializable {
  def this() = this(Vector.empty[SequenceRecord])

  private val byName: Map[String, SequenceRecord] = records.view.map(r => r.name -> r).toMap
  assert(byName.size == records.length, "SequenceRecords with duplicate names aren't permitted")

  def isCompatibleWith(that: SequenceDictionary): Boolean = {
    for (record <- that.records) {
      val myRecord = byName.get(record.name)
      if (myRecord.isDefined && myRecord.get != record)
        return false
    }
    true
  }

  def apply(name: String): Option[SequenceRecord] = byName.get(name)
  def containsRefName(name: String): Boolean = byName.containsKey(name)

  def +(record: SequenceRecord): SequenceDictionary = this ++ SequenceDictionary(record)
  def ++(that: SequenceDictionary): SequenceDictionary = {
    assert(this.isCompatibleWith(that))
    new SequenceDictionary(records ++ that.records.filter(r => !byName.contains(r.name)))
  }

  override def hashCode = records.hashCode()
  override def equals(o: Any) = o match {
    case that: SequenceDictionary => records.equals(that.records)
    case _                        => false
  }

  /**
   * Converts this ADAM style sequence dictionary into a SAM style sequence dictionary.
   *
   * @return Returns a SAM formatted sequence dictionary.
   */
  def toSAMSequenceDictionary: SAMSequenceDictionary = {
    new SAMSequenceDictionary(records.map(_ toSAMSequenceRecord).toList)
  }

  override def toString: String = {
    records.map(_.toString).fold("SequenceDictionary{")(_ + "\n" + _) + "}"
  }
}

/**
 * Utility class within the SequenceDictionary; represents unique reference name-to-id correspondence
 *
 */
class SequenceRecord(
    val name: String,
    val length: Long,
    val url: Option[String] = None,
    val md5: Option[String] = None,
    val refseq: Option[String] = None,
    val genbank: Option[String] = None,
    val assembly: Option[String] = None,
    val species: Option[String] = None) extends Serializable {

  assert(name != null && !name.isEmpty, "SequenceRecord.name is null or empty")
  assert(length > 0, "SequenceRecord.length <= 0")

  override def toString: String = "%s->%s".format(name, length)

  /**
   * Converts this sequence record into a SAM sequence record.
   *
   * @return A SAM formatted sequence record.
   */
  def toSAMSequenceRecord: SAMSequenceRecord = {
    val rec = new SAMSequenceRecord(name.toString, length.toInt)

    // set md5 if available
    md5.foreach(s => rec.setAttribute(SAMSequenceRecord.MD5_TAG, s.toUpperCase))

    // set URL if available
    url.foreach(rec.setAttribute(SAMSequenceRecord.URI_TAG, _))

    // set species if available
    species.foreach(rec.setAttribute(SAMSequenceRecord.SPECIES_TAG, _))

    // set assembly if available
    assembly.foreach(rec.setAssembly)

    // set refseq accession number if available
    refseq.foreach(rec.setAttribute("REFSEQ", _))

    // set genbank accession number if available
    genbank.foreach(rec.setAttribute("GENBANK", _))

    // return record
    rec
  }

  override def equals(o: Any): Boolean = o match {
    case that: SequenceRecord =>
      name == that.name && length == that.length && optionEq(md5, that.md5) && optionEq(url, that.url)
    case _ => false
  }

  // No md5/url is "equal" to any md5/url in this setting
  private def optionEq(o1: Option[String], o2: Option[String]) = (o1, o2) match {
    case (Some(c1), Some(c2)) => c1 == c2
    case _                    => true
  }
}

object SequenceRecord {
  val REFSEQ_TAG = "REFSEQ"
  val GENBANK_TAG = "GENBANK"

  def apply(name: String,
            length: Long,
            md5: String = null,
            url: String = null,
            refseq: String = null,
            genbank: String = null,
            assembly: String = null,
            species: String = null): SequenceRecord = {
    new SequenceRecord(
      name,
      length,
      Option(url).map(_.toString),
      Option(md5).map(_.toString),
      Option(refseq).map(_.toString),
      Option(genbank).map(_.toString),
      Option(assembly).map(_.toString),
      Option(species).map(_.toString))
  }

  /*
   * Generates a sequence record from a SAMSequence record.
   *
   * @param seqRecord SAM Sequence record input.
   * @return A new ADAM sequence record.
   */
  def fromSAMSequenceRecord(record: SAMSequenceRecord): SequenceRecord = {
    SequenceRecord(
      record.getSequenceName,
      record.getSequenceLength,
      md5 = record.getAttribute(SAMSequenceRecord.MD5_TAG),
      url = record.getAttribute(SAMSequenceRecord.URI_TAG),
      refseq = record.getAttribute(REFSEQ_TAG),
      genbank = record.getAttribute(GENBANK_TAG),
      assembly = record.getAssembly,
      species = record.getAttribute(SAMSequenceRecord.SPECIES_TAG))

  }
  def toSAMSequenceRecord(record: SequenceRecord): SAMSequenceRecord = {
    val sam = new SAMSequenceRecord(record.name, record.length.toInt)
    record.md5.foreach(v => sam.setAttribute(SAMSequenceRecord.MD5_TAG, v.toString))
    record.url.foreach(v => sam.setAttribute(SAMSequenceRecord.URI_TAG, v.toString))
    sam
  }

  def fromADAMContig(contig: Contig): SequenceRecord = {
    SequenceRecord(
      contig.getContigName.toString,
      contig.getContigLength,
      md5 = contig.getContigName,
      url = contig.getReferenceURL,
      assembly = contig.getAssembly,
      species = contig.getSpecies)
  }

  def toADAMContig(record: SequenceRecord): Contig = {
    val builder = Contig.newBuilder()
      .setContigName(record.name)
      .setContigLength(record.length)
    record.md5.foreach(builder.setContigMD5)
    record.url.foreach(builder.setReferenceURL)
    record.assembly.foreach(builder.setAssembly)
    record.species.foreach(builder.setSpecies)
    builder.build
  }

  def fromADAMContigFragment(fragment: NucleotideContigFragment): SequenceRecord = {
    fromADAMContig(fragment.getContig)
  }

  /**
   * Convert an Read into one or more SequenceRecords.
   * The reason that we can't simply use the "fromSpecificRecord" method, below, is that each Read
   * can (through the fact that it could be a pair of reads) contain 1 or 2 possible SequenceRecord entries
   * for the SequenceDictionary itself.  Both have to be extracted, separately.
   *
   * @param rec The Read from which to extract the SequenceRecord entries
   * @return a list of all SequenceRecord entries derivable from this record.
   */
  def fromADAMRecord(rec: AlignmentRecord): Set[SequenceRecord] = {
    assert(rec != null, "Read was null")
    if ((!rec.getReadPaired || rec.getFirstOfPair) && (rec.getContig != null || rec.getMateContig != null)) {
      // The contig should be null for unmapped read
      List(Option(rec.getContig), Option(rec.getMateContig))
        .flatten
        .map(fromADAMContig)
        .toSet
    } else
      Set()
  }

  def fromSpecificRecord(rec: SpecificRecord): SequenceRecord = {
    val schema = rec.getSchema
    if (schema.getField("referenceId") != null) {
      SequenceRecord(
        rec.get(schema.getField("referenceName").pos()).toString,
        rec.get(schema.getField("referenceLength").pos()).asInstanceOf[Long],
        url = rec.get(schema.getField("referenceUrl").pos()).toString)
    } else if (schema.getField("contig") != null) {
      val pos = schema.getField("contig").pos()
      fromADAMContig(rec.get(pos).asInstanceOf[Contig])
    } else {
      throw new AssertionError("Missing information to generate SequenceRecord")
    }
  }
}
