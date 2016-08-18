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

import htsjdk.samtools.{ SamReader, SAMFileHeader, SAMSequenceRecord, SAMSequenceDictionary }
import htsjdk.variant.vcf.VCFHeader
import org.apache.avro.generic.IndexedRecord
import org.bdgenomics.formats.avro.{ AlignmentRecord, NucleotideContigFragment, Contig }
import scala.collection._
import scala.collection.JavaConversions._

/**
 * SequenceDictionary contains the (bijective) map between Ints (the referenceId) and Strings (the referenceName)
 * from the header of a BAM file, or the combined result of multiple such SequenceDictionaries.
 */

object SequenceDictionary {
  /**
   * @return Creates a new, empty SequenceDictionary.
   */
  def empty: SequenceDictionary = new SequenceDictionary()

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
   * Creates a sequence dictionary from a sequence of Avro Contigs.
   *
   * @param contigs Seq of Contig records.
   * @return Returns a sequence dictionary.
   */
  def fromAvro(contigs: Seq[Contig]): SequenceDictionary = {
    new SequenceDictionary(contigs.map(SequenceRecord.fromADAMContig).toVector)
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
   * Extracts a SAM sequence dictionary from a VCF header and returns an
   * ADAM sequence dictionary.
   *
   * @see fromSAMHeader
   *
   * @param header VCF file header.
   * @return Returns an ADAM style sequence dictionary.
   */
  def fromVCFHeader(header: VCFHeader): SequenceDictionary = {
    val samDict = header.getSequenceDictionary

    // vcf files can have null sequence dictionaries
    Option(samDict).fold(SequenceDictionary.empty)(ssd => fromSAMSequenceDictionary(ssd))
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
    val samDictRecords = samDict.getSequences
    new SequenceDictionary(samDictRecords.map(SequenceRecord.fromSAMSequenceRecord).toVector)
  }

  def fromSAMReader(samReader: SamReader): SequenceDictionary =
    fromSAMHeader(samReader.getFileHeader)
}

class SequenceDictionary(val records: Vector[SequenceRecord]) extends Serializable {
  def this() = this(Vector.empty[SequenceRecord])

  private val byName: Map[String, SequenceRecord] = records.view.map(r => r.name -> r).toMap
  assert(byName.size == records.length, "SequenceRecords with duplicate names aren't permitted")

  private val hasSequenceOrdering = records.forall(_.referenceIndex.isDefined)

  def isCompatibleWith(that: SequenceDictionary): Boolean = {
    for (record <- that.records) {
      val myRecord = byName.get(record.name)
      if (myRecord.exists(_ != record))
        return false
    }
    true
  }

  def apply(name: String): Option[SequenceRecord] = byName.get(name)
  def containsRefName(name: String): Boolean = byName.containsKey(name)

  def +(record: SequenceRecord): SequenceDictionary = this ++ SequenceDictionary(record)
  def ++(that: SequenceDictionary): SequenceDictionary = {
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

  /**
   * Strips indices from a Sequence Dictionary.
   *
   * @return This returns a new sequence dictionary devoid of indices. This is
   *   important for sorting: the default sort in ADAM is based on a lexical
   *   ordering, while the default sort in SAM is based on sequence indices. If
   *   the indices are not stripped before a file is saved back to SAM/BAM, the
   *   SAM/BAM header sequence ordering will not match the sort order of the
   *   records in the file.
   *
   * @see sorted
   */
  def stripIndices: SequenceDictionary = {
    new SequenceDictionary(records.map(_.stripIndex))
  }

  /**
   * Sort the records in a sequence dictionary.
   *
   * @return Returns a new sequence dictionary where the sequence records are
   *   sorted. If the sequence records have indices, the records will be sorted
   *   by their indices. If not, the sequence records will be sorted lexically
   *   by contig name.
   *
   * @see stripIndices
   */
  def sorted: SequenceDictionary = {
    implicit val ordering: Ordering[SequenceRecord] =
      if (hasSequenceOrdering)
        SequenceOrderingByRefIdx
      else
        SequenceOrderingByName
    new SequenceDictionary(records.sorted)
  }

  override def toString: String = {
    records.map(_.toString).fold("SequenceDictionary{")(_ + "\n" + _) + "}"
  }

  private[adam] def toAvro: Seq[Contig] = {
    records.map(SequenceRecord.toADAMContig)
      .toSeq
  }

  def isEmpty: Boolean = records.isEmpty
}

object SequenceOrderingByName extends Ordering[SequenceRecord] {
  def compare(
    a: SequenceRecord,
    b: SequenceRecord): Int = {
    a.name.compareTo(b.name)
  }
}

object SequenceOrderingByRefIdx extends Ordering[SequenceRecord] {
  def compare(
    a: SequenceRecord,
    b: SequenceRecord): Int = {
    (for {
      aRefIdx <- a.referenceIndex
      bRefIdx <- b.referenceIndex
    } yield {
      aRefIdx.compareTo(bRefIdx)
    }).getOrElse(
      throw new Exception(s"Missing reference index when comparing SequenceRecords: $a, $b")
    )
  }
}

/**
 * Utility class within the SequenceDictionary; represents unique reference name-to-id correspondence
 *
 */
case class SequenceRecord(
    name: String,
    length: Long,
    url: Option[String],
    md5: Option[String],
    refseq: Option[String],
    genbank: Option[String],
    assembly: Option[String],
    species: Option[String],
    referenceIndex: Option[Int]) extends Serializable {

  assert(name != null && !name.isEmpty, "SequenceRecord.name is null or empty")
  assert(length > 0, "SequenceRecord.length <= 0")

  /**
   * @return Returns a new sequence record with the index unset.
   */
  def stripIndex: SequenceRecord = {
    SequenceRecord(name,
      length,
      url,
      md5,
      refseq,
      genbank,
      assembly,
      species,
      None)
  }

  override def toString: String = "%s->%s%s".format(name,
    length,
    referenceIndex.fold("")(d => ", %d".format(d)))

  /**
   * Converts this sequence record into a SAM sequence record.
   *
   * @return A SAM formatted sequence record.
   */
  def toSAMSequenceRecord: SAMSequenceRecord = {
    val rec = new SAMSequenceRecord(name, length.toInt)

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

    referenceIndex.foreach(rec.setSequenceIndex)

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

  def apply(
    name: String,
    length: Long,
    md5: String = null,
    url: String = null,
    refseq: String = null,
    genbank: String = null,
    assembly: String = null,
    species: String = null,
    referenceIndex: Option[Int] = None): SequenceRecord = {
    new SequenceRecord(
      name,
      length,
      Option(url),
      Option(md5),
      Option(refseq),
      Option(genbank),
      Option(assembly),
      Option(species),
      referenceIndex
    )
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
      species = record.getAttribute(SAMSequenceRecord.SPECIES_TAG),
      referenceIndex = if (record.getSequenceIndex == -1) None else Some(record.getSequenceIndex)
    )

  }
  def toSAMSequenceRecord(record: SequenceRecord): SAMSequenceRecord = {
    val sam = new SAMSequenceRecord(record.name, record.length.toInt)
    record.md5.foreach(v => sam.setAttribute(SAMSequenceRecord.MD5_TAG, v))
    record.url.foreach(v => sam.setAttribute(SAMSequenceRecord.URI_TAG, v))
    sam
  }

  def fromADAMContig(contig: Contig): SequenceRecord = {
    SequenceRecord(
      contig.getContigName,
      Option(contig.getContigLength).map(l => l: Long).getOrElse(Long.MaxValue),
      md5 = contig.getContigMD5,
      url = contig.getReferenceURL,
      assembly = contig.getAssembly,
      species = contig.getSpecies,
      referenceIndex = Option(contig.getReferenceIndex).map(Integer2int)
    )
  }

  def toADAMContig(record: SequenceRecord): Contig = {
    val builder = Contig.newBuilder()
      .setContigName(record.name)
      .setContigLength(record.length)
    record.md5.foreach(builder.setContigMD5)
    record.url.foreach(builder.setReferenceURL)
    record.assembly.foreach(builder.setAssembly)
    record.species.foreach(builder.setSpecies)
    record.referenceIndex.foreach(builder.setReferenceIndex(_))
    builder.build
  }

  def fromADAMContigFragment(fragment: NucleotideContigFragment): SequenceRecord = {
    fromADAMContig(fragment.getContig)
  }

  def fromSpecificRecord(rec: IndexedRecord): SequenceRecord = {
    val schema = rec.getSchema
    if (schema.getField("referenceId") != null) {
      SequenceRecord(
        rec.get(schema.getField("referenceName").pos()).toString,
        rec.get(schema.getField("referenceLength").pos()).asInstanceOf[Long],
        url = rec.get(schema.getField("referenceUrl").pos()).toString
      )
    } else if (schema.getField("contig") != null) {
      val pos = schema.getField("contig").pos()
      fromADAMContig(rec.get(pos).asInstanceOf[Contig])
    } else {
      throw new AssertionError("Missing information to generate SequenceRecord")
    }
  }

}

