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

import htsjdk.samtools.{ SAMFileHeader, SAMSequenceDictionary, SAMSequenceRecord }
import htsjdk.variant.vcf.VCFHeader
import org.bdgenomics.formats.avro.{ NucleotideContigFragment, Reference }
import scala.collection.JavaConversions.{ asScalaIterator, seqAsJavaList }
import scala.collection.JavaConverters._
import scala.collection._

/**
 * Singleton object for creating SequenceDictionaries.
 */
object SequenceDictionary {

  /**
   * @return Creates a new, empty SequenceDictionary.
   */
  def empty: SequenceDictionary = new SequenceDictionary()

  /**
   * Builds a sequence dictionary for a variable length collection of records.
   *
   * @param records Records to include in the dictionary.
   * @return A sequence dictionary containing these records.
   */
  def apply(records: SequenceRecord*): SequenceDictionary = new SequenceDictionary(records.toVector)

  /**
   * Builds a sequence dictionary from an htsjdk SAMSequenceDictionary.
   *
   * @param dict Htsjdk sequence dictionary to build from.
   * @return A SequenceDictionary with populated sequence records.
   */
  def apply(dict: SAMSequenceDictionary): SequenceDictionary = {
    new SequenceDictionary(dict.getSequences.iterator().map(SequenceRecord.fromSAMSequenceRecord).toVector)
  }

  /**
   * Makes a SequenceDictionary from a SAMFileHeader.
   *
   * @param header htsjdk SAMFileHeader to extract sequences from.
   * @return A SequenceDictionary with populated sequence records.
   */
  def apply(header: SAMFileHeader): SequenceDictionary = {
    SequenceDictionary(header.getSequenceDictionary)
  }

  /**
   * Creates a sequence dictionary from a sequence of Avro References.
   *
   * @param contigs Seq of Reference records.
   * @return Returns a sequence dictionary.
   */
  def fromAvro(references: Seq[Reference]): SequenceDictionary = {
    new SequenceDictionary(references.map(SequenceRecord.fromADAMReference).toVector)
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
    new SequenceDictionary(samDictRecords.iterator().map(SequenceRecord.fromSAMSequenceRecord).toVector)
  }
}

/**
 * A SequenceDictionary contains metadata about the reference build genomic data
 * is aligned against.
 *
 * @see SequenceRecord
 *
 * @param records The individual reference sequences.
 */
class SequenceDictionary(val records: Vector[SequenceRecord]) extends Serializable {
  def this() = this(Vector.empty[SequenceRecord])

  private val byName: Map[String, SequenceRecord] = records.view.map(r => r.name -> r).toMap
  assert(byName.size == records.length, "SequenceRecords with duplicate names aren't permitted")

  val hasSequenceOrdering = records.forall(_.index.isDefined)

  /**
   * The number of sequences in the dictionary.
   */
  def size: Int = records.size

  /**
   * @param that Sequence dictionary to compare against.
   * @return True if each record in this dictionary exists in the other dictionary.
   */
  def isCompatibleWith(that: SequenceDictionary): Boolean = {
    for (record <- that.records) {
      val myRecord = byName.get(record.name)
      if (myRecord.exists(_ != record))
        return false
    }
    true
  }

  /**
   * @param name The name of the reference to extract.
   * @return If available, the sequence record for this reference.
   */
  def apply(name: String): Option[SequenceRecord] = byName.get(name)

  /**
   * Checks to see if we have a reference with a given name.
   *
   * @param name The name of the reference to extract.
   * @return True if we have a sequence record for this reference.
   */
  def containsReferenceName(name: String): Boolean = byName.contains(name)

  /**
   * Adds a sequence record to this dictionary.
   *
   * @param record The sequence record to add.
   * @return A new sequence dictionary with the new record added.
   */
  def +(record: SequenceRecord): SequenceDictionary = this ++ SequenceDictionary(record)

  /**
   * Merges two sequence dictionaries.
   *
   * Filters any sequence records that exist in both dictionaries.
   *
   * @param that The sequence dictionary to add.
   * @return A new sequence dictionary that contains a record per reference in each
   *   input dictionary.
   */
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
    new SAMSequenceDictionary(records.iterator.map(_.toSAMSequenceRecord).toList)
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
   * Filter this sequence dictionary to include only those sequence records
   * for the specified reference name.
   *
   * @param referenceName Reference name to filter by.
   * @return SequenceDictionary filtered to include only those sequence records
   *    for the specified reference name
   */
  def filterToReferenceName(referenceName: String): SequenceDictionary = {
    byName.get(referenceName).fold(SequenceDictionary.empty)(sr => new SequenceDictionary(Vector(sr)))
  }

  /**
   * Filter this sequence dictionary to include only those sequence records
   * for the specified reference names.
   *
   * @param referenceNames Reference names to filter by.
   * @return SequenceDictionary filtered to include only those sequence records
   *    for the specified reference names
   */
  def filterToReferenceNames(referenceNames: Iterable[String]): SequenceDictionary = {
    new SequenceDictionary(referenceNames.map(byName.get).flatten.toVector)
  }

  /**
   * Filter this sequence dictionary to include only those sequence records
   * with reference names that pass the specified filter function.
   *
   * @param fn Reference name filter function to filter by.
   * @return SequenceDictionary filtered to include only those sequence records
   *    with reference names that pass the specified filter function
   */
  def filterToReferenceNames(fn: (String) => Boolean): SequenceDictionary = {
    new SequenceDictionary(records.filter(sr => fn.apply(sr.name)))
  }

  /**
   * Sort the records in a sequence dictionary.
   *
   * @return Returns a new sequence dictionary where the sequence records are
   *   sorted. If the sequence records have indices, the records will be sorted
   *   by their indices. If not, the sequence records will be sorted lexically
   *   by reference name.
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

  private[adam] def toAvro: Seq[Reference] = {
    records.map(_.toADAMReference)
  }

  /**
   * @return True if this dictionary contains no sequence records.
   */
  def isEmpty: Boolean = records.isEmpty
}

private object SequenceOrderingByName extends Ordering[SequenceRecord] {
  def compare(
    a: SequenceRecord,
    b: SequenceRecord): Int = {
    a.name.compareTo(b.name)
  }
}

private object SequenceOrderingByRefIdx extends Ordering[SequenceRecord] {
  def compare(
    a: SequenceRecord,
    b: SequenceRecord): Int = {
    (for {
      aRefIdx <- a.index
      bRefIdx <- b.index
    } yield {
      aRefIdx.compareTo(bRefIdx)
    }).getOrElse(
      throw new Exception(s"Missing reference index when comparing SequenceRecords: $a, $b")
    )
  }
}

/**
 * Metadata about a single reference sequence.
 *
 * @param name The name of this reference in the assembly.
 * @param length The length of the sequence for this reference.
 * @param url If available, the URI from which the reference sequence was obtained.
 * @param md5 If available, the MD5 checksum uniquely representing this
 *    reference as a lower-case hexadecimal string, calculated as the MD5
 *    of the upper-case sequence excluding all whitespace characters (equivalent
 *    to SQ:M5 in SAM).
 * @param refseq If available, the REFSEQ ID for the reference.
 * @param genbank If available, the Genbank ID for the reference.
 * @param assembly If available, the name of the assembly for this reference.
 * @param species If available, the species that this reference is for.
 * @param index If available, the number of this reference in a set of
 *   references.
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
    index: Option[Int]) extends Serializable {

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
    index.fold("")(d => ", %d".format(d)))

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

    index.foreach(rec.setSequenceIndex)

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

  /**
   * @return Builds an Avro Reference representation from this record.
   */
  def toADAMReference: Reference = {
    val builder = Reference.newBuilder()
      .setName(name)
      .setLength(length)
    md5.foreach(builder.setMd5)
    url.foreach(builder.setSourceUri)
    val sourceAccessions = refseq ++ genbank
    if (!sourceAccessions.isEmpty) {
      builder.setSourceAccessions(sourceAccessions.toList.asJava)
    }
    species.foreach(builder.setSpecies)
    index.foreach(builder.setIndex(_))
    builder.build
  }
}

/**
 * Companion object for creating Sequence Records.
 */
object SequenceRecord {
  private val REFSEQ_TAG = "REFSEQ"
  private val GENBANK_TAG = "GENBANK"

  /**
   * Java friendly apply method that wraps null strings.
   *
   * @param name The name of this reference in the assembly.
   * @param length The length of the sequence for this reference.
   * @param url If available, the URI from which the reference sequence was obtained.
   * @param md5 If available, the MD5 checksum uniquely representing this
   *    reference as a lower-case hexadecimal string, calculated as the MD5
   *    of the upper-case sequence excluding all whitespace characters (equivalent
   *    to SQ:M5 in SAM).
   * @param refseq If available, the REFSEQ ID for the reference.
   * @param genbank If available, the Genbank ID for the reference.
   * @param assembly If available, the name of the assembly for this reference.
   * @param species If available, the species that this reference is for.
   * @param index If available, the number of this reference in a set of
   *   references.
   * @return Returns a new SequenceRecord where all strings except for name are
   *   wrapped in Options to check for null values.
   */
  def apply(
    name: String,
    length: Long,
    md5: String = null,
    url: String = null,
    refseq: String = null,
    genbank: String = null,
    assembly: String = null,
    species: String = null,
    index: Option[Int] = None): SequenceRecord = {
    new SequenceRecord(
      name,
      length,
      Option(url),
      Option(md5),
      Option(refseq),
      Option(genbank),
      Option(assembly),
      Option(species),
      index
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
      index = if (record.getSequenceIndex == -1) None else Some(record.getSequenceIndex)
    )
  }

  /**
   * Builds a sequence record from an Avro Contig.
   *
   * @param contig Contig record to build from.
   * @return This Contig record as a SequenceRecord.
   */
  def fromADAMReference(reference: Reference): SequenceRecord = {
    SequenceRecord(
      reference.getName,
      Option(reference.getLength).map(l => l: Long).getOrElse(Long.MaxValue),
      md5 = reference.getMd5,
      url = reference.getSourceUri,
      assembly = reference.getAssembly,
      species = reference.getSpecies,
      index = Option(reference.getIndex).map(Integer2int)
    )
  }

  /**
   * Extracts the contig metadata from a nucleotide fragment.
   *
   * @param fragment The assembly fragment to extract a SequenceRecord from.
   * @return The sequence record metadata from a single assembly fragment.
   */
  def fromADAMContigFragment(fragment: NucleotideContigFragment): SequenceRecord = {
    SequenceRecord(fragment.getContigName,
      fragment.getContigLength)
  }
}

