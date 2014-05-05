/**
 * Copyright 2013-2014. Genome Bridge LLC.
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
package org.bdgenomics.adam.models

import org.bdgenomics.adam.avro.{ ADAMRecord, ADAMNucleotideContigFragment, ADAMContig }
import org.bdgenomics.adam.rdd.ADAMContext._
import net.sf.samtools.{ SAMFileReader, SAMFileHeader, SAMSequenceRecord, SAMSequenceDictionary }
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
    new SequenceDictionary(dict.getSequences.map(SequenceRecord.fromSAMSequenceRecord(_)).toVector)
  }
  def apply(header: SAMFileHeader): SequenceDictionary = SequenceDictionary(header.getSequenceDictionary)
  def apply(reader: SAMFileReader): SequenceDictionary = SequenceDictionary(reader.getFileHeader)

  def toSAMSequenceDictionary(dictionary: SequenceDictionary): SAMSequenceDictionary = {
    new SAMSequenceDictionary(dictionary.records.map(SequenceRecord.toSAMSequenceRecord(_)).toList)
  }

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

  // This getOrElse smells...
  def apply(name: String): SequenceRecord = byName.getOrElse(name, null)
  def containsRefName(name: String): Boolean = byName.containsKey(name)

  def +(record: SequenceRecord): SequenceDictionary = this ++ SequenceDictionary(record)
  def ++(that: SequenceDictionary): SequenceDictionary = {
    assert(this.isCompatibleWith(that))
    new SequenceDictionary(records ++ that.records.filter(r => !byName.contains(r.name)))
  }

  override def hashCode = records.hashCode
  override def equals(o: Any) = o match {
    case that: SequenceDictionary => records.equals(that.records)
    case _                        => false
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
    val genbank: Option[String] = None) extends Serializable {

  assert(name != null && !name.isEmpty, "SequenceRecord.name is null or empty")
  assert(length > 0, "SequenceRecord.length <= 0")

  override def toString: String = "%s->%s".format(name, length)

  override def equals(o: Any): Boolean = o match {
    case that: SequenceRecord => {
      name == that.name && length == that.length && optionEq(md5, that.md5) && optionEq(url, that.url)
    }
    case _ => false
  }

  private def optionEq(o1: Option[CharSequence], o2: Option[CharSequence]) = (o1, o2) match {
    case (Some(c1), Some(c2)) => c1 == c2
    case _                    => true
  }
}

object SequenceRecord {
  val REFSEQ_TAG = "REFSEQ"
  val GENBANK_TAG = "GENBANK"

  def apply(name: String, length: Long, md5: CharSequence = null, url: CharSequence = null, refseq: CharSequence = null, genbank: CharSequence = null): SequenceRecord = {
    new SequenceRecord(
      name,
      length,
      Option(url).map(_.toString),
      Option(md5).map(_.toString),
      Option(refseq).map(_.toString),
      Option(genbank).map(_.toString))
  }

  def fromSAMSequenceRecord(record: SAMSequenceRecord): SequenceRecord = {
    SequenceRecord(
      record.getSequenceName,
      record.getSequenceLength,
      md5 = record.getAttribute(SAMSequenceRecord.MD5_TAG),
      url = record.getAttribute(SAMSequenceRecord.URI_TAG),
      refseq = record.getAttribute(REFSEQ_TAG),
      genbank = record.getAttribute(GENBANK_TAG))

  }
  def toSAMSequenceRecord(record: SequenceRecord): SAMSequenceRecord = {
    val sam = new SAMSequenceRecord(record.name, record.length.toInt)
    record.md5.foreach(v => sam.setAttribute(SAMSequenceRecord.MD5_TAG, v.toString))
    record.url.foreach(v => sam.setAttribute(SAMSequenceRecord.URI_TAG, v.toString))
    sam
  }

  def fromADAMContig(contig: ADAMContig): SequenceRecord = {
    SequenceRecord(
      contig.getContigName.toString,
      contig.getContigLength,
      md5 = contig.getContigName,
      url = contig.getReferenceURL)
  }

  def toADAMContig(record: SequenceRecord): ADAMContig = {
    val builder = ADAMContig.newBuilder()
      .setContigName(record.name)
      .setContigLength(record.length)
    record.md5.foreach(builder.setContigMD5(_))
    record.url.foreach(builder.setReferenceURL(_))
    builder.build
  }

  def fromADAMContigFragment(fragment: ADAMNucleotideContigFragment): SequenceRecord = {
    fromADAMContig(fragment.getContig)
  }

  /**
   * Convert an ADAMRecord into one or more SequenceRecords.
   * The reason that we can't simply use the "fromSpecificRecord" method, below, is that each ADAMRecord
   * can (through the fact that it could be a pair of reads) contain 1 or 2 possible SequenceRecord entries
   * for the SequenceDictionary itself.  Both have to be extracted, separately.
   *
   * @param rec The ADAMRecord from which to extract the SequenceRecord entries
   * @return a list of all SequenceRecord entries derivable from this record.
   */
  def fromADAMRecord(rec: ADAMRecord): Set[SequenceRecord] = {
    assert(rec != null, "ADAMRecord was null")
    if ((!rec.getReadPaired || rec.getFirstOfPair) && (rec.getContig != null || rec.getMateContig != null)) {
      // The contig should be null for unmapped read
      List(Option(rec.getContig), Option(rec.getMateContig))
        .flatten
        .map(fromADAMContig(_))
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
      fromADAMContig(rec.get(pos).asInstanceOf[ADAMContig])
    } else {
      assert(false, "Missing information to generate SequenceRecord")
      null
    }
  }
}
