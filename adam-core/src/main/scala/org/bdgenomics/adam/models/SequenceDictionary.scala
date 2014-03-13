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
import net.sf.samtools.{ SAMFileHeader, SAMFileReader, SAMSequenceRecord, SAMSequenceDictionary }
import org.apache.avro.specific.SpecificRecord
import org.broadinstitute.variant.vcf.{ VCFHeader, VCFContigHeaderLine }
import scala.collection._
import scala.math.Ordering.Implicits._

/**
 * SequenceDictionary contains the (bijective) map between Ints (the referenceId) and Strings (the referenceName)
 * from the header of a BAM file, or the combined result of multiple such SequenceDictionaries.
 */
class SequenceDictionary(val recordsIn: Array[SequenceRecord]) extends Serializable {

  // Intermediate value used to ensure that no referenceName is listed twice.
  private val sequenceNames = recordsIn.groupBy(_.name)

  // Check that, for every sequenceName -> SequenceRecord mapping, the
  // sequence records are all the same.
  assert(sequenceNames.map(k => k._2.toSet).filter(k => k.size > 1).size == 0)

  // Pre-compute the hashCode, based on a sorted version of the idNamePairs list.
  private val _hashCode: Int = sequenceNames.map(k => k._1).toSeq.sortWith(_.toString < _.toString).foldLeft(0) {
    (hash: Int, p: CharSequence) => 37 * hash + p.hashCode
  }

  // Maps referenceName -> SequenceRecord
  private lazy val recordNames: mutable.Map[CharSequence, SequenceRecord] =
    mutable.Map(recordsIn.map {
      // Call toString explicitly, since otherwise we were picking up an Avro-specific Utf8 value here,
      // which was making the containsRefName method below fail in a hard-to-understand way.
      rec => (rec.name.toString, rec)
    }.toSeq: _*)

  def assignments: Map[String, SequenceRecord] = recordNames.map {
    case (name: String, rec: SequenceRecord) =>
      (name, rec)
  }

  /**
   * Returns the sequence record associated with a specific contig name.
   *
   * @throws AssertionError Throws assertion error if sequence corresponding to contig name
   * is not found.
   *
   * @param name Name to search for.
   * @return SequenceRecord associated with this record.
   */
  def apply(name: CharSequence): SequenceRecord = {
    // must explicitly call toString - see note at recordNames creation RE: Avro & Utf8
    val rec = recordsIn.find(kv => kv.name.toString == name.toString)

    assert(rec.isDefined, "Could not find key " + name + " in dictionary.")
    rec.get
  }

  /**
   * Returns true if this sequence dictionary contains a reference with a specific name.
   *
   * @param name Reference name to look for.
   * @return True if reference is in this dictionary.
   */
  def containsRefName(name: CharSequence): Boolean = {
    // must explicitly call toString - see note at recordNames creation RE: Avro & Utf8
    !recordsIn.forall(kv => kv.name.toString != name.toString)
  }

  def records: Set[SequenceRecord] = recordNames.values.toSet

  private[models] def cleanAndMerge(a1: Array[SequenceRecord],
                                    a2: Array[SequenceRecord]): Array[SequenceRecord] = {
    val a2filt = a2.filter(k => !a1.contains(k))

    a1 ++ a2filt
  }

  def +(record: SequenceRecord): SequenceDictionary = {
    if (recordsIn.contains(record)) {
      new SequenceDictionary(recordsIn)
    } else {
      new SequenceDictionary(recordsIn :+ record)
    }
  }

  def ++(dict: SequenceDictionary): SequenceDictionary =
    new SequenceDictionary(cleanAndMerge(recordsIn, dict.recordsIn))

  def ++(recs: Array[SequenceRecord]): SequenceDictionary =
    new SequenceDictionary(cleanAndMerge(recordsIn, recs))

  /**
   * Tests whether two dictionaries are compatible, where "compatible" means that
   * shared referenceName values are associated with the same SequenceRecord.
   *
   * Roughly, two dictionaries are compatible if the ++ operator will succeed when
   * called on them together.
   *
   * @param dict The other dictionary with which to test compatibility
   * @return true if the dictionaries are compatible, false otherwise.
   */
  def isCompatibleWith(dict: SequenceDictionary): Boolean =
    recordNames.keys.filter(dict.recordNames.contains).filter(
      name => recordNames(name) != dict.recordNames(name)).isEmpty

  override def equals(x: Any): Boolean = {
    x match {
      case d: SequenceDictionary =>
        recordNames == d.recordNames
      case _ => false
    }
  }

  override def hashCode(): Int = _hashCode

  /**
   * Converts this ADAM style sequence dictionary into a SAM style
   * sequence dictionary.
   *
   * @return Returns a SAM formatted sequence dictionary.
   */
  def toSAMSequenceDictionary(): SAMSequenceDictionary = {
    new SAMSequenceDictionary(recordsIn.map(_.toSAMSequenceRecord).toList)
  }

  /**
   * Returns the reference names stored in this dictionary.
   *
   * @return Returns the reference names in this dictionary.
   */
  def getReferenceNames(): Iterable[String] = {
    recordsIn.map(_.name.toString)
  }
}

object SequenceDictionary {

  def apply(recordsIn: SequenceRecord*) = new SequenceDictionary(recordsIn.toArray)

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
   * @see fromSAMSequenceDictionary
   *
   * @param header SAM file header.
   * @return Returns an ADAM style sequence dictionary.
   */
  def fromVCFHeader(header: VCFHeader): SequenceDictionary = {
    val contigLines: List[VCFContigHeaderLine] = header.getContigLines()

    // map over contig lines,
    apply(contigLines.map(l => {
      val name = l.getID()
      val index = l.getContigIndex()

      // TODO: this is clearly not correct. however, the picard version we are currently using does _not_ have a way
      // to report contig length from a vcf. we can't fix this without an update being made to hadoop-bam first, so
      // i've flagged the hadoop-bam team to let them know -- FAN, 2/5/2014
      val length = 1

      SequenceRecord(name, length.toLong, null)
    }): _*)
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
    val seqDict: SequenceDictionary =
      SequenceDictionary(samDictRecords.map {
        seqRecord: SAMSequenceRecord => SequenceRecord.fromSamSequenceRecord(seqRecord)
      }: _*)

    seqDict
  }

  def fromSAMReader(samReader: SAMFileReader): SequenceDictionary =
    fromSAMHeader(samReader.getFileHeader)

  def nonoverlappingHash(x: CharSequence, conflicts: Int => Boolean): Int = {
    var hash = x.hashCode
    while (conflicts(hash)) {
      hash += 1
    }
    hash
  }
}

/**
 * Utility class within the SequenceDictionary; represents unique reference name-to-id correspondence
 *
 * @param id
 * @param name
 * @param length
 * @param url
 * @param md5
 */
class SequenceRecord(val name: CharSequence, val length: Long, val url: CharSequence, val md5: CharSequence) extends Serializable {

  assert(name != null, "SequenceRecord.name is null")
  assert(name.length > 0, "SequenceRecord.name has length 0")
  assert(length > 0, "SequenceRecord.length <= 0")

  override def equals(x: Any): Boolean = {
    x match {
      case y: SequenceRecord =>
        name == y.name && length == y.length && (
          md5 == null || y.md5 == null || md5 == y.md5)
      case _ => false
    }
  }

  override def hashCode: Int = (name.hashCode * 37 + length.hashCode) * 37

  override def toString: String = "%s->%s".format(name, length)

  /**
   * Converts this sequence record into a SAM sequence record.
   *
   * @return A SAM formatted sequence record.
   */
  def toSAMSequenceRecord(): SAMSequenceRecord = {
    val rec = new SAMSequenceRecord(name.toString, length.toInt)

    // NOTE: we should set the sam sequence record's id here, but, that is private inside of samtools - FAN, 2/5/2014

    // if url is defined, set it
    if (url != null) {
      rec.setAssembly(url)
    }

    rec
  }
}

object SequenceRecord {

  def apply(name: CharSequence, length: Long, url: CharSequence = null, md5: CharSequence = null): SequenceRecord =
    new SequenceRecord(name, length, url, md5)

  /**
   * Converts an ADAM contig into a sequence record.
   *
   * @param ctg Contig to convert.
   * @return Contig expressed as a sequence record.
   */
  def fromADAMContigFragment(fragment: ADAMNucleotideContigFragment): SequenceRecord = {
    val contig = fragment.getContig
    apply(contig.getContigName, contig.getContigLength, contig.getReferenceURL)
  }

  /*
   * Generates a sequence record from a SAMSequence record.
   *
   * @param seqRecord SAM Sequence record input.
   * @return A new ADAM sequence record.
   */
  def fromSamSequenceRecord(seqRecord: SAMSequenceRecord): SequenceRecord = {
    apply(seqRecord.getSequenceName, seqRecord.getSequenceLength, seqRecord.getAssembly)
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

    if (rec.getReadPaired) {

      // only process a read pair, if we're looking at the first element of the pair.
      if (rec.getFirstOfPair) {

        val left =
          if (rec.getReadMapped) {
            val contig = rec.getContig
            Set(SequenceRecord(contig.getContigName, contig.getContigLength, contig.getReferenceURL))
          } else
            Set()

        val right =
          if (rec.getMateMapped) {
            val contig = rec.getMateContig
            Set(SequenceRecord(contig.getContigName, contig.getContigLength, contig.getReferenceURL))
          } else
            Set()
        left ++ right
      } else {
        Set()
      }

    } else {
      if (rec.getReadMapped) {
        val contig = rec.getContig
        Set(SequenceRecord(contig.getContigName, contig.getContigLength, contig.getReferenceURL))
      } else {
        // If the read isn't mapped, then ignore the fields altogether.
        Set()
      }
    }
  }

  def fromSpecificRecord(rec: SpecificRecord): SequenceRecord = {

    val schema = rec.getSchema
    if (schema.getField("referenceId") != null) {
      // This should be able to be removed now that we've consolidated everything into
      // contained fields of type ADAMContig.
      new SequenceRecord(
        rec.get(schema.getField("referenceName").pos()).asInstanceOf[CharSequence],
        rec.get(schema.getField("referenceLength").pos()).asInstanceOf[Long],
        rec.get(schema.getField("referenceUrl").pos()).asInstanceOf[CharSequence],
        null)
    } else if (schema.getField("contig") != null) {
      val pos = schema.getField("contig").pos()
      val contig = rec.get(pos).asInstanceOf[ADAMContig]
      val contigSchema = contig.getSchema
      new SequenceRecord(
        contig.get(contigSchema.getField("contigName").pos()).asInstanceOf[CharSequence],
        contig.get(contigSchema.getField("contigLength").pos()).asInstanceOf[Long],
        contig.get(contigSchema.getField("referenceURL").pos()).asInstanceOf[CharSequence],
        contig.get(contigSchema.getField("contigMD5").pos()).asInstanceOf[CharSequence])
    } else {
      null
    }
  }
}
