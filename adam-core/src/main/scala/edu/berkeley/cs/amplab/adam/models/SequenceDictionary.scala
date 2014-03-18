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
package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.avro.{ADAMRecord, ADAMNucleotideContigFragment, ADAMContig}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import net.sf.samtools.{SAMFileHeader, SAMFileReader, SAMSequenceRecord, SAMSequenceDictionary}
import org.apache.avro.specific.SpecificRecord
import org.broadinstitute.variant.vcf.{VCFHeader, VCFContigHeaderLine}
import scala.collection._
import scala.math.Ordering.Implicits._

/**
 * SequenceDictionary contains the (bijective) map between Ints (the referenceId) and Strings (the referenceName)
 * from the header of a BAM file, or the combined result of multiple such SequenceDictionaries.
 */
class SequenceDictionary(val recordsIn: Array[SequenceRecord]) extends Serializable {

  // Intermediate value used to ensure that no referenceName or referenceId is listed twice with a different
  // referenceId or referenceName (respectively).  Notice the "toSet", which means it's okay to pass an Iterable
  // that lists the _same_ SequenceRecord twice.
  private val idNamePairs = recordsIn.map(rec => (rec.id, rec.name.toString)).toSet

  // check that no referenceId value is listed twice, to two different referenceNames
  assert(idNamePairs.groupBy(_._1).map(p => (p._1, p._2.size)).filter(p => p._2 > 1).isEmpty,
    "Duplicate ID in %s".format(idNamePairs))

  // check that no referenceName is listed twice, to two different referenceIds
  assert(idNamePairs.groupBy(_._2).map(p => (p._1, p._2.size)).filter(p => p._2 > 1).isEmpty,
    "Duplicate Name in %s".format(idNamePairs))

  // Pre-compute the hashCode, based on a sorted version of the idNamePairs list.
  private val _hashCode: Int = idNamePairs.toSeq.sortWith(_ < _).foldLeft(0) {
    (hash: Int, p: (Int, CharSequence)) => 37 * (hash + p._1) + p._2.hashCode
  }

  // Maps referenceId -> SequenceRecord
  private lazy val recordIndices: mutable.Map[Int, SequenceRecord] =
    mutable.Map(recordsIn.map {
      rec => (rec.id, rec)
    }.toSeq: _*)

  // Maps referenceName -> SequenceRecord
  private lazy val recordNames: mutable.Map[CharSequence, SequenceRecord] =
    mutable.Map(recordsIn.map {
      // Call toString explicitly, since otherwise we were picking up an Avro-specific Utf8 value here,
      // which was making the containsRefName method below fail in a hard-to-understand way.
      rec => (rec.name.toString, rec)
    }.toSeq: _*)

  def assignments: Map[Int, CharSequence] = recordIndices.map {
    case (id: Int, rec: SequenceRecord) =>
      (id, rec.name)
  }

  def apply(id: Int): SequenceRecord = recordIndices(id)

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
  def containsRefName(name : CharSequence) : Boolean = {
    // must explicitly call toString - see note at recordNames creation RE: Avro & Utf8
    !recordsIn.forall(kv => kv.name.toString != name.toString)
  }

  /**
   * Returns true if this sequence dictionary contains a reference with the specified (integer) ID.
   * @param id refId to look for
   * @return True if the refId is in this dictionary
   */
  def containsRefId(id : Int) : Boolean = {
    recordIndices.contains(id)
  }

  /**
   * Produces a Map of Int -> Int which maps the referenceIds from this SequenceDictionary
   * into referenceIds compatible with the argument SequenceDictionary ('dict').
   *
   * There are basically three cases that we have to handle here:
   * (1) ids for the same sequence name which are different between the dictionaries. These are
   * converted (from this.referenceId into dict.referenceId).
   * (2) ids which are in use (for different sequences) between the two dictionaries. In this case,
   * we mint a new identifier (using nonoverlappingHash) for the sequence in this dictionary
   * that won't conflict with any sequence in either dictionary.
   * (3) ids for sequences that aren't in the argument dict, and which don't conflict as in (2),
   * can be carried over as-is.
   *
   * (Note: if the source referenceId isn't in the Map produced by mapTo, we can assume that it
   * can be used without change in the new dictionary.  The method remap, below, actually implements
   * this identity.)
   *
   * The results of this mapTo should be useable by remap to produce a "compatible" dictionary,
   * i.e. for all d1 and d2,
   *
   * d1.remap(d1.mapTo(d2)).isCompatibleWith(d2)
   *
   * should be true.
   *
   * @param dict The target dictionary into whose referenceId space the ids of this dictionary should be mapped.
   * @return A Map whose values change the referenceIds in this dictionary; every referenceId in the source
   *         dictionary should be present in this Map
   */
  def mapTo(dict: SequenceDictionary): Map[Int, Int] = {

    /*
     * we start by assuming that all the sequences in the target dictionary will maintain their
     * existing identifiers -- mapTo won't imply any changes to the id/sequence correspondence in
     * the target dictionary.
     */
    val assign: mutable.Map[Int, CharSequence] = mutable.Map(dict.assignments.toSeq: _*)

    /*
     * Next, for every source sequence that is _not_ in the target dictionary, there are two cases:
     * 1. the source ID is not in use in the target -- in this case, just carry over the existing
     *    identifier into the assignment.
     * 2. the source ID _is_ already in use in the assignment -- in this case, we assign a new identifier
     *    for the source sequence, and store it in the assignment.
     */
    recordNames.keys.filter(!dict.recordNames.contains(_)).foreach {
      name =>
        val myIdx = recordNames(name).id
        if (assign.contains(myIdx)) {
          // using dict.nonoverlappingHash (rather than this.nonoverlappingHash) ensures
          // that the new identifier won't overlap with any other in the target dictionary
          // (and therefore, in the assignment map we're building, above).
          assign(dict.nonoverlappingHash(name)) = name
        } else {
          assign(myIdx) = name
        }
    }

    /*
     * At this point, 'assign' holds the desired id->sequence mapping of the "combined" target
     * and source dictionaries; to some extent, we've reverse-engineered the results of
     *
     *   this.remap(this.mapTo(dict)) ++ dict
     *
     * So now, we reverse the mapping (into sequence->id) and use it to convert source identifiers
     * into target identifiers.
     */
    val rassign: Map[CharSequence, Int] = Map(assign.toSeq.map(p => (p._2, p._1)): _*)
    val idxMap: Map[Int, Int] = Map(recordIndices.keys.map(idx => (idx, rassign(recordIndices(idx).name))).toSeq: _*)

    assert(idxMap.keys.filter(!recordIndices.contains(_)).isEmpty,
      "There we keys in the mapTo Map that weren't actually sequence indices")
    assert(recordIndices.keys.filter(!idxMap.contains(_)).isEmpty,
      "There were keys which weren't remapped by the mapTo idxMap")

    idxMap
  }

  /**
   * See the note to mapTo, above.
   * The results of this remap and mapTo should be to produce a "compatible" dictionary,
   * i.e. for all d1 and d2,
   *
   * d1.remap(d1.mapTo(d2)).isCompatibleWith(d2)
   *
   * should be true.
   *
   * @param idTransform The Map[Int,Int] to transform the identifiers of this dictionary; e.g. the output of
   *                    mapTo.
   * @return A new SequenceDictionary with just the referenceIds mapped through the given Map argument.
   */
 def remap(idTransform: Map[Int, Int]): SequenceDictionary = {
    def remapIndex(i: Int): Int =
      if (idTransform.contains(i)) idTransform(i) else i

    new SequenceDictionary(idNamePairs.map {
      case (id, name) =>
        recordIndices(id).withReferenceId(remapIndex(id))
    }.toArray)
  }

  def records: Set[SequenceRecord] = recordIndices.values.toSet

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
   * shared referenceName values are associated with the same referenceId, and
   * shared referenceId values are associated with the same referenceName.
   *
   * Roughly, two dictionaries are compatible if the ++ operator will succeed when
   * called on them together.
   *
   * @param dict The other dictionary with which to test compatibility
   * @return true if the dictionaries are compatible, false otherwise.
   */
  def isCompatibleWith(dict: SequenceDictionary): Boolean =
    recordIndices.keys.filter(dict.recordIndices.contains).filter(idx => recordIndices(idx) != dict.recordIndices(idx)).isEmpty &&
      recordNames.keys.filter(dict.recordNames.contains).filter(name => recordNames(name) != dict.recordNames(name)).isEmpty

  def nonoverlappingHash(x: CharSequence): Int =
    SequenceDictionary.nonoverlappingHash(x, idx => recordIndices.contains(idx))

  override def equals(x: Any): Boolean = {
    x match {
      case d: SequenceDictionary =>
        recordNames == d.recordNames && recordIndices == d.recordIndices
      case _ => false
    }
  }

  override def hashCode(): Int = _hashCode

  override def toString: String = idNamePairs.toString()

  /**
   * Converts this ADAM style sequence dictionary into a SAM style sequence dictionary.
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
  def getReferenceNames (): Iterable[String] = {
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

      SequenceRecord(index, name, length.toLong, null)
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
class SequenceRecord(val id: Int, val name: CharSequence, val length: Long, val url: CharSequence, val md5: CharSequence) extends Serializable {

  assert(name != null, "SequenceRecord.name is null")
  assert(name.length > 0, "SequenceRecord.name has length 0")
  assert(length > 0, "SequenceRecord.length <= 0")

  def withReferenceId(newId: Int): SequenceRecord =
    new SequenceRecord(newId, name, length, url, md5)

  override def equals(x: Any): Boolean = {
    x match {
      case y: SequenceRecord =>
        id == y.id && name == y.name && length == y.length && url == y.url && md5 == y.md5
      case _ => false
    }
  }

  override def hashCode: Int = ((id + name.hashCode) * 37 + length.hashCode) * 37

  override def toString: String = "%s->%s=%d".format(id, name, length)

  /**
   * Converts this sequence record into a SAM sequence record.
   *
   * @return A SAM formatted sequence record.
   */
  def toSAMSequenceRecord (): SAMSequenceRecord = {
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

  def apply(id: Int, name: CharSequence, length: Long, url: CharSequence = null, md5: CharSequence = null): SequenceRecord =
    new SequenceRecord(id, name, length, url, md5)


  /**
   * Converts an ADAM contig into a sequence record.
   *
   * @param ctg Contig to convert.
   * @return Contig expressed as a sequence record.
   */
  def fromADAMContigFragment (ctg: ADAMNucleotideContigFragment): SequenceRecord = {
    apply(ctg.getContigId, ctg.getContigName, ctg.getContigLength, ctg.getUrl)
  }

  /*
   * Generates a sequence record from a SAMSequence record.
   *
   * @param seqRecord SAM Sequence record input.
   * @return A new ADAM sequence record.
   */
  def fromSamSequenceRecord(seqRecord: SAMSequenceRecord): SequenceRecord = {
    apply(seqRecord.getSequenceIndex, seqRecord.getSequenceName, seqRecord.getSequenceLength, seqRecord.getAssembly)
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
          if (rec.getReadMapped)
            Set(SequenceRecord(rec.getReferenceId, rec.getReferenceName, rec.getReferenceLength, rec.getReferenceUrl))
          else
            Set()

        val right =
          if (rec.getMateMapped)
            Set(SequenceRecord(rec.getMateReferenceId, rec.getMateReference, rec.getMateReferenceLength, rec.getMateReferenceUrl))
          else
            Set()

        left ++ right

      } else {
        Set()
      }

    } else {

      if (rec.getReadMapped) {
        Set(SequenceRecord(rec.getReferenceId, rec.getReferenceName, rec.getReferenceLength, rec.getReferenceUrl))
      } else {
        // If the read isn't mapped, then ignore the fields altogether.
        Set()
      }
    }
  }

  def fromSpecificRecord(rec: SpecificRecord): SequenceRecord = {

    val schema = rec.getSchema
    if (schema.getField("referenceId") != null) {
      new SequenceRecord(
        rec.get(schema.getField("referenceId").pos()).asInstanceOf[Int],
        rec.get(schema.getField("referenceName").pos()).asInstanceOf[CharSequence],
        rec.get(schema.getField("referenceLength").pos()).asInstanceOf[Long],
        rec.get(schema.getField("referenceUrl").pos()).asInstanceOf[CharSequence],
        null)
    } else if (schema.getField("contig") != null) {
      val pos = schema.getField("contig").pos()
      val contig = rec.get(pos).asInstanceOf[ADAMContig]
      val contigSchema = contig.getSchema
      new SequenceRecord(
        contig.get(contigSchema.getField("contigId").pos()).asInstanceOf[Int],
        contig.get(contigSchema.getField("contigName").pos()).asInstanceOf[CharSequence],
        contig.get(contigSchema.getField("contigLength").pos()).asInstanceOf[Long],
        contig.get(contigSchema.getField("referenceURL").pos()).asInstanceOf[CharSequence],
        contig.get(contigSchema.getField("contigMD5").pos()).asInstanceOf[CharSequence])
    } else {
      null
    }
  }
}
