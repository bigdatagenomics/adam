/**
 * Copyright (c) 2013 Genome Bridge LLC
 */
package edu.berkeley.cs.amplab.adam.models

import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.math.Ordering.Implicits._

import scala.collection._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import net.sf.samtools.{SAMFileHeader, SAMFileReader, SAMSequenceRecord}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8

/**
 * SequenceDictionary contains the (bijective) map between Ints (the referenceId) and Strings (the referenceName)
 * from the header of a BAM file, or the combined result of multiple such SequenceDictionaries.
 */
class SequenceDictionary(recordsIn : Iterable[SequenceRecord]) extends Serializable {

  // Intermediate value used to ensure that no referenceName or referenceId is listed twice with a different
  // referenceId or referenceName (respectively).  Notice the "toSet", which means it's okay to pass an Iterable
  // that lists the _same_ SequenceRecord twice.
  private val idNamePairs = recordsIn.map( rec => ( rec.id, rec.name.toString )).toSet

  // check that no referenceId value is listed twice, to two different referenceNames
  assert( idNamePairs.groupBy( _._1 ).map( p => ( p._1, p._2.size ) ).filter( p => p._2 > 1 ).isEmpty,
    "Duplicate ID in %s".format(idNamePairs))

  // check that no referenceName is listed twice, to two different referenceIds
  assert( idNamePairs.groupBy( _._2 ).map( p => ( p._1, p._2.size ) ).filter( p => p._2 > 1 ).isEmpty,
    "Duplicate Name in %s".format(idNamePairs))

  // Pre-compute the hashCode, based on a sorted version of the idNamePairs list.
  private val _hashCode : Int = idNamePairs.toSeq.sortWith( _ < _ ).foldLeft(0) {
    ( hash : Int, p : (Int, CharSequence)) => 37 * (hash + p._1) + p._2.hashCode
  }

  // Maps referenceId -> SequenceRecord
  private val recordIndices : mutable.Map[Int,SequenceRecord] =
    mutable.Map(recordsIn.map {
      rec => ( rec.id, rec )
    }.toSeq : _*)

  // Maps referenceName -> SequenceRecord
  private val recordNames : mutable.Map[CharSequence,SequenceRecord] =
    mutable.Map(recordsIn.map {
      rec => ( rec.name, rec )
    }.toSeq : _*)

  def assignments : Map[Int,CharSequence] = recordIndices.map {
    case ( id : Int, rec : SequenceRecord ) =>
      ( id, rec.name )
  }

  def apply(id : Int) : SequenceRecord = recordIndices(id)
  def apply(name : CharSequence) : SequenceRecord = recordNames(name)

  /**
   * Produces a Map of Int -> Int which maps the referenceIds from this SequenceDictionary
   * into referenceIds compatible with the argument SequenceDictionary ('dict').
   *
   * There are basically three cases that we have to handle here:
   * (1) ids for the same sequence name which are different between the dictionaries. These are
   *     converted (from this.referenceId into dict.referenceId).
   * (2) ids which are in use (for different sequences) between the two dictionaries. In this case,
   *     we mint a new identifier (using nonoverlappingHash) for the sequence in this dictionary
   *     that won't conflict with any sequence in either dictionary.
   * (3) ids for sequences that aren't in the argument dict, and which don't conflict as in (2),
   *     can be carried over as-is.
   *
   * (Note: if the source referenceId isn't in the Map produced by mapTo, we can assume that it
   * can be used without change in the new dictionary.  The method remap, below, actually implements
   * this identity.)
   *
   * The results of this mapTo should be useable by remap to produce a "compatible" dictionary,
   * i.e. for all d1 and d2,
   *
   *   d1.remap(d1.mapTo(d2)).isCompatibleWith(d2)
   *
   * should be true.
   *
   * @param dict The target dictionary into whose referenceId space the ids of this dictionary should be mapped.
   * @return A Map whose values change the referenceIds in this dictionary; every referenceId in the source
   *         dictionary should be present in this Map
   */
  def mapTo(dict : SequenceDictionary) : Map[Int,Int] = {

    /*
     * we start by assuming that all the sequences in the target dictionary will maintain their
     * existing identifiers -- mapTo won't imply any changes to the id/sequence correspondence in
     * the target dictionary.
     */
    val assign : mutable.Map[Int,CharSequence] = mutable.Map(dict.assignments.toSeq : _*)

    /*
     * Next, for every source sequence that is _not_ in the target dictionary, there are two cases:
     * 1. the source ID is not in use in the target -- in this case, just carry over the existing
     *    identifier into the assignment.
     * 2. the source ID _is_ already in use in the assignment -- in this case, we assign a new identifier
     *    for the source sequence, and store it in the assignment.
     */
    recordNames.keys.filter( !dict.recordNames.contains(_) ).foreach {
      name =>
        val myIdx = recordNames(name).id
        if(assign.contains(myIdx)) {
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
    val rassign : Map[CharSequence,Int] = Map(assign.toSeq.map( p => (p._2, p._1) ) : _*)
    val idxMap : Map[Int,Int] = Map(recordIndices.keys.map( idx => ( idx, rassign(recordIndices(idx).name) ) ).toSeq : _*)

    assert( idxMap.keys.filter(!recordIndices.contains(_)).isEmpty,
      "There we keys in the mapTo Map that weren't actually sequence indices")
    assert( recordIndices.keys.filter(!idxMap.contains(_)).isEmpty,
      "There were keys which weren't remapped by the mapTo idxMap")

    idxMap
  }

  /**
   * See the note to mapTo, above.

   * The results of this remap and mapTo should be to produce a "compatible" dictionary,
   * i.e. for all d1 and d2,
   *
   *   d1.remap(d1.mapTo(d2)).isCompatibleWith(d2)
   *
   * should be true.
   *
   * @param idTransform The Map[Int,Int] to transform the identifiers of this dictionary; e.g. the output of
   *                    mapTo.
   * @return A new SequenceDictionary with just the referenceIds mapped through the given Map argument.
   */
  def remap(idTransform : Map[Int,Int]) : SequenceDictionary = {
    def remapIndex(i : Int) : Int =
      if(idTransform.contains(i)) idTransform(i) else i

    SequenceDictionary(idNamePairs.map {
      case ( id, name ) =>
        recordIndices(id).withReferenceId(remapIndex(id))
    }.toSeq : _*)
  }

  def records : Seq[SequenceRecord] = recordIndices.values.toSeq

  def +(rec : SequenceRecord) : SequenceDictionary =
    new SequenceDictionary(recordsIn ++ List(rec))

  def +=(rec : SequenceRecord) : SequenceDictionary = {

    recordIndices.put(rec.id, rec)
    recordNames.put(rec.name, rec)
    this
  }

  def ++(dict : SequenceDictionary) : SequenceDictionary =
    new SequenceDictionary(recordsIn ++ dict.records)

  def ++(recs : Seq[SequenceRecord]) : SequenceDictionary =
    recs.foldRight(this)((rec, dict) => dict + rec)

  def ++=(recs : Seq[SequenceRecord]) : SequenceDictionary = {
    recs.foreach {
      rec => this += rec
    }
    this
  }

  def ++=(dict : SequenceDictionary) : SequenceDictionary = {
    dict.recordIndices.keys.foreach {
      idx => {
        val newrec = dict.recordIndices(idx)
        recordIndices.put(newrec.id, newrec)
        recordNames.put(newrec.name, newrec)
      }
    }
    this
  }

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
  def isCompatibleWith(dict : SequenceDictionary) : Boolean =
    recordIndices.keys.filter( dict.recordIndices.contains ).filter(idx => recordIndices(idx) != dict.recordIndices(idx)).isEmpty &&
      recordNames.keys.filter( dict.recordNames.contains ).filter(name => recordNames(name) != dict.recordNames(name)).isEmpty

  def nonoverlappingHash(x : CharSequence) : Int =
    SequenceDictionary.nonoverlappingHash(x, idx => recordIndices.contains(idx))

  override def equals(x : Any) : Boolean = {
    x match {
      case d : SequenceDictionary =>
        recordNames == d.recordNames && recordIndices == d.recordIndices
      case _ => false
    }
  }

  override def hashCode() : Int = _hashCode

  override def toString : String = idNamePairs.toString()
}

object SequenceDictionary {

  def apply(recordsIn : SequenceRecord*) = new SequenceDictionary(recordsIn)

  def fromSAMHeader(header : SAMFileHeader) : SequenceDictionary = {
    val samDict = header.getSequenceDictionary
    val seqDict : SequenceDictionary =
      SequenceDictionary(
        samDict.getSequences.map {
          seqRecord : SAMSequenceRecord =>
            SequenceRecord(seqRecord.getSequenceIndex, seqRecord.getSequenceName,
              seqRecord.getSequenceLength, null)
        } : _*)

    seqDict
  }

  def fromSAMReader(samReader : SAMFileReader) : SequenceDictionary =
    fromSAMHeader(samReader.getFileHeader)

  def nonoverlappingHash(x : CharSequence, conflicts : Int => Boolean) : Int = {
    var hash = x.hashCode
    while(conflicts(hash)) { hash += 1 }
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
 */
class SequenceRecord(val id : Int, val name : CharSequence, val length : Long, val url : CharSequence) extends Serializable {

  assert( name != null, "SequenceRecord.name is null" )
  assert( name.length > 0, "SequenceRecord.name has length 0" )
  assert( length > 0, "SequenceRecord.length <= 0" )

  def withReferenceId(newId : Int) : SequenceRecord =
    new SequenceRecord(newId, name, length, url)

  override def equals(x : Any) : Boolean = {
    x match {
      case y : SequenceRecord =>
        id == y.id && name == y.name && length == y.length && url == y.url
      case _ => false
    }
  }

  override def hashCode: Int = ((id + name.hashCode) * 37 + length.hashCode) * 37

  override def toString : String = "%s->%s=%d".format(id, name, length)
}

object SequenceRecord {

  def apply(id : Int, name : CharSequence, length : Long, url : CharSequence = null) : SequenceRecord =
    new SequenceRecord(id, name, length, url)

  /**
   * Convert an ADAMRecord into one or more SequenceRecords.
   * The reason that we can't simply use the "fromSpecificRecord" method, below, is that each ADAMRecord
   * can (through the fact that it could be a pair of reads) contain 1 or 2 possible SequenceRecord entries
   * for the SequenceDictionary itself.  Both have to be extracted, separately.
   *
   * @param rec The ADAMRecord from which to extract the SequenceRecord entries
   * @return a list of all SequenceRecord entries derivable from this record.
   */
  def fromADAMRecord(rec : ADAMRecord) : Seq[SequenceRecord] = {

    assert( rec != null, "ADAMRecord was null" )

    if(rec.getReadPaired) {

      // only process a read pair, if we're looking at the first element of the pair.
      if(rec.getFirstOfPair) {

        val left =
          if(rec.getReadMapped)
            List(SequenceRecord(rec.getReferenceId, rec.getReferenceName, rec.getReferenceLength, rec.getReferenceUrl))
          else
            List()

        val right =
          if(rec.getMateMapped)
            List(SequenceRecord(rec.getMateReferenceId, rec.getMateReference, rec.getMateReferenceLength, rec.getMateReferenceUrl))
          else
            List()

        left ++ right

      } else {
        List()
      }

    } else {

      if(rec.getReadMapped) {
        List(SequenceRecord(rec.getReferenceId, rec.getReferenceName, rec.getReferenceLength, rec.getReferenceUrl))
      } else {
        // If the read isn't mapped, then ignore the fields altogether.
        List()
      }
    }
  }

  def fromSpecificRecord(rec : SpecificRecord) : SequenceRecord = {

    val schema = rec.getSchema

    new SequenceRecord(
      rec.get(schema.getField("referenceId").pos()).asInstanceOf[Int],
      rec.get(schema.getField("referenceName").pos()).asInstanceOf[CharSequence],
      rec.get(schema.getField("referenceLength").pos()).asInstanceOf[Long],
      rec.get(schema.getField("referenceUrl").pos()).asInstanceOf[CharSequence])

  }

}
