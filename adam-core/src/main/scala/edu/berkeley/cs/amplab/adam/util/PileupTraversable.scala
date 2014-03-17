/*
 * Copyright (c) 2013. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.util

import parquet.hadoop.ParquetInputFormat
import parquet.avro.AvroReadSupport
import edu.berkeley.cs.amplab.adam.predicates.LocusPredicate
import org.apache.hadoop.mapreduce.Job
import scala.collection.JavaConversions._
import net.sf.samtools.{TextCigarCodec, CigarOperator}
import scala.collection.mutable.ListBuffer
import parquet.hadoop.util.ContextUtil
import scala.collection.SortedMap
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Base extends Enumeration with Serializable {
  val A, C, T, G, N = Value
}

abstract class PileupEvent(readName: String) extends Serializable

case class MatchEvent(readName: String,
                      isReverseStrand: Boolean,
                      eventOffset: Int,
                      eventLength: Int,
                      mapQ: Int,
                      qual: Int) extends PileupEvent(readName)

case class MismatchEvent(readName: String,
                         mismatchedBase: Base.Value,
                         isReverseStrand: Boolean,
                         eventOffset: Int,
                         eventLength: Int,
                         mapQ: Int,
                         qual: Int) extends PileupEvent(readName)

case class InsertionEvent(readName: String,
                          insertedSequence: String,
                          mapQ: Int,
                          qual: Int) extends PileupEvent(readName)

case class DeletionEvent(readName: String,
                         eventOffset: Int,
                         eventLength: Int,
                         mapQ: Int,
                         qual: Int) extends PileupEvent(readName)

class Pileup(val referenceId: Int,
             val referencePosition: Long,
             val referenceName: String,
             val referenceBase: Option[Base.Value],
             val matches: List[MatchEvent] = List.empty,
             val mismatches: List[MismatchEvent] = List.empty,
             val insertions: List[InsertionEvent] = List.empty,
             val deletes: List[DeletionEvent] = List.empty) extends Serializable {
  val numReads = matches.length + mismatches.length + insertions.length + deletes.length
}

object PileupTraversable {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

  def apply(reads: RDD[ADAMRecord]) {
    new PileupTraversable(reads)
  }
}

class PileupTraversable(reads: RDD[ADAMRecord]) extends Traversable[Pileup] with Serializable {

  def stringToQualitySanger(score: String): Int = {
    try {
      // quality score is padded by 33
      score.toInt - 33
    } catch {
      // thrown if phred score is omitted
      case nfe: NumberFormatException => return 0
    }
  }

  def readToPileups(record: ADAMRecord): List[Pileup] = {
    if (record == null || record.getCigar == null || record.getMismatchingPositions == null) {
      // TODO: log this later... We can't create a pileup without the CIGAR and MD tag
      // in the future, we can also get reference information from a reference file
      return List.empty
    }

    def baseFromSequence(pos: Int): Base.Value = {
      val baseString = record.getSequence.subSequence(pos, pos + 1).toString
      Base.withName(baseString)
    }

    var referencePos = record.getStart
    val isReverseStrand = record.getReadNegativeStrand
    var readPos = 0

    val cigar = PileupTraversable.CIGAR_CODEC.decode(record.getCigar.toString)
    val mdTag = MdTag(record.getMismatchingPositions.toString, referencePos)

    var pileupList = List[Pileup]()

    cigar.getCigarElements.foreach(cigarElement =>
      cigarElement.getOperator match {

        // INSERT
        case CigarOperator.I =>
          val insertEvent = new InsertionEvent(record.getReadName.toString, record.getSequence.toString,
            record.getMapq.toInt, stringToQualitySanger(record.getQual.toString))
          pileupList ::= new Pileup(record.getReferenceId, referencePos, record.getReferenceName.toString,
            None, insertions = List(insertEvent))
          readPos += cigarElement.getLength

        // MATCH (sequence match or mismatch)
        case CigarOperator.M =>

          for (i <- 0 until cigarElement.getLength) {

            if (mdTag.isMatch(referencePos)) {
              // sequence match
              val matchEvent = new MatchEvent(record.getReadName.toString, isReverseStrand, i, cigarElement.getLength,
                record.getMapq.toInt, stringToQualitySanger(record.getQual.toString))
              pileupList ::= new Pileup(record.getReferenceId, referencePos, record.getReferenceName.toString,
                Some(baseFromSequence(readPos)), matches = List(matchEvent))
            } else {
              val mismatchBase = mdTag.mismatchedBase(referencePos)
              if (mismatchBase.isEmpty) {
                throw new IllegalArgumentException("Cigar match has no MD (mis)match @" + referencePos + " "
                  + record.getCigar + " " + record.getMismatchingPositions)
              }
              val mismatchEvent = new MismatchEvent(record.getReadName.toString,
                baseFromSequence(readPos), isReverseStrand, i, cigarElement.getLength,
                record.getMapq.toInt, stringToQualitySanger(record.getQual.toString))
              pileupList ::= new Pileup(record.getReferenceId, referencePos, record.getReferenceName.toString,
                Some(Base.withName(mismatchBase.get.toString)), mismatches = List(mismatchEvent))
            }

            readPos += 1
            referencePos += 1
          }

        // DELETE
        case CigarOperator.D =>

          for (i <- 0 until cigarElement.getLength) {
            val deletedBase = mdTag.deletedBase(referencePos)
            if (deletedBase.isEmpty) {
              throw new IllegalArgumentException("CIGAR delete but the MD tag is not a delete")
            }
            val deleteEvent = new DeletionEvent(record.getReadName.toString, i, cigarElement.getLength,
              record.getMapq.toInt, stringToQualitySanger(record.getQual.toString))
            pileupList ::= new Pileup(record.getReferenceId, referencePos, record.getReferenceName.toString,
              Some(Base.withName(deletedBase.get.toString)), deletes = List(deleteEvent))
            // Consume reference bases but not read bases
            referencePos += 1
          }

        // All other cases (TODO: add X and EQ?)
        case _ =>
          if (cigarElement.getOperator.consumesReadBases()) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases()) {
            referencePos += cigarElement.getLength
          }
      }
    )

    pileupList
  }

  def foreach[U](f: (Pileup) => U) {
    // TODO: use a Priority Queue as soon as this Scala bug is fixed
    // https://issues.scala-lang.org/browse/SI-7568
    // val queue = new collection.mutable.PriorityQueue[(Int, Long)]()
    var pileups = SortedMap[Long, ListBuffer[Pileup]]()

    var currentReference: Option[Int] = None
    var currentReferencePosition: Option[Long] = None

    def flushPileups(beforePosition: Option[Long] = None) {
      val locationsToFlush = beforePosition match {
        case Some(v) =>
          pileups.keySet.filter(_ < v)
        case None =>
          pileups.keySet
      }
      for (location <- locationsToFlush) {
        var matches = ListBuffer[MatchEvent]()
        var mismatches = ListBuffer[MismatchEvent]()
        var deletes = ListBuffer[DeletionEvent]()
        var inserts = ListBuffer[InsertionEvent]()
        val pileupsAtLocation = pileups(location)
        var referenceName: Option[String] = None
        var referenceBase: Option[Base.Value] = None
        pileupsAtLocation foreach {
          pileup =>
            assert(pileup.referenceId == currentReference.get)
            assert(pileup.referencePosition == location)
            matches ++= pileup.matches
            mismatches ++= pileup.mismatches
            deletes ++= pileup.deletes
            inserts ++= pileup.insertions
            if (referenceName.isDefined) {
              assert(referenceName == Some(pileup.referenceName))
            }
            if (referenceBase.isDefined && pileup.referenceBase.isDefined) {
              assert(referenceBase == pileup.referenceBase)
            }
            referenceName = Some(pileup.referenceName)
            if (pileup.referenceBase.isDefined) {
              referenceBase = pileup.referenceBase
            }
        }
        f(new Pileup(currentReference.get, location, referenceName.get, referenceBase,
          matches.toList, mismatches.toList, inserts.toList, deletes.toList))
      }
      pileups --= locationsToFlush
    }

    for (read: ADAMRecord <- reads) {

      def updateCurrentInfo(read: ADAMRecord) = {
        currentReference = Some(read.getReferenceId)
        currentReferencePosition = Some(read.getStart)
      }

      currentReference match {
        case Some(reference) =>
          if (reference != read.getReferenceId.toInt) {
            // We're starting a new reference, flush all events from the previous reference
            flushPileups()
            updateCurrentInfo(read)
          } else {
            // We're process the same reference, make sure that the reads are arriving sorted
            assert(read.getStart >= currentReferencePosition.get, "You can only create pileups on sorted BAM/ADAM files")
          }
        case None =>
          updateCurrentInfo(read)
      }

      for (pileup <- readToPileups(read)) {
        pileups.get(pileup.referencePosition) match {
          case Some(pileupsFound) => pileupsFound += pileup
          case None =>
            pileups += (pileup.referencePosition -> ListBuffer(pileup))
        }
      }

      // Flush all pileups before the start of this read since they are completed
      flushPileups(Some(read.getStart))
    }

    // Flush any remaining pileups
    flushPileups()
  }
}
