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
package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.algorithms.consensus.{ConsensusGenerator, ConsensusGeneratorFromReads}
import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.{RealignmentTargetFinder,
                                                                 IndelRealignmentTarget,
                                                                 TargetOrdering,
                                                                 ZippedTargetOrdering,
                                                                 TargetSet,
                                                                 ZippedTargetSet}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.{Consensus, ReferencePosition, ReferenceRegion}
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.rich.RichCigar
import edu.berkeley.cs.amplab.adam.rich.RichCigar._
import edu.berkeley.cs.amplab.adam.util.MdTag
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions._
import edu.berkeley.cs.amplab.adam.util.NormalizationUtils._
import net.sf.samtools.{Cigar, CigarOperator, CigarElement}
import org.apache.spark.{Logging, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import scala.collection.mutable.Map
import scala.collection.immutable.{NumericRange, TreeSet}
import scala.util.Random

private[rdd] object RealignIndels {

  /**
   * Realigns an RDD of reads.
   *
   * @param rdd RDD of reads to realign.
   * @return RDD of realigned reads.
   */
  def apply(rdd: RDD[ADAMRecord],
            consensusModel: ConsensusGenerator = new ConsensusGeneratorFromReads,
            dataIsSorted: Boolean = false,
            maxIndelSize: Int = 500,
            maxConsensusNumber: Int = 30,
            lodThreshold: Double = 5.0,
            maxTargetSize: Int = 3000): RDD[ADAMRecord] = {
    new RealignIndels(consensusModel,
                      dataIsSorted, 
                      maxIndelSize, 
                      maxConsensusNumber, 
                      lodThreshold,
                      maxTargetSize).realignIndels(rdd)
  }

  /**
   * Method to map a record to an indel realignment target. Returns the index of the target to align to if the read has a
   * target and should be realigned, else returns the "empty" target (denoted by a negative index).
   * 
   * @note Generally, this function shouldn't be called directly---for most cases, prefer mapTargets.
   * @note This function should not be called in a context where target set serialization is needed.
   * Instead, call mapToTarget(RichADAMRecord, ZippedTargetSet), which wraps this function.
   * 
   * @param read Read to check.
   * @param targets Sorted set of realignment targets.
   * @return If overlapping target is found, returns that target. Else, returns the "empty" target.
   *
   * @see mapTargets
   */
  @tailrec final def mapToTarget (read: RichADAMRecord,
                                  targets: TreeSet[Tuple2[IndelRealignmentTarget, Int]]): Int = {
    // Perform tail call recursive binary search
    if (targets.size == 1) {
      if (TargetOrdering.contains (targets.head._1, read)) {
        // if there is overlap, return the overlapping target
        targets.head._2
      } else {
        // else, return an empty target (negative index)
        // to prevent key skew, split up by max indel alignment length
        (-1 - (read.record.getStart() / 3000L)).toInt
      }
    } else {
      // split the set and recurse
      val (head, tail) = targets.splitAt(targets.size / 2)
      val reducedSet = if (TargetOrdering.lt(tail.head._1, read)) {
        head
      } else {
        tail
      }
      mapToTarget (read, reducedSet)
    }
  }

  /**
   * This method wraps mapToTarget(RichADAMRecord, TreeSet[Tuple2[IndelRealignmentTarget, Int]]) for
   * serialization purposes.
   *
   * @param read Read to check.
   * @param targets Wrapped zipped indel realignment target.
   * @return Target if an overlapping target is found, else the empty target.
   *
   * @see mapTargets
   */
  def mapToTarget (read: RichADAMRecord,
                   targets: ZippedTargetSet): Int = {
    mapToTarget(read, targets.set)
  }

  /**
   * Method to map a target index to an indel realignment target.
   *
   * @note Generally, this function shouldn't be called directly---for most cases, prefer mapTargets.
   * @note This function should not be called in a context where target set serialization is needed.
   * Instead, call mapToTarget(Int, ZippedTargetSet), which wraps this function.
   *
   * @param int Index of target.
   * @param targets Set of realignment targets.
   * @return Indel realignment target.
   * 
   * @see mapTargets
   */
  def mapToTargetUnpacked (targetIndex: Int, 
                           targets: TreeSet[Tuple2[IndelRealignmentTarget, Int]]): Option[IndelRealignmentTarget] = {
    if (targetIndex < 0) {
      None
    } else {
      Some(targets.filter(p => p._2 == targetIndex).head._1)
    }
  }

  /**
   * Wrapper for mapToTarget(Int, TreeSet[Tuple2[IndelRealignmentTarget, Int]]) for contexts where
   * serialization is needed.
   *
   * @param int Index of target.
   * @param targets Set of realignment targets.
   * @return Indel realignment target.
   * 
   * @see mapTargets
   */
  def mapToTarget (targetIndex: Int, targets: ZippedTargetSet): Option[IndelRealignmentTarget] = {
    mapToTargetUnpacked(targetIndex, targets.set)
  }

  /**
   * Maps reads to targets. Wraps both mapToTarget functions together and handles target index creation and broadcast.
   *
   * @note This function may return multiple sets of reads mapped to empty targets. This is intentional. For typical workloads, there
   * will be many more reads that map to the empty target (reads that do not need to be realigned) than reads that need to be realigned.
   * Thus, we must spread the reads that do not need to be realigned across multiple empty targets to reduce imbalance.
   *
   * @param rich_rdd RDD containing RichADAMRecords which are to be mapped to a realignment target.
   * @param targets Set of targets that are to be mapped against.
   *
   * @return A key-value pair RDD with realignment targets matched with sets of reads.
   * 
   * @see mapToTarget
   */
  def mapTargets (rich_rdd: RDD[RichADAMRecord], targets: TreeSet[IndelRealignmentTarget]): RDD[(Option[IndelRealignmentTarget], Seq[RichADAMRecord])] = {
    val tmpZippedTargets = targets.zip(0 until targets.count(t => true))
    var tmpZippedTargets2 = new TreeSet[Tuple2[IndelRealignmentTarget, Int]]()(ZippedTargetOrdering)
    tmpZippedTargets.foreach(t => tmpZippedTargets2 = tmpZippedTargets2 + t)
    val zippedTargets = new ZippedTargetSet(tmpZippedTargets2)
    
    // group reads by target
    val broadcastTargets = rich_rdd.context.broadcast(zippedTargets)
    val readsMappedToTarget = rich_rdd.groupBy(mapToTarget(_, broadcastTargets.value))
      .map(kv => {
        val (k, v) = kv
        
        val target = mapToTarget(k, broadcastTargets.value)
        
        (target, v)
      })
    
    readsMappedToTarget
  }

  /**
   * From a set of reads, returns the reference sequence that they overlap.
   */
  def getReferenceFromReads (reads: Seq[RichADAMRecord]): (String, Long, Long) = {
    // get reference and range from a single read
    val readRefs = reads.map((r: RichADAMRecord) => {
      (r.mdTag.get.getReference(r), r.getStart.toLong to r.end.get)
    })
      .sortBy(_._2.head)

    // fold over sequences and append - sequence is sorted at start
    val ref = readRefs.reverse.foldRight[(String,Long)](("", readRefs.head._2.head))((refReads: (String, NumericRange[Long]), reference: (String, Long)) => {
      if (refReads._2.end < reference._2) {
        reference
      } else if (reference._2 >= refReads._2.head) {
        (reference._1 + refReads._1.substring((reference._2 - refReads._2.head).toInt), refReads._2.end)
      } else {
        // there is a gap in the sequence
        throw new IllegalArgumentException("Current sequence has a gap at " + reference._2 + "with " + refReads._2.head + "," + refReads._2.end + ".")
      }
    })

    (ref._1, readRefs.head._2.head, ref._2)
  }
}

import RealignIndels._

private[rdd] class RealignIndels (val consensusModel: ConsensusGenerator = new ConsensusGeneratorFromReads,
                                  val dataIsSorted: Boolean = false,
                                  val maxIndelSize: Int = 500,
                                  val maxConsensusNumber: Int = 30,
                                  val lodThreshold: Double = 5.0,
                                  val maxTargetSize: Int = 3000) extends Serializable with Logging {
  /**
   * Given a target group with an indel realignment target and a group of reads to realign, this method
   * generates read consensuses and realigns reads if a consensus leads to a sufficient improvement.
   *
   * @param targetGroup A tuple consisting of an indel realignment target and a seq of reads
   * @return A sequence of reads which have either been realigned if there is a sufficiently good alternative
   * consensus, or not realigned if there is not a sufficiently good consensus.
   */
  def realignTargetGroup (targetGroup: (Option[IndelRealignmentTarget], Seq[RichADAMRecord])): Seq[RichADAMRecord] = {
    val (target, reads) = targetGroup
    var realignedReads = reads.filter(r => r.mdTag.isDefined && !r.mdTag.get.hasMismatches)

    // get reference from reads
    val (reference, refStart, refEnd) = getReferenceFromReads(reads.map(r => new RichADAMRecord(r)))
    val refRegion = ReferenceRegion(reads.head.record.getReferenceId, refStart, refEnd)

    // preprocess reads and get consensus
    val readsToClean = consensusModel.preprocessReadsForRealignment(reads.filter(r => !r.mdTag.isDefined || r.mdTag.get.hasMismatches),
                                                                    reference,
                                                                    refRegion)
    var consensus = consensusModel.findConsensus(readsToClean)

    // reduce count of consensus sequences
    if (consensus.length > maxConsensusNumber) {
      val r = new Random()
      consensus = r.shuffle(consensus).take(maxConsensusNumber)
    }

    if (target.isEmpty) {
      // if the indel realignment target is empty, do not realign
      reads
    } else {

      var mismatchSum = 0L

      if(readsToClean.length > 0 && consensus.length > 0) {

        // do not check realigned reads - they must match
        val totalMismatchSumPreCleaning = readsToClean.map(sumMismatchQuality(_)).reduce(_ + _)

        /* list to log the outcome of all consensus trials. stores:  
         *  - mismatch quality of reads against new consensus sequence
         *  - the consensus sequence itself
         *  - a map containing each realigned read and it's offset into the new sequence
         */
        var consensusOutcomes = List[(Int, Consensus, Map[RichADAMRecord,Int])]()

        // loop over all consensuses and evaluate
        consensus.foreach(c => {
          // generate a reference sequence from the consensus
          val consensusSequence = c.insertIntoReference(reference, refStart, refEnd)

          // evaluate all reads against the new consensus
          val sweptValues = readsToClean.map(r => {
            val (qual, pos) = sweepReadOverReferenceForQuality(r.getSequence, consensusSequence, r.qualityScores.map(_.toInt))
            val originalQual = sumMismatchQuality(r)

            // if the read's mismatch quality improves over the original alignment, save 
            // its alignment in the consensus sequence, else store -1 
            if (qual < originalQual) {
              (r, (qual, pos))
            } else {
              (r, (originalQual, -1))
            }
          })

          // sum all mismatch qualities to get the total mismatch quality for this alignment
          val totalQuality = sweptValues.map(_._2._1).reduce(_ + _)

          // package data
          var readMappings = Map[RichADAMRecord,Int]()
          sweptValues.map(kv => (kv._1, kv._2._2)).foreach(m => {
            readMappings += (m._1 -> m._2)
          })

          // add to outcome list
          consensusOutcomes = (totalQuality, c, readMappings) :: consensusOutcomes
        })

        // perform reduction to pick the consensus with the lowest aggregated mismatch score
        val bestConsensusTuple = consensusOutcomes.reduce ((c1: (Int, Consensus, Map[RichADAMRecord, Int]), c2: (Int, Consensus, Map[RichADAMRecord, Int])) => {
          if (c1._1 <= c2._1) {
            c1
          } else {
            c2
          }
        })

        val (bestConsensusMismatchSum, bestConsensus, bestMappings) = bestConsensusTuple

        // check for a sufficient improvement in mismatch quality versus threshold
        if ((totalMismatchSumPreCleaning - bestConsensusMismatchSum).toDouble / 10.0 > lodThreshold) {

          // if we see a sufficient improvement, realign the reads
          val cleanedReads : Seq[RichADAMRecord] = readsToClean.map(r => {

            val builder : ADAMRecord.Builder = ADAMRecord.newBuilder(r)
            val remapping = bestMappings(r)

            // if read alignment is improved by aligning against new consensus, realign
            if (remapping != -1) {
              // bump up mapping quality by 10
              builder.setMapq(r.getMapq + 10)

              // set new start to consider offset
              builder.setStart(refStart + remapping)

              // recompute cigar
              val newCigar: Cigar = if (refStart + remapping >= bestConsensus.index.start && refStart + remapping <= bestConsensus.index.end - 1) {
                // if element overlaps with consensus indel, modify cigar with indel
                val (idElement, endLength) = if(bestConsensus.index.start == bestConsensus.index.end - 1) {
                  (new CigarElement(bestConsensus.consensus.length, CigarOperator.I),
                    r.getSequence.length - bestConsensus.consensus.length - (bestConsensus.index.start - (refStart + remapping)))
                } else {
                  (new CigarElement((bestConsensus.index.end - 1 - bestConsensus.index.start).toInt, CigarOperator.D),
                    r.getSequence.length - (bestConsensus.index.start - (refStart + remapping)))
                }

                val cigarElements = List[CigarElement](new CigarElement((refStart + remapping - bestConsensus.index.start).toInt, CigarOperator.M),
                  idElement,
                  new CigarElement(endLength.toInt, CigarOperator.M))

                new Cigar(cigarElements)
              } else {
                // else, new cigar is all matches
                new Cigar(List[CigarElement](new CigarElement(r.getSequence.length, CigarOperator.M)))
              }

              // update mdtag and cigar
              builder.setMismatchingPositions(MdTag.moveAlignment(r, newCigar, reference.drop(remapping), refStart + remapping).toString)
              builder.setCigar(newCigar.toString)
              new RichADAMRecord(builder.build())
              // TODO: fix mismatchingPositions string
            } else
              new RichADAMRecord(builder.build())
          })

          realignedReads = cleanedReads ++ realignedReads
        } else {
          realignedReads = readsToClean ++ realignedReads
        }
      }

      // return all reads that we cleaned and all reads that were initially realigned
      realignedReads
    }
  }

  /**
   * Sweeps the read across a reference sequence and evaluates the mismatch quality at each position. Returns
   * the alignment offset that leads to the lowest mismatch quality score. Invariant: reference sequence must be
   * longer than the read sequence.
   *
   * @param read Read to test.
   * @param reference Reference sequence to sweep across.
   * @param qualities Integer sequence of phred scaled base quality scores.
   * @return Tuple of (mismatch quality score, alignment offset).
   */
  def sweepReadOverReferenceForQuality (read: String, reference: String, qualities: Seq[Int]): (Int, Int) = {

    var qualityScores = List[(Int, Int)]()

    // calculate mismatch quality score for all admissable alignment offsets
    for (i <- 0 until (reference.length - read.length)) {
      val qualityScore = sumMismatchQualityIgnoreCigar(read, reference.substring(i, i + read.length), qualities)
      qualityScores = (qualityScore, i) :: qualityScores
    }

    // perform reduction to get best quality offset
    qualityScores.reduce ((p1: (Int, Int), p2: (Int, Int)) => {
      if (p1._1 < p2._1) {
        p1
      } else {
        p2
      }
    })
  }

  /**
   * Sums the mismatch quality of a read against a reference. Mismatch quality is defined as the sum
   * of the base quality for all bases in the read that do not match the reference. This method
   * ignores the cigar string, which treats indels as causing mismatches.
   *
   * @param read Read to evaluate.
   * @param reference Reference sequence to look for mismatches against.
   * @param qualities Sequence of base quality scores.
   * @return Mismatch quality sum.
   */
  def sumMismatchQualityIgnoreCigar (read: String, reference: String, qualities: Seq[Int]): Int = {
    val mismatchQualities = read.zip(reference)
      .zip(qualities)
      .filter(r => r._1._1 != r._1._2)
      .map(_._2)

    if (mismatchQualities.length > 0) {
      mismatchQualities.reduce(_ + _)
    } else {
      0
    }
  }

  /**
   * Given a read, sums the mismatch quality against it's current alignment position.
   * Does NOT ignore cigar.
   *
   * @param read Read over which to sum mismatch quality.
   * @return Mismatch quality of read for current alignment.
   */
  def sumMismatchQuality (read: ADAMRecord): Int = {
    sumMismatchQualityIgnoreCigar (read.getSequence, 
                                   read.mdTag.get.getReference(read), 
                                   read.qualityScores.map(_.toInt))
  }

  /**
   * Performs realignment for an RDD of reads. This includes target generation, read/target
   * classification, and read realignment.
   *
   * @param rdd Reads to realign.
   * @return Realigned read.
   */
  def realignIndels (rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    val sortedRdd = if (dataIsSorted) {
      rdd.filter(r => r.getReadMapped)
    } else {
      val sr = rdd.filter(r => r.getReadMapped)
        .keyBy(r => ReferencePosition(r).get)
        .sortByKey()

      sr.map(kv => kv._2)
    }

    // we only want to convert once so let's get it over with
    val rich_rdd = sortedRdd.map(new RichADAMRecord(_))

    // find realignment targets
    log.info("Generating realignment targets...")
    val targets: TreeSet[IndelRealignmentTarget] = RealignmentTargetFinder(rich_rdd,
                                                                           maxIndelSize,
                                                                           maxTargetSize)

    // map reads to targets
    log.info("Grouping reads by target...")
    val readsMappedToTarget = RealignIndels.mapTargets(rich_rdd, targets)

    // realign target groups
    log.info("Sorting reads by reference in ADAM RDD")
    readsMappedToTarget.flatMap(realignTargetGroup).map(r => r.record)
  }

}
