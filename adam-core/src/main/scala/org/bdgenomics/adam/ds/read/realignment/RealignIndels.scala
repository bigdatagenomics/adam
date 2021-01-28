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
package org.bdgenomics.adam.ds.read.realignment

import htsjdk.samtools.{ Cigar, CigarElement, CigarOperator }
import grizzled.slf4j.Logging
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.algorithms.consensus.{ Consensus, ConsensusGenerator }
import org.bdgenomics.adam.models.{ MdTag, ReferencePosition, ReferenceRegion }
import org.bdgenomics.adam.rich.{ RichAlignment, RichCigar }
import org.bdgenomics.adam.rich.RichAlignment._
import org.bdgenomics.adam.util.ReferenceFile
import org.bdgenomics.formats.avro.Alignment
import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.immutable.{ NumericRange, TreeSet }
import scala.collection.mutable

private[read] object RealignIndels extends Serializable with Logging {

  /**
   * Realigns an RDD of reads.
   *
   * @param rdd RDD of reads to realign.
   * @return RDD of realigned reads.
   */
  def apply(
    rdd: RDD[Alignment],
    consensusModel: ConsensusGenerator = ConsensusGenerator.fromReads,
    dataIsSorted: Boolean = false,
    maxIndelSize: Int = 500,
    maxConsensusNumber: Int = 30,
    lodThreshold: Double = 5.0,
    maxTargetSize: Int = 3000,
    maxReadsPerTarget: Int = 20000,
    optReferenceFile: Option[ReferenceFile] = None,
    unclipReads: Boolean = false): RDD[Alignment] = {
    new RealignIndels(
      rdd.context,
      consensusGenerator = consensusModel,
      dataIsSorted = dataIsSorted,
      maxIndelSize = maxIndelSize,
      maxConsensusNumber = maxConsensusNumber,
      lodThreshold = lodThreshold,
      maxTargetSize = maxTargetSize,
      maxReadsPerTarget = maxReadsPerTarget,
      optReferenceFile = optReferenceFile,
      unclipReads = unclipReads
    ).realignIndels(rdd)
  }

  /**
   * Method to map a record to an indel realignment target. Returns the index of the target to align to if the read has a
   * target and should be realigned, else returns the "empty" target (denoted by a negative index).
   *
   * @note Generally, this function shouldn't be called directly---for most cases, prefer mapTargets.
   * @param read Read to check.
   * @param targets Sorted array of realignment targets.
   * @return If overlapping target is found, returns that target. Else, returns the "empty" target.
   *
   * @see mapTargets
   */
  @tailrec final def mapToTarget(
    read: RichAlignment,
    targets: Array[IndelRealignmentTarget],
    headIdx: Int,
    tailIdx: Int,
    targetsToDrop: Set[Int] = Set.empty): Int = {
    // Perform tail call recursive binary search
    if (TargetOrdering.contains(targets(headIdx), read)) {
      // if there is overlap, return the overlapping target, unless it has been
      // flagged to be dropped, in which case, return an empty target (negative
      // index)
      if (targetsToDrop(headIdx)) {
        // read names are unique and faster to hash
        -Option(read.record.getReadName).getOrElse(read.record).hashCode.abs
      } else {
        headIdx
      }
    } else if (tailIdx - headIdx <= 1) {
      // else, return an empty target (negative index)
      // read names are unique and faster to hash
      -Option(read.record.getReadName).getOrElse(read.record).hashCode.abs
    } else {
      // split the set and recurse
      val splitIdx = headIdx + ((tailIdx - headIdx) / 2)

      if (TargetOrdering.contains(targets(splitIdx), read)) {
        if (targetsToDrop(splitIdx)) {
          // read names are unique and faster to hash
          -Option(read.record.getReadName).getOrElse(read.record).hashCode.abs
        } else {
          splitIdx
        }
      } else {
        val (newHeadIdx, newTailIdx) = if (TargetOrdering.lt(targets(splitIdx), read)) {
          (splitIdx, tailIdx)
        } else {
          (headIdx, splitIdx)
        }
        mapToTarget(read,
          targets,
          newHeadIdx,
          newTailIdx,
          targetsToDrop = targetsToDrop)
      }
    }
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
   * @param maxReadsPerTarget The maximum number of reads to allow per target.
   *
   * @return A key-value pair RDD with realignment targets matched with sets of reads.
   *
   * @see mapToTarget
   */
  def mapTargets(
    rich_rdd: RDD[RichAlignment],
    targets: Array[IndelRealignmentTarget],
    maxReadsPerTarget: Int = Int.MaxValue): RDD[(Option[(Int, IndelRealignmentTarget)], Iterable[RichAlignment])] = {

    // group reads by target
    val broadcastTargets = rich_rdd.context.broadcast(targets)
    val targetSize = targets.length
    info("Mapping reads to %d targets.".format(targetSize))

    // identify targets that are covered too highly and drop them
    val targetsToDrop = rich_rdd.flatMap(r => {
      Some(mapToTarget(r, broadcastTargets.value, 0, targetSize))
        .filter(_ >= 0)
        .map(v => (v, 1))
    }).reduceByKey(_ + _)
      .filter(p => p._2 >= maxReadsPerTarget)
      .collect

    val targetsToDropSet = targetsToDrop.map(_._1)
      .toSet

    info("Dropping %d targets whose coverage is too high:\n%s".format(targetsToDrop.length, targetsToDrop.mkString("\n")))

    val bcastTargetsToDrop = rich_rdd.context.broadcast(targetsToDropSet)
    val readsMappedToTarget = rich_rdd.groupBy((r: RichAlignment) => {
      mapToTarget(r,
        broadcastTargets.value,
        0,
        targetSize,
        targetsToDrop = bcastTargetsToDrop.value)
    }, ModPartitioner(rich_rdd.partitions.length))
      .map(kv => {
        val (k, v) = kv

        if (k < 0) {
          (None, v)
        } else {
          (Some((k, broadcastTargets.value(k))), v)
        }
      })

    readsMappedToTarget
  }

  /**
   * From a set of reads, returns the reference sequence that they overlap.
   */
  def getReferenceFromReads(reads: Iterable[RichAlignment]): (String, Long, Long) = {
    var tossedReads = 0

    // get reference and range from a single read
    val readRefs = reads.flatMap((r: RichAlignment) => {
      r.mdTag.fold {
        warn("Discarding read " + r.record.getReadName + " during reference re-creation.")
        tossedReads += 1
        (None: Option[(String, NumericRange[Long])])
      } { (tag) =>
        Some((tag.getReference(r), (r.getStart: Long) to r.getEnd))
      }
    })
      .toSeq
      .sortBy(_._2.head)

    // fold over sequences and append - sequence is sorted at start
    val ref = readRefs.reverse.foldRight[(String, Long)](
      ("", readRefs.head._2.head))(
        (refReads: (String, NumericRange[Long]), reference: (String, Long)) => {
          if (refReads._2.end < reference._2) {
            reference
          } else if (reference._2 >= refReads._2.head) {
            (reference._1 + refReads._1.substring((reference._2 - refReads._2.head).toInt),
              refReads._2.end)
          } else {
            // there is a gap in the sequence
            throw new IllegalArgumentException("Current sequence has a gap at " + reference._2 + "with " + refReads._2.head + "," + refReads._2.end +
              ". Discarded " + tossedReads + " in region when reconstructing region; reads may not have MD tag attached.")
          }
        })

    (ref._1, readRefs.head._2.head, ref._2)
  }
}

import org.bdgenomics.adam.ds.read.realignment.RealignIndels._

private[read] class RealignIndels(
    @transient val sc: SparkContext,
    val consensusGenerator: ConsensusGenerator = ConsensusGenerator.fromReads,
    val dataIsSorted: Boolean = false,
    val maxIndelSize: Int = 500,
    val maxConsensusNumber: Int = 30,
    val lodThreshold: Double = 5.0,
    val maxTargetSize: Int = 3000,
    val maxReadsPerTarget: Int = 20000,
    @transient val optReferenceFile: Option[ReferenceFile] = None,
    val unclipReads: Boolean = false) extends Serializable with Logging {

  private val optBcastReferenceFile = optReferenceFile.map(rf => {
    sc.broadcast(rf)
  })

  /**
   * Extracts the sequence and quality from the read. Drops soft clipped bases.
   *
   * @param read The read to extract the sequence and quality from.
   * @return Returns a tuple of (bases, Phred quality scores as Ints) where the
   *   soft clipped bases in the read have been dropped.
   */
  def extractSequenceAndQuality(
    read: RichAlignment): (String, Seq[Int]) = {

    // is this read clipped at the start or end? if so, how many bases?
    val richCigar = new RichCigar(read.samtoolsCigar)
    val startClipped = richCigar.softClippedBasesAtStart
    val endClipped = richCigar.softClippedBasesAtEnd

    if (unclipReads ||
      (startClipped == 0 &&
        endClipped == 0)) {
      (read.record.getSequence,
        read.qualityScoreValues)
    } else {
      (read.record.getSequence.drop(startClipped).dropRight(endClipped),
        read.qualityScoreValues.drop(startClipped).dropRight(endClipped))
    }
  }

  /**
   * Given a target group with an indel realignment target and a group of reads to realign, this method
   * generates read consensuses and realigns reads if a consensus leads to a sufficient improvement.
   *
   * @param targetGroup A tuple consisting of an indel realignment target and a seq of reads
   * @param partitionIdx The ID of the partition this is on. Only used for logging.
   * @return A sequence of reads which have either been realigned if there is a sufficiently good alternative
   * consensus, or not realigned if there is not a sufficiently good consensus.
   */
  def realignTargetGroup(targetGroup: (Option[(Int, IndelRealignmentTarget)], Iterable[RichAlignment]),
                         partitionIdx: Int = 0): Iterable[RichAlignment] = {
    val (target, reads) = targetGroup

    if (target.isEmpty) {
      // if the indel realignment target is empty, do not realign
      reads
    } else {
      try {
        val (targetIdx, _) = target.get
        val startTime = System.nanoTime()
        // bootstrap realigned read set with the reads that need to be realigned

        // get reference from reads
        val refStart = reads.map(_.getStart).min
        val refEnd = reads.map(_.getEnd).max
        val refRegion = ReferenceRegion(reads.head.record.getReferenceName, refStart, refEnd)
        val reference = optBcastReferenceFile.fold(getReferenceFromReads(reads.map(r => new RichAlignment(r)))._1)(brf => brf.value.extract(refRegion))

        // preprocess reads and get consensus
        val readsToClean = consensusGenerator.preprocessReadsForRealignment(
          reads,
          reference,
          refRegion
        ).zipWithIndex
        val observedConsensusSeq = consensusGenerator.findConsensus(reads)
          .toSeq
        val observedConsensus = observedConsensusSeq.distinct

        // reduce count of consensus sequences
        val consensus = if (observedConsensus.size > maxConsensusNumber) {
          // sort by the number of times that the consensus was seen
          // sorts ascending (least frequent comes first), so take from right
          observedConsensus.sortBy(c => {
            observedConsensusSeq.count(c == _)
          }).takeRight(maxConsensusNumber)
        } else {
          observedConsensus
        }

        val finalReads = if (reads.size > 0 && consensus.size > 0) {

          // do not check realigned reads - they must match
          val mismatchQualities = {
            readsToClean.map(r => sumMismatchQuality(r._1)).toArray
          }
          val totalMismatchSumPreCleaning = mismatchQualities.sum

          /* list to log the outcome of all consensus trials. stores:
           *  - mismatch quality of reads against new consensus sequence
           *  - the consensus sequence itself
           *  - a map containing each realigned read and it's offset into the new sequence
           */
          val consensusOutcomes = new Array[(Int, Consensus, Map[RichAlignment, Int])](consensus.size)

          // loop over all consensuses and evaluate
          consensus.zipWithIndex.foreach(p => {
            val (c, cIdx) = p

            // generate a reference sequence from the consensus
            val consensusSequence = c.insertIntoReference(reference, refRegion)

            // evaluate all reads against the new consensus
            val sweptValues = readsToClean.map(p => {
              val (r, rIdx) = p
              val originalQual = mismatchQualities(rIdx)
              val (sequence, quality) = extractSequenceAndQuality(r)

              val qualAndPos = sweepReadOverReferenceForQuality(sequence, consensusSequence, quality, originalQual)

              (r, qualAndPos)
            })

            // sum all mismatch qualities to get the total mismatch quality for this alignment
            val totalQuality = sweptValues.map(_._2._1).sum

            // package data
            val readMappings = sweptValues.map(kv => (kv._1, kv._2._2)).toMap

            // add to outcome list
            consensusOutcomes(cIdx) = (totalQuality, c, readMappings)
          })

          // perform reduction to pick the consensus with the lowest aggregated mismatch score
          val bestConsensusTuple = consensusOutcomes.minBy(_._1)

          val (bestConsensusMismatchSum, bestConsensus, bestMappings) = bestConsensusTuple

          // check for a sufficient improvement in mismatch quality versus threshold
          info("On " + refRegion + ", before realignment, sum was " + totalMismatchSumPreCleaning +
            ", best realignment is " + bestConsensus + " with " + bestConsensusMismatchSum)
          val lodImprovement = (totalMismatchSumPreCleaning - bestConsensusMismatchSum).toDouble / 10.0
          if (lodImprovement > lodThreshold) {
            var realignedReadCount = 0

            // generate a reference sequence from the consensus
            val consensusSequence = bestConsensus.insertIntoReference(reference, refRegion)

            // if we see a sufficient improvement, realign the reads
            val cleanedReads: Iterable[RichAlignment] = readsToClean.map(p => {
              val (r, rIdx) = p

              try {
                val finalRemapping = bestMappings(r)

                // if read alignment is improved by aligning against new consensus, realign
                if (finalRemapping != -1) {
                  realignedReadCount += 1
                  val builder: Alignment.Builder = Alignment.newBuilder(r)

                  // bump up mapping quality by 10
                  builder.setMappingQuality(r.getMappingQuality + 10)

                  // how many bases are clipped at the start/end of the read?
                  val (basesClippedAtStart, basesClippedAtEnd) = if (unclipReads) {
                    (0, 0)
                  } else {
                    val richCigar = new RichCigar(r.samtoolsCigar)
                    (richCigar.softClippedBasesAtStart, richCigar.softClippedBasesAtEnd)
                  }

                  // get start, end, cigar
                  val (newStart, newEnd, newCigar) = cigarAndCoordinates(
                    r.getSequence.length,
                    basesClippedAtStart, basesClippedAtEnd,
                    r.getBasesTrimmedFromStart, r.getBasesTrimmedFromEnd,
                    refStart,
                    finalRemapping,
                    bestConsensus)

                  if (newEnd <= newStart) {
                    warn("Realigning read %s failed because realignment issued an illegal alignment: start %d, end %d, CIGAR %s.".format(r, newStart, newEnd, newCigar))
                    r
                  } else {
                    builder.setStart(newStart)
                      .setEnd(newEnd)
                      .setCigar(newCigar.toString)

                    // update mdtag and cigar
                    val newMdTag = MdTag.moveAlignment(r,
                      newCigar,
                      reference.drop((newStart - refStart).toInt),
                      newStart).toString()
                    builder.setMismatchingPositions(newMdTag)
                    builder.setOriginalStart(r.getStart())
                    builder.setOriginalCigar(r.getCigar())
                    val rec = builder.build()

                    new RichAlignment(rec)
                  }
                } else {
                  r
                }
              } catch {
                case t: Throwable => {
                  warn("Realigning read %s failed with %s. At:".format(r, t))
                  r
                }
              }
            })

            info("On " + refRegion + ", realigned " + realignedReadCount + " reads to " +
              bestConsensus + " due to LOD improvement of " + lodImprovement)

            cleanedReads
          } else {
            info("On " + refRegion + ", skipping realignment due to insufficient LOD improvement (" +
              lodImprovement + "for consensus " + bestConsensus)
            reads
          }
        } else {
          reads
        }

        // return all reads that we cleaned and all reads that were initially realigned
        val endTime = System.nanoTime()
        info("TARGET|\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%d".format(partitionIdx,
          targetIdx,
          endTime - startTime,
          refRegion.referenceName,
          refRegion.start,
          refRegion.end,
          reads.size,
          observedConsensus.size,
          consensus.size))
        finalReads
      } catch {
        case t: Throwable => {
          warn("Realigning target %s failed with %s.".format(target, t))
          reads
        }
      }
    }
  }

  /**
   * Computes the cigar and start/end coordinates for a realigned read.
   *
   * @param readLength The number of bases in the read (excluding hard clipped
   *   bases, which should not show up in the read).
   * @param basesSoftClippedAtStart Number of bases aligned as soft clipped
   *   at the start of the read.
   * @param basesSoftClippedAtEnd Number of bases aligned as soft clipped
   *   at the end of the read.
   * @param basesHardClippedAtStart Number of bases aligned as hard clipped
   *   at the start of the read.
   * @param basesHardClippedAtEnd Number of bases aligned as hard clipped
   *   at the end of the read.
   * @param refStart Reference genome start position for this realignment
   *   target.
   * @param remappingIdx The location in the consensus sequence where this
   *   read has been realigned.
   * @param consensus The consensus variant to realign against.
   * @return Returns a tuple with the (read start, read end, cigar).
   */
  def cigarAndCoordinates(readLength: Int,
                          basesSoftClippedAtStart: Int,
                          basesSoftClippedAtEnd: Int,
                          basesHardClippedAtStart: Int,
                          basesHardClippedAtEnd: Int,
                          refStart: Long,
                          remappingIdx: Int,
                          consensus: Consensus): (Long, Long, Cigar) = {

    // where does the consensus variant start and end?
    val rawStartIdx = consensus.index.start - refStart
    val (consensusStartIdx, consensusEndIdx) = if (consensus.consensus.isEmpty) {
      (rawStartIdx - 1, rawStartIdx)
    } else {
      (rawStartIdx, rawStartIdx + consensus.consensus.size + 1)
    }

    // how many unclipped bases do we have?
    val alignedBases = readLength - (basesSoftClippedAtStart + basesSoftClippedAtEnd)

    def nonDeletedCigar: Cigar = {
      new Cigar(List(
        new CigarElement(basesHardClippedAtStart, CigarOperator.H),
        new CigarElement(basesSoftClippedAtStart, CigarOperator.S),
        new CigarElement(alignedBases, CigarOperator.M),
        new CigarElement(basesSoftClippedAtEnd, CigarOperator.S),
        new CigarElement(basesHardClippedAtEnd, CigarOperator.H))
        .filter(_.getLength > 0))
    }

    // if read is fully before or fully after the consensus, then we
    // have a full match
    if ((remappingIdx + alignedBases) <= consensusStartIdx) {
      // read fully before
      (refStart + remappingIdx,
        refStart + remappingIdx + alignedBases,
        nonDeletedCigar)
    } else if (remappingIdx > consensusEndIdx) {
      // read fully after
      val idxAfterEnd = if (consensus.consensus.isEmpty) {
        remappingIdx - consensusEndIdx - 1
      } else {
        remappingIdx - consensusEndIdx
      }
      (consensus.index.end + idxAfterEnd,
        consensus.index.end + idxAfterEnd + alignedBases,
        nonDeletedCigar)
    } else if (remappingIdx <= consensusStartIdx &&
      (remappingIdx + alignedBases) > consensusEndIdx) {
      // read fully spans the event

      // is the event an insert or a deletion?
      val (basesBefore, basesConsumed, eventOperator, endOffset) = if (consensus.consensus.isEmpty) {
        (consensusStartIdx - remappingIdx + 1,
          0,
          new CigarElement(consensus.index.length.toInt - 1, CigarOperator.D),
          -1)
      } else {
        (consensusStartIdx - remappingIdx + 1,
          consensus.consensus.length,
          new CigarElement(consensus.consensus.length, CigarOperator.I),
          0)
      }
      val basesAfter = alignedBases - (basesBefore + basesConsumed)

      (refStart + remappingIdx,
        consensus.index.end + basesAfter + endOffset,
        new Cigar(List(
          new CigarElement(basesHardClippedAtStart, CigarOperator.H),
          new CigarElement(basesSoftClippedAtStart, CigarOperator.S),
          new CigarElement(basesBefore.toInt, CigarOperator.M),
          eventOperator,
          new CigarElement(basesAfter.toInt, CigarOperator.M),
          new CigarElement(basesSoftClippedAtEnd, CigarOperator.S),
          new CigarElement(basesHardClippedAtEnd, CigarOperator.H))
          .filter(_.getLength > 0)))
    } else if (remappingIdx <= consensusStartIdx) {
      // read starts before variant and partially spans it
      // must be an insertion
      assert(consensus.consensus.nonEmpty)

      val basesBefore = consensusStartIdx - remappingIdx + 1
      val basesLeft = alignedBases - basesBefore
      (refStart + remappingIdx,
        consensus.index.end,
        new Cigar(List(
          new CigarElement(basesHardClippedAtStart, CigarOperator.H),
          new CigarElement(basesSoftClippedAtStart, CigarOperator.S),
          new CigarElement(basesBefore.toInt, CigarOperator.M),
          new CigarElement(basesSoftClippedAtEnd + basesLeft.toInt, CigarOperator.S),
          new CigarElement(basesHardClippedAtEnd, CigarOperator.H))
          .filter(_.getLength > 0)))
    } else {
      // read starts in variant and partially spans it
      // must be an insertion
      assert(consensus.consensus.nonEmpty)

      val basesIntoVariant = remappingIdx - consensusStartIdx - 1
      val basesInVariant = consensus.consensus.length - basesIntoVariant
      val basesAfter = alignedBases - basesInVariant

      (consensus.index.end,
        consensus.index.end + basesAfter,
        new Cigar(List(
          new CigarElement(basesHardClippedAtStart, CigarOperator.H),
          new CigarElement(basesSoftClippedAtStart + basesInVariant.toInt, CigarOperator.S),
          new CigarElement(basesAfter.toInt, CigarOperator.M),
          new CigarElement(basesSoftClippedAtEnd, CigarOperator.S),
          new CigarElement(basesHardClippedAtEnd, CigarOperator.H))
          .filter(_.getLength > 0)))
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
   * @param originalQuality The original score of the read vs. the reference genome.
   * @return Tuple of (mismatch quality score, alignment offset).
   */
  def sweepReadOverReferenceForQuality(read: String, reference: String, qualities: Seq[Int], originalQuality: Int): (Int, Int) = {

    @tailrec def sweep(i: Int,
                       upTo: Int,
                       minScore: Int,
                       minPos: Int): (Int, Int) = {
      if (i >= upTo) {
        (minScore, minPos)
      } else {
        val qualityScore = sumMismatchQualityIgnoreCigar(read, reference, qualities, minScore, i)
        val (newMinScore, newMinPos) = if (qualityScore <= minScore) {
          (qualityScore, i)
        } else {
          (minScore, minPos)
        }
        sweep(i + 1, upTo, newMinScore, newMinPos)
      }
    }

    // calculate mismatch quality score for all admissable alignment offsets
    sweep(0, reference.length - read.length + 1, originalQuality, -1)
  }

  /**
   * Sums the mismatch quality of a read against a reference. Mismatch quality is defined as the sum
   * of the base quality for all bases in the read that do not match the reference. This method
   * ignores the cigar string, which treats indels as causing mismatches.
   *
   * @param read Read to evaluate.
   * @param reference Reference sequence to look for mismatches against.
   * @param qualities Sequence of base quality scores.
   * @param scoreThreshold Stops summing if score is greater than this value.
   * @param refOffset Offset into the reference sequence.
   * @return Mismatch quality sum.
   */
  def sumMismatchQualityIgnoreCigar(read: String,
                                    reference: String,
                                    qualities: Seq[Int],
                                    scoreThreshold: Int,
                                    refOffset: Int): Int = {

    @tailrec def loopAndSum(idx: Int = 0,
                            runningSum: Int = 0): Int = {
      if (idx >= read.length || (idx + refOffset) >= reference.length) {
        runningSum
      } else if (runningSum > scoreThreshold) {
        Int.MaxValue
      } else {
        val newRunningSum = if (read(idx) == reference(idx + refOffset) ||
          reference(idx + refOffset) == '_') {
          runningSum
        } else {
          runningSum + qualities(idx)
        }
        loopAndSum(idx + 1, newRunningSum)
      }
    }

    loopAndSum()
  }

  /**
   * Given a read, sums the mismatch quality against it's current alignment position.
   * Does NOT ignore cigar.
   *
   * @param read Read over which to sum mismatch quality.
   * @return Mismatch quality of read for current alignment.
   */
  def sumMismatchQuality(read: Alignment): Int = {
    sumMismatchQualityIgnoreCigar(
      read.getSequence,
      read.mdTag.get.getReference(read, withGaps = true),
      read.qualityScoreValues,
      Int.MaxValue,
      0
    )
  }

  /**
   * Performs realignment for an RDD of reads. This includes target generation, read/target
   * classification, and read realignment.
   *
   * @param rdd Reads to realign.
   * @return Realigned read.
   */
  def realignIndels(rdd: RDD[Alignment]): RDD[Alignment] = {
    val sortedRdd = if (dataIsSorted) {
      rdd.filter(r => r.getReadMapped)
    } else {
      val sr = rdd.filter(r => r.getReadMapped)
        .keyBy(ReferencePosition(_))
        .sortByKey()
      sr.map(kv => kv._2)
    }

    // we only want to convert once so let's get it over with
    val richRdd = sortedRdd.map(new RichAlignment(_))
    richRdd.cache()

    // find realignment targets
    info("Generating realignment targets...")
    val targets: Array[IndelRealignmentTarget] = RealignmentTargetFinder(
      richRdd,
      consensusGenerator,
      maxIndelSize,
      maxTargetSize
    ).toArray

    // we should only attempt realignment if the target set isn't empty
    if (targets.isEmpty) {
      val readRdd = richRdd.map(r => r.record)
      richRdd.unpersist()
      readRdd
    } else {
      // map reads to targets
      info("Grouping reads by target...")
      val readsMappedToTarget = RealignIndels.mapTargets(richRdd,
        targets,
        maxReadsPerTarget = maxReadsPerTarget)
      richRdd.unpersist()

      // realign target groups
      info("Sorting reads by reference in ADAM RDD")
      readsMappedToTarget.mapPartitionsWithIndex((idx, iter) => {
        iter.flatMap(realignTargetGroup(_, idx))
      }).map(r => r.record)
    }
  }
}
