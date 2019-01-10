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
package org.bdgenomics.adam.rdd.read.recalibration

import htsjdk.samtools.{
  CigarElement,
  CigarOperator,
  TextCigarCodec,
  ValidationStringency
}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.{
  MdTag,
  ReadGroupDictionary,
  ReferenceRegion,
  SnpTable
}
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.misc.Logging
import scala.annotation.tailrec

/**
 * The algorithm proceeds in two phases. First, we make a pass over the reads
 * to collect statistics and build the recalibration tables. Then, we perform
 * a second pass over the reads to apply the recalibration and assign adjusted
 * quality scores.
 *
 * @param input The reads to recalibrate.
 * @param knownSnps A broadcast variable containing the locations of any known
 *   SNPs to ignore during recalibration table generation.
 * @param readGroups The read groups that the reads in this dataset are from.
 * @param minAcceptableAsciiPhred The minimum acceptable Phred score to attempt
 *   recalibrating, expressed as an ASCII character with Illumina (33) encodings.
 *   Defaults to '&' (Phred 5).
 * @param optStorageLevel An optional storage level to apply if caching the
 *   output of the first stage of BQSR.
 * @param optSamplingFraction An optional fraction of reads to sample when
 *   generating the covariate table.
 * @param optSamplingSeed An optional seed to provide if downsampling reads.
 */
private class BaseQualityRecalibration(
  val input: RDD[AlignmentRecord],
  val knownSnps: Broadcast[SnpTable],
  val readGroups: ReadGroupDictionary,
  val minAcceptableAsciiPhred: Char = (5 + 33).toChar,
  val optStorageLevel: Option[StorageLevel] = None,
  val optSamplingFraction: Option[Double] = None,
  val optSamplingSeed: Option[Long] = None)
    extends Serializable with Logging {

  /**
   * An RDD representing the input reads along with any error covariates that
   * the bases in the read map to.
   */
  val dataset: RDD[(AlignmentRecord, Array[CovariateKey])] = {
    val covRdd = input.map(read => {
      val covariates = if (BaseQualityRecalibration.shouldIncludeRead(read)) {
        BaseQualityRecalibration.observe(read, knownSnps, readGroups)
      } else {
        Array.empty[CovariateKey]
      }
      (read, covariates)
    })

    optStorageLevel.fold(covRdd)(sl => {
      log.info("User requested %s persistance for covariate RDD.".format(sl))
      covRdd.persist(sl)
    })
  }

  /**
   * A table containing the error frequency observations for the given reads.
   */
  val observed: ObservationTable = {
    val maybeSampledDataset = optSamplingFraction.fold(dataset)(samplingFraction => {
      optSamplingSeed.fold(dataset.sample(false, samplingFraction))(samplingSeed => {
        dataset.sample(false, samplingFraction, seed = samplingSeed)
      })
    })

    val observations = maybeSampledDataset.flatMap(p => p._2).flatMap(cov => {
      if (cov.shouldInclude) {
        Some((cov.toDefault, Observation(cov.isMismatch)))
      } else {
        None
      }
    }).reduceByKeyLocally(_ + _)
    new ObservationTable(observations)
  }

  /**
   * The input reads but with the quality scores of the reads replaced with
   * the quality scores given by the observation table.
   */
  val result: RDD[AlignmentRecord] = {
    val recalibrator = Recalibrator(observed, minAcceptableAsciiPhred)
    dataset.map(p => recalibrator(p._1, p._2))
  }
}

private[read] object BaseQualityRecalibration {

  /**
   * @param read The read to check to see if it is a proper quality read.
   * @return We consider a read valid for use in generating the recalibration
   *   table if it is a "proper" read. That is to say, it is the canonical read
   *   (mapped as a primary alignment with non-zero mapQ and not marked as a
   *   duplicate), it did not fail vendor checks, and the quality and CIGAR
   *   are defined.
   */
  private def shouldIncludeRead(read: AlignmentRecord) = {
    (read.getReadMapped && read.getPrimaryAlignment && !read.getDuplicateRead) &&
      read.getQuality != null &&
      (read.getMappingQuality != null && read.getMappingQuality > 0) &&
      (read.getCigar != null && read.getCigar != "*") &&
      !read.getFailedVendorQualityChecks
  }

  /**
   * @param read The read to generate residue inclusion/mismatch flags for.
   * @param maskedSites The set of positions overlapped by this read that
   *   contain known SNPs. This is the product of a join-like operation.
   * @return Returns two arrays that have the length of the read. The first
   *   array contains true at an index if the base at this index should be
   *   included in recalibration (non-zero quality, not in INDEL, not an N,
   *   not masked) and the second array contains true if the base is not a
   *   mismatch.
   */
  private def computeResiduesToInclude(read: AlignmentRecord,
                                       maskedSites: Set[Long]): (Array[Boolean], Array[Boolean]) = {
    val readSequence = read.getSequence.toArray
    val readQualities = read.getQuality.toArray
    val shouldInclude = new Array[Boolean](readSequence.length)
    val isMismatch = new Array[Boolean](readSequence.length)
    val readCigar = TextCigarCodec.decode(read.getCigar)
    val readCigarIterator = readCigar.iterator
    val firstCigarElement = readCigarIterator.next

    @tailrec def shouldIncludeResidue(shouldInclude: Array[Boolean],
                                      isMismatch: Array[Boolean],
                                      currentPos: Long,
                                      readSequence: Array[Char],
                                      readQualities: Array[Char],
                                      readCigar: java.util.Iterator[CigarElement],
                                      currentCigarElem: CigarElement,
                                      optMd: Option[MdTag],
                                      maskedSites: Set[Long],
                                      cigarIdx: Int,
                                      baseIdx: Int) {
      if (baseIdx < readSequence.length) {
        val currentCigarOp = currentCigarElem.getOperator
        val (newPos,
          newCigarElem,
          newCigarIdx,
          newBaseIdx) = if (currentCigarOp == CigarOperator.H) {
          // we must either be at the start or the end of the read
          // if we are at the end, we will have exited the loop already
          // so just check to make sure we're at base 0
          assert(baseIdx == 0)
          (currentPos, readCigar.next, 0, 0)
        } else if (currentCigarOp == CigarOperator.N ||
          currentCigarOp == CigarOperator.D) {
          (currentPos + currentCigarElem.getLength,
            readCigar.next,
            0,
            baseIdx)
        } else if (currentCigarOp == CigarOperator.I ||
          currentCigarOp == CigarOperator.S) {

          // soft clip at start needs "unclipped start pos"
          val pos = if (currentCigarOp == CigarOperator.S &&
            baseIdx == 0) {
            currentPos - currentCigarElem.getLength
          } else {
            currentPos
          }

          // set all the overlaping bases to be ignored
          val opLength = currentCigarElem.getLength
          var localIdx = baseIdx
          val baseIdxEnd = baseIdx + opLength - 1
          while (localIdx < baseIdxEnd) {
            shouldInclude(localIdx) = false
            isMismatch(localIdx) = false
            localIdx += 1
          }

          // if we have an insertion, we have already advanced
          // the start position in the match preceding the insertion
          // if we have a soft clip, we have no need to advance the
          // start position, since a soft clip implies that this base
          // is not part of the local alignment
          if (readCigar.hasNext) {
            (currentPos, readCigar.next, 0, baseIdxEnd + 1)
          } else {
            // we cannot pop off the next cigar element when we are at the
            // end of the read, so fake it
            assert(baseIdxEnd >= readSequence.length - 1, baseIdxEnd)
            (currentPos, currentCigarElem, 0, baseIdxEnd + 1)
          }
        } else {
          shouldInclude(baseIdx) = ((readSequence(baseIdx) != 'N') &&
            !maskedSites(currentPos) &&
            readQualities(baseIdx) > '!')
          isMismatch(baseIdx) = currentCigarOp match {
            case CigarOperator.X => true
            case CigarOperator.M => {
              // TODO: allow user to broadcast a ReferenceFile
              require(optMd.isDefined,
                "Cigar M was seen for read with undefined MD tag.")
              !optMd.get.isMatch(currentPos)
            }
            case _ => {
              // we fall through to this case IFF we see a Cigar =
              // however, the compiler can't check this since we
              // if/else out of the non-X/M cases above (DHSIN, disregard P)
              // so, we treat it as a case _
              false
            }
          }

          // are we at the end of this cigar block? if so, grab a new operator
          val (nextCigarElem,
            nextCigarIdx) = if (cigarIdx == currentCigarElem.getLength - 1) {
            if (readCigar.hasNext) {
              (readCigar.next, 0)
            } else {
              // we cannot pop off the next cigar element when we are at the
              // end of the read, so fake it
              assert(baseIdx >= readSequence.length - 1, baseIdx.toString)
              (currentCigarElem, cigarIdx + 1)
            }
          } else {
            (currentCigarElem, cigarIdx + 1)
          }

          (currentPos + 1L,
            nextCigarElem,
            nextCigarIdx,
            baseIdx + 1)
        }

        // call recursively
        shouldIncludeResidue(shouldInclude,
          isMismatch,
          newPos,
          readSequence,
          readQualities,
          readCigar,
          newCigarElem,
          optMd,
          maskedSites,
          newCigarIdx,
          newBaseIdx)
      }
    }

    shouldIncludeResidue(shouldInclude, isMismatch,
      read.getStart,
      readSequence,
      readQualities,
      readCigarIterator,
      firstCigarElement,
      Option(read.getMismatchingPositions)
        .map(MdTag(_, read.getStart, readCigar)),
      maskedSites,
      0, 0)
    (shouldInclude, isMismatch)
  }

  /**
   * Observes the error covariates contained in a read.
   *
   * @param read The read to observe.
   * @param readGroups A read group dictionary containing the read group
   *   this read is from.
   * @param maskedSites The known SNP loci that this read covers.
   * @return Returns an array of CovariateKeys that describe the per-base
   *   error covariates seen in this read.
   *
   * @note This method was split out from the other observe method to make it
   *   simpler to write unit tests for this package. Similarly, that is why
   *   this method is package private, while the other is private.
   */
  private[recalibration] def observe(
    read: AlignmentRecord,
    readGroups: ReadGroupDictionary,
    maskedSites: Set[Long] = Set.empty): Array[CovariateKey] = ObservingRead.time {
    val (toInclude, isMismatch) = ReadResidues.time {
      computeResiduesToInclude(read, maskedSites)
    }
    ReadCovariates.time {
      CovariateSpace(read,
        toInclude,
        isMismatch,
        readGroups)
    }
  }

  /**
   * Observes the error covariates contained in a read.
   *
   * @param read The read to observe.
   * @param knownSnps A broadcast variable containing all known SNP loci to mask.
   * @param readGroups A read group dictionary containing the read group
   *   this read is from.
   * @return Returns an array of CovariateKeys that describe the per-base
   *   error covariates seen in this read.
   */
  private def observe(
    read: AlignmentRecord,
    knownSnps: Broadcast[SnpTable],
    readGroups: ReadGroupDictionary): Array[CovariateKey] = ObservingRead.time {
    val maskedSites = knownSnps.value
      .maskedSites(ReferenceRegion.unstranded(read))
    observe(read, readGroups, maskedSites)
  }

  /**
   * Runs base quality score recalibration.
   *
   * @param rdd The reads to recalibrate.
   * @param knownSnps A broadcast table of known variation to mask during the
   *   recalibration process.
   * @param readGroups The read groups that generated these reads.
   * @param minAcceptableQuality The minimum quality score to attempt to
   *   recalibrate.
   * @param optStorageLevel An optional storage level to apply if caching the
   *   output of the first stage of BQSR
   * @param optSamplingFraction An optional fraction of reads to sample when
   *   generating the covariate table.
   * @param optSamplingSeed An optional seed to provide if downsampling reads.
   */
  def apply(
    rdd: RDD[AlignmentRecord],
    knownSnps: Broadcast[SnpTable],
    readGroups: ReadGroupDictionary,
    minAcceptableQuality: Int,
    optStorageLevel: Option[StorageLevel],
    optSamplingFraction: Option[Double] = None,
    optSamplingSeed: Option[Long] = None): RDD[AlignmentRecord] = {
    require(minAcceptableQuality >= 0 && minAcceptableQuality < 93,
      "MinAcceptableQuality (%d) must be positive and less than 93.")
    optSamplingFraction.foreach(samplingFraction => {
      require(samplingFraction > 0.0 && samplingFraction <= 1.0,
        "Sampling fraction (%d) must be greater than 0.0 and less than or equal to 1.0.")
    })
    new BaseQualityRecalibration(rdd,
      knownSnps,
      readGroups,
      (minAcceptableQuality + 33).toChar,
      optStorageLevel,
      optSamplingFraction,
      optSamplingSeed).result
  }
}
