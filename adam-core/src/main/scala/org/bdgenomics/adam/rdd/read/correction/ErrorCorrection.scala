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
package org.bdgenomics.adam.rdd.read.correction

import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.algorithms.distributions.{
  CategoricalDistribution,
  GammaMixtureModel,
  GammaDistribution
}
import org.bdgenomics.adam.algorithms.prefixtrie.DNAPrefixTrie
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.ProbabilisticSequence
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro.AlignmentRecord
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import scala.collection.immutable.StringOps
import scala.math.{ abs, exp, log => mathLog, max, min, Pi, pow, sqrt }

private[rdd] object ErrorCorrection extends Logging {

  val ec = new ErrorCorrection

  /**
   * Cuts reads into _q_-mers, and then finds the _q_-mer weight. Q-mers are described in:
   *
   * Kelley, David R., Michael C. Schatz, and Steven L. Salzberg. "Quake: quality-aware detection
   * and correction of sequencing errors." Genome Biol 11.11 (2010): R116.
   *
   * _q_-mers are _k_-mers weighted by the quality score of the bases in the _k_-mer.
   *
   * @param rdd RDD to count q-mers on.
   * @param qmerLength The value of _q_ to use for cutting _q_-mers.
   * @return Returns an RDD containing q-mer/weight pairs.
   */
  def countQmers(rdd: RDD[AlignmentRecord],
                 qmerLength: Int): RDD[(String, Double)] = {
    // generate qmer counts
    rdd.flatMap(ec.readToQmers(_, qmerLength))
      .reduceByKey(_ + _)
  }

  /**
   * For an RDD of read data, performs read error correction.
   *
   * @param rdd The RDD of reads to correct.
   * @param qmerLength The length of the q-mers to create. The default value is 20.
   * @return Returns a corrected RDD of reads.
   */
  def apply(rdd: RDD[AlignmentRecord],
            qmerLength: Int = 20,
            maxIterations: Int = 10,
            fixingThreshold: Int = 20,
            emThreshold: Double = mathLog(0.5),
            missingKmerProbability: Double = 0.05,
            ploidy: Int = 2): RDD[AlignmentRecord] = {

    // trim N's
    val trimmedReads = TrimReads.trimNs(rdd)
      .cache()

    // generate qmer counts
    val qmerCounts: RDD[(String, Double)] = trimmedReads.flatMap(ec.readToQmers(_, qmerLength))
      .reduceByKey(_ + _)
      .cache()

    val counts = qmerCounts.map(kv => kv._2)
      .cache()

    // run em to fit distributions
    val distributions = GammaMixtureModel.fit(counts,
      ploidy + 1,
      maxIterations,
      maxIterations,
      Some(emThreshold))
    val errorDistribution = distributions.head
    val trustedDistributions = distributions.drop(1)

    // determine if kmers are trusted
    val qmerLikelihoods: RDD[(String, Double, Double)] = qmerCounts.map(p => {
      val (qmer, count) = p

      // apply distributions
      (qmer,
        errorDistribution.probabilityDensity(count),
        trustedDistributions.map(_.probabilityDensity(count)).sum)
    })

    // unpersist counts
    qmerCounts.unpersist()

    // filter and collect trusted kmers
    val trustedKmers = qmerLikelihoods.filter(t => t._2 <= t._3 &&
      // normally, i wouldn't create a "new StringOps", but there is a collision between implicits
      new StringOps(t._1).find(c => c == 'n' || c == 'N').isEmpty)
      .map(t => (t._1, t._3 / (t._2 + t._3)))
      .collect

    // build prefix trie out of trusted kmers
    val kmerTrie = DNAPrefixTrie(trustedKmers.toMap)

    // fix reads
    val fixedReads = fixReads(trimmedReads,
      kmerTrie,
      qmerLength,
      PhredUtils.successProbabilityToPhred(fixingThreshold),
      missingKmerProbability)

    // unpersist original trimmed reads and return
    trimmedReads.unpersist()

    fixedReads
  }

  /**
   * Measures base transition probabilities. Each record contains the relative likelihoods of
   * an A/C/G/T at each position. We collect a map which contains the prior probablities of
   * a base read with a given phred score at a given index transitioning to another base.
   *
   * @param rdd An RDD containing tuples of probabilistic sequences and sequence records.
   * @return Returns a map of error covariates to transition probabilities.
   */
  private[correction] def measureTransitions(rdd: RDD[(ProbabilisticSequence, AlignmentRecord)]): Map[ErrorCovariate, CategoricalDistribution] = {

    // cut read into covariate slices
    val covariates = rdd.flatMap(rp => {
      val (seq, read) = rp

      // loop over and emit
      (0 until seq.sequence.length).map(i => {
        (ErrorCovariate(read.getSequence.charAt(i), i, read.getQual.charAt(i)), seq.sequence(i))
      })
    })

    def aggregateArray(a1: Array[Double], a2: Array[Double]): Array[Double] = {
      (0 until a1.length).foreach(i => {
        a1(i) += a2(i)
      })
      a1
    }

    // compute MLE
    covariates.aggregateByKey(Array(0.0, 0.0, 0.0, 0.0))(aggregateArray, aggregateArray)
      .map(kv => (kv._1, CategoricalDistribution.softmax(kv._2)))
      .collect()
      .toMap
  }

  /**
   * Uses a probabilistic model for base transitions to fix errors in reads.
   *
   * @param rdd An RDD of reads to correct.
   * @param kmerTrie A prefix trie populated with the probability of a kmer being
   *                 a "correct" kmer.
   * @param qmerLength The length of k to use when cutting q-mers.
   * @param fixingThreshold The minimum probability to require for allowing a base to be fixed.
   * @param missingProbability The assumed (upper bound) probability for a k-mer that is not
   *                           resident in the prefix trie.
   * @return Returns an RDD of fixed reads.
   */
  private[correction] def fixReads(rdd: RDD[AlignmentRecord],
                                   kmerTrie: DNAPrefixTrie[Double],
                                   qmerLength: Int,
                                   fixingThreshold: Double,
                                   missingProbability: Double): RDD[AlignmentRecord] = {
    // cut reads into k-mers and get probabilities
    val cutReads = rdd.map(ec.cutRead(_, kmerTrie, qmerLength, missingProbability))
      .cache()

    // measure the transition probabilities
    val transitionDistributions = measureTransitions(cutReads)

    // correct the reads
    val fixPhred = (PhredUtils.successProbabilityToPhred(fixingThreshold) + 33).toChar
    val correctedReads = cutReads.map(ec.correctRead(_,
      fixingThreshold,
      fixPhred,
      transitionDistributions,
      kmerTrie,
      qmerLength,
      missingProbability))

    // unpersist cut reads
    cutReads.unpersist()

    correctedReads
  }
}

/**
 * This case class is used as a key for tracking transition probabilities of bases.
 */
private[correction] case class ErrorCovariate(base: Char, cycle: Int, phred: Char) {
}

private[correction] class ErrorCorrection extends Serializable with Logging {

  /**
   * Cuts a single read into q-mers.
   *
   * @param read Read to cut.
   * @param qmerLength The length of the qmer to cut.
   * @return Returns an iterator containing q-mer/weight mappings.
   */
  def readToQmers(read: AlignmentRecord,
                  qmerLength: Int = 20): Iterator[(String, Double)] = {
    // get read bases and quality scores
    val bases = read.getSequence.toSeq
    val scores = read.getQual.toString.toCharArray.map(q => {
      PhredUtils.phredToSuccessProbability(q.toInt - 33)
    })

    // zip and put into sliding windows to get qmers
    bases.zip(scores)
      .sliding(qmerLength)
      .map(w => {
        // bases are first in tuple
        val b = w.map(_._1)

        // quals are second
        val q = w.map(_._2)

        // reduce bases into string, reduce quality scores
        (b.map(_.toString).reduce(_ + _), q.reduce(_ * _))
      })
  }

  /**
   * Cuts a read into q-mers and then generates base likelihoods.
   *
   * @param read Read to cut.
   * @param trie A trie containing the probability that a q-mer is "true".
   * @param kmerLength The length of k to use when cutting q-mers.
   * @param missingKmerProbability The upper bound probability to assign to a
   *                               k-mer not found in the trie.
   * @return Returns a probabilistic sequence and the original read.
   */
  def cutRead(read: AlignmentRecord,
              trie: DNAPrefixTrie[Double],
              kmerLength: Int,
              missingKmerProbability: Double): (ProbabilisticSequence, AlignmentRecord) = {

    // cut sequence into k-mers
    val readSequence = read.getSequence.toString

    // call to cut string
    (cutString(readSequence, trie, kmerLength, missingKmerProbability), read)
  }

  /**
   * Cuts a string into q-mers and then generates base likelihoods. Helper function.
   *
   * @see cutRead
   * @see correctRead
   *
   * @param readSequence String to cut.
   * @param trie A trie containing the probability that a q-mer is "true".
   * @param kmerLength The length of k to use when cutting q-mers.
   * @param missingKmerProbability The upper bound probability to assign to a
   *                               k-mer not found in the trie.
   * @return Returns a probabilistic sequence and the original read.
   */
  private[correction] def cutString(readSequence: String,
                                    trie: DNAPrefixTrie[Double],
                                    kmerLength: Int,
                                    missingKmerProbability: Double,
                                    start: Int = 0,
                                    end: Int = Int.MaxValue): ProbabilisticSequence = {

    // cut sequence into kmers
    val kmers = readSequence.sliding(kmerLength).toArray

    // loop over sequence and get probabilities
    val readLength = readSequence.length
    val kmersLength = kmers.length
    val readProbabilities = new Array[Array[Double]](readLength)

    (0 until readLength).foreach(i => {
      readProbabilities(i) = Array(1.0, 1.0, 1.0, 1.0)

      // we only do a probability update if requested
      if (i >= start && i <= end) {
        val startIdx = if (i == 0) {
          0
        } else {
          max(i - kmerLength + 1, 0)
        }
        val endIdx = min(i, kmersLength - 1)

        (startIdx to endIdx).foreach(j => {
          val kmer = kmers(j)
          val kIdx = i - j
          (0 to 3).foreach(b => {
            val testKmer = kmer.take(kIdx) + intToBase(b) + kmer.drop(kIdx + 1)
            readProbabilities(i)(b) *= trie.getOrElse(testKmer, missingKmerProbability)
          })
        })
      }
    })

    // build probabilistic sequence and return
    ProbabilisticSequence(readProbabilities)
  }

  def intToBase(i: Int): String = i match {
    case 0 => "A"
    case 1 => "C"
    case 2 => "G"
    case _ => "T"
  }

  def baseToInt(c: Char): Int = c match {
    case 'A' | 'a' => 0
    case 'C' | 'c' => 1
    case 'G' | 'g' => 2
    case 'T' | 't' => 3
    case _         => 4
  }

  /**
   * Performs the read correction step, after transition probabilities have been
   * estimated. This is done via a coordinate descent process, where the read is
   * considered to have a different coordinate at each position. We continue as
   * long as the probability of the read is increasing, and only touch each base
   * once.
   *
   * @param read Tuple of probabilistic sequence and original read.
   * @param fixingThreshold The threshold to use for correcting a read.
   * @param fixingThresholdAsPhred The phred score corresponding to the fixing threshold.
   * @param compensation The covariate prior probability table.
   * @param kmerTrie Trie containing k-mer "trueness" probabilities.
   * @param kmerLength Length k to use when cutting k-mers.
   * @param missingKmerProbability The upper bound probability to assign to any k-mer
   *                               not in the trie.
   * @return Returns a fixed read.
   */
  private[correction] def correctRead(read: (ProbabilisticSequence, AlignmentRecord),
                                      fixingThreshold: Double,
                                      fixingThresholdAsPhred: Char,
                                      compensation: Map[ErrorCovariate, CategoricalDistribution],
                                      kmerTrie: DNAPrefixTrie[Double],
                                      kmerLength: Int,
                                      missingKmerProbability: Double): AlignmentRecord = {

    @tailrec def tryFix(checkablePositions: Array[((Array[Double], Char), Int)],
                        pSeq: ProbabilisticSequence,
                        seq: String,
                        pRead: Double,
                        substitutedPositions: Set[Int]): ProbabilisticSequence = {
      // if there are no further positions we can check, then we are done
      if (checkablePositions.isEmpty) {
        pSeq
      } else {
        // check the position with the highest probability of a change
        val toCheck = checkablePositions.minBy(vk => {
          val ((probabilities, originalBase), _) = vk
          probabilities(baseToInt(originalBase))
        })

        // unpack this position
        val ((probabilities, originalBase), position) = toCheck

        // we want to try the highest probability base
        val maxIdx = probabilities.indexOf(probabilities.max)

        // if this base is greater than the substitution probability,
        // we try the change
        if (probabilities(maxIdx) < fixingThreshold) {
          // if our current top probability doesn't meet the fixing threshold, we can return
          pSeq
        } else {
          // build the new sequence
          val newBase = intToBase(maxIdx)
          val testSequence = seq.take(position) + newBase + seq.drop(position + 1)

          // chop up the new read
          val testPSeq = cutString(testSequence, kmerTrie, kmerLength, missingKmerProbability, position - kmerLength, position + kmerLength)

          // use the last probabilistic sequence as a prior for the new probablistic sequence
          // then apply a softmax in place
          (0 until testPSeq.sequence.length).foreach(i => {
            (0 to 3).foreach(j => {
              testPSeq.sequence(i)(j) *= pSeq.sequence(i)(j)
            })
          })
          testPSeq.softMax()

          // is this an improvement?
          val testPRead = readProbability(testPSeq, testSequence)
          val (newCPos, newPSeq, newSeq, newPRead, newSub) = if (testPRead > pRead) {
            // recompute the positions to check
            val newPositionsToCheck = checkablePositions.flatMap(vk => {
              val ((_, base), idx) = vk

              // if this site is the site we just evaluated, we can filter it out
              if (idx == position) {
                None
              } else {
                // collect probabilities for this position from the current probabilistic read
                val newProbabilities = testPSeq.sequence(idx)

                // what is the max probability at this site?
                val maxProbability = newProbabilities.max

                // if the current base is the max probability base, we can filter,
                // else return updated probabilities
                if (maxProbability == newProbabilities(baseToInt(base))) {
                  None
                } else {
                  Some(((newProbabilities, base), idx))
                }
              }
            })

            // emit new artifacts
            (newPositionsToCheck, testPSeq, testSequence, testPRead, substitutedPositions + maxIdx)
          } else {
            // if we don't have an improvement in probability, we don't make a
            // change, just filter out the position we just checked
            (checkablePositions.filter(vk => vk._2 != position), pSeq, seq, pRead, substitutedPositions)
          }

          // recurse, and try the next fix candidate
          tryFix(newCPos, newPSeq, newSeq, newPRead, newSub)
        }
      }
    }

    def readProbability(pSeq: ProbabilisticSequence,
                        read: String): Double = {
      pSeq.sequence
        .zip(read)
        .map(p => {
          val (pArray, base) = p
          pArray(baseToInt(base))
        }).reduce(_ * _)
    }

    // unpack probabilities and read sequence
    val (pSeq, oldRead) = read
    val sequence = oldRead.getSequence.toString
    val phred = oldRead.getQual.toString

    // compensate all read bases by their transition probabilities
    val compensatedQualities = pSeq.sequence
      .zip(sequence.zip(phred))
      .zipWithIndex
      .map(vk => {
        val ((probabilities, (base, qual)), idx) = vk

        // look up prior for this base in the compensation table
        val priors = compensation(ErrorCovariate(base, idx, qual))

        // compensate by transition probabilities
        (0 to 3).foreach(i => probabilities(i) *= priors.probability(i))

        // find factor to normalize by
        val normFactor = 1.0 / probabilities.sum

        // normalize all possibilities
        (0 to 3).foreach(i => probabilities(i) *= normFactor)

        // return tuple of ((probabilities, base), idx)
        ((probabilities, base), idx)
      })

    // create probabilistic sequence by mapping down to probabilities
    val startPSeq = ProbabilisticSequence(compensatedQualities.map(v => v._1._1).toArray)
    val pRead = readProbability(startPSeq, sequence)

    // filter out bases that are already over the fixing threshold
    val positions = compensatedQualities.filter(vk => {
      val ((probabilities, base), _) = vk

      probabilities(baseToInt(base)) < fixingThreshold
    })

    // try to fix the read
    val newPSeq = tryFix(positions,
      startPSeq,
      sequence,
      pRead,
      Set[Int]())
    val (newSequence, newQuals) = newPSeq.toSequence

    // finalize the fix and return
    finalizeFix(oldRead, newSequence, newQuals, fixingThresholdAsPhred)
  }

  /**
   * Finalizes a fixed read by trimming low quality bases off of the end of
   * the read, and converting quals to phred.
   *
   * @param oldRead Initial read with all structural information.
   * @param newSequence The new, error corrected sequence.
   * @param newQuals The integer phred quality of all bases in the read.
   * @param fixPhred The phred score limit for accepting a base.
   * @return Returns a fixed read.
   */
  def finalizeFix(oldRead: AlignmentRecord,
                  newSequence: String,
                  newQuals: Array[Int],
                  fixPhred: Char): AlignmentRecord = {

    val phredAsInt = fixPhred.toInt - 33

    def dropLength(array: Array[Int],
                   reverse: Boolean): Int = {
      val (startIdx, increment) = if (reverse) {
        (array.length - 1, -1)
      } else {
        (0, 1)
      }

      @tailrec def dropTest(idx: Int,
                            dropCount: Int): Int = {
        if (array(idx) >= phredAsInt) {
          dropCount
        } else {
          dropTest(idx + increment, dropCount + 1)
        }
      }

      dropTest(startIdx, 0)
    }

    // trim ends off of read, if they are below the fixing threshold
    val trimStart = dropLength(newQuals, false)
    val trimEnd = dropLength(newQuals, true)

    // rebuild read with trimmed ends
    AlignmentRecord.newBuilder(oldRead)
      .setSequence(newSequence.drop(trimStart).dropRight(trimEnd))
      .setQual(newQuals.drop(trimStart).dropRight(trimEnd).mkString)
      .setBasesTrimmedFromStart(trimStart)
      .setBasesTrimmedFromEnd(trimEnd)
      .build()
  }
}
