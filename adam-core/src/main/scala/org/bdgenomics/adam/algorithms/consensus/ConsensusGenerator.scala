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
package org.bdgenomics.adam.algorithms.consensus

import htsjdk.samtools.{ Cigar, CigarOperator }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget
import org.bdgenomics.adam.rdd.variant.VariantDataset
import org.bdgenomics.adam.rich.RichAlignmentRecord
import scala.collection.JavaConversions._

/**
 * Singleton object for creating consensus generators.
 *
 * Consensus generators are used in INDEL realignment to generate the consensus
 * sequences that reads are realigned against. We have three consensus modes:
 *
 * * From reads: This mode looks at the read alignments for evidence of INDELs.
 *   Any INDEL that shows up in a read alignment is extracted and used to
 *   generate a consensus sequence.
 * * From reads with Smith-Waterman: This mode discards the original read
 *   alignments and uses Smith-Waterman to locally realign the read. If this
 *   realignment leads to a read being aligned with an insertion or deletion
 *   against the reference, we generate a consensus sequence for the INDEL.
 * * From knowns: In this mode, we use a set of provided INDEL variants as the
 *   INDELs to generate consensus sequences for.
 *
 * Additionally, we support a union operator, that takes the union of several
 * consensus modes.
 */
object ConsensusGenerator {

  /**
   * Provides a generator to extract consensuses from aligned reads.
   *
   * @return A consensus generator that looks directly at aligned reads. Here,
   *   consensus sequences are extracted by substituting INDELs that are
   *   present in a single aligned read back into the reference sequence where
   *   they are aligned.
   */
  def fromReads(): ConsensusGenerator = {
    new ConsensusGeneratorFromReads
  }

  /**
   * Provides a generator to extract consensuses by realigning reads.
   *
   * @param wMatch Match weight to use for Smith-Waterman.
   * @param wMismatch Mismatch penalty to use for Smith-Waterman.
   * @param wInsert Insert penalty to use for Smith-Waterman.
   * @param wDelete Deletion penalty to use for Smith-Waterman.
   * @return A consensus generator that uses Smith-Waterman to realign reads to
   *   the reference sequence they overlap. INDELs that are present after this
   *   realignment stage are then used as targets for a second realignment phase.
   */
  def fromReadsWithSmithWaterman(wMatch: Double,
                                 wMismatch: Double,
                                 wInsert: Double,
                                 wDelete: Double): ConsensusGenerator = {
    new ConsensusGeneratorFromSmithWaterman(wMatch,
      wMismatch,
      wInsert,
      wDelete)
  }

  /**
   * (Java-specific) Provides a generator to extract consensuses from a known set of INDELs.
   *
   * @param rdd The previously called INDEL variants.
   * @return A consensus generator that looks at previously called INDELs.
   */
  def fromKnownIndels(rdd: VariantDataset): ConsensusGenerator = {
    new ConsensusGeneratorFromKnowns(rdd.rdd, 0)
  }

  /**
   * (Java-specific) Provides a generator to extract consensuses from a known set of INDELs.
   *
   * @param rdd The previously called INDEL variants.
   * @param flankSize The number of bases to flank each known INDEL by.
   * @return A consensus generator that looks at previously called INDELs.
   */
  def fromKnownIndels(rdd: VariantDataset,
                      flankSize: java.lang.Integer): ConsensusGenerator = {
    new ConsensusGeneratorFromKnowns(rdd.rdd, flankSize)
  }

  /**
   * (Scala-specific) Provides a generator to extract consensuses from a known set of INDELs.
   *
   * @param rdd The previously called INDEL variants.
   * @param flankSize The number of bases to flank each known INDEL by. Default
   *   is 0 bases.
   * @return A consensus generator that looks at previously called INDELs.
   */
  def fromKnownIndels(rdd: VariantDataset,
                      flankSize: Int = 0): ConsensusGenerator = {
    new ConsensusGeneratorFromKnowns(rdd.rdd, flankSize)
  }

  /**
   * Provides a generator to extract consensuses using several methods.
   *
   * @return A consensus generator that generates consensuses with several
   *   methods.
   */
  @scala.annotation.varargs
  def union(generators: ConsensusGenerator*): ConsensusGenerator = {
    UnionConsensusGenerator(generators.toSeq)
  }
}

/**
 * Abstract class for generating consensus sequences for INDEL realignment.
 *
 * INDEL realignment scores read alignments against the reference genome and
 * a set of "consensus" sequences. These consensus sequences represent alternate
 * alleles/haplotypes, and can be generated via a variety of methods (e.g., seen
 * in previous projects --> 1kg INDELs, seen in read alignments, etc). This
 * trait provides an interface that a consensus generation method should
 * implement to provide it's consensus sequences to the realigner.
 */
abstract class ConsensusGenerator extends Serializable {

  /**
   * @param cigar The CIGAR to process.
   * @return The number of alignment blocks that are alignment matches.
   */
  protected def numAlignmentBlocks(cigar: Cigar): Int = {
    cigar.getCigarElements.map(element => {
      element.getOperator match {
        case CigarOperator.M => 1
        case _               => 0
      }
    }).sum
  }

  /**
   * Generates targets to add to initial set of indel realignment targets, if additional
   * targets are necessary.
   *
   * @return Returns an option which wraps an RDD of indel realignment targets.
   */
  def targetsToAdd(): Option[RDD[IndelRealignmentTarget]]

  /**
   * Performs any preprocessing specific to this consensus generation algorithm, e.g.,
   * indel normalization.
   *
   * @param reads Reads to preprocess.
   * @param reference Reference genome bases covering this target.
   * @param region The region covered by this target.
   * @return Preprocessed reads.
   */
  def preprocessReadsForRealignment(
    reads: Iterable[RichAlignmentRecord],
    reference: String,
    region: ReferenceRegion): Iterable[RichAlignmentRecord]

  /**
   * For all reads in this region, generates the list of consensus sequences for realignment.
   *
   * @param reads Reads to generate consensus sequences from.
   * @return Consensus sequences to use for realignment.
   */
  def findConsensus(reads: Iterable[RichAlignmentRecord]): Iterable[Consensus]
}
