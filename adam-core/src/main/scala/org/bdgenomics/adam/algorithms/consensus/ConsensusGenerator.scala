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
import org.bdgenomics.adam.rich.RichAlignmentRecord
import scala.collection.JavaConversions._

/**
 * Trait for generating consensus sequences for INDEL realignment.
 *
 * INDEL realignment scores read alinments against the reference genome and
 * a set of "consensus" sequences. These consensus sequences represent alternate
 * alleles/haplotypes, and can be generated via a variety of methods (e.g., seen
 * in previous projects --> 1kg INDELs, seen in read alignments, etc). This
 * trait provides an interface that a consensus generation method should
 * implement to provide it's consensus sequences to the realigner.
 */
private[adam] trait ConsensusGenerator extends Serializable {

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
