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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.ds.read.realignment.IndelRealignmentTarget
import org.bdgenomics.adam.rich.RichAlignment

/**
 * A consensus generator that wraps multiple other consensus generators.
 *
 * @param generator The consensus generator implementations to delegate to.
 */
private[adam] case class UnionConsensusGenerator(
    generators: Iterable[ConsensusGenerator]) extends ConsensusGenerator {

  /**
   * Adds optional known targets before reads are examined.
   *
   * Loops over the underlying consensus generators and calls their method for
   * generating known targets. If all underlying methods return no targets, then
   * we also return no targets.
   *
   * @return Returns all a priori known targets.
   */
  def targetsToAdd(): Option[RDD[IndelRealignmentTarget]] = {
    val possibleTargets = generators.flatMap(_.targetsToAdd)

    if (possibleTargets.isEmpty) {
      None
    } else {
      Some(possibleTargets.head.context.union(possibleTargets.toSeq))
    }
  }

  /**
   * Preprocesses reads aligned to a given target.
   *
   * Loops over the underlying methods and calls their preprocessing algorithms.
   * The preprocessed reads are then passed to each new algorithm. I.e., the
   * first underlying implementation will run on the unprocessed reads, then the
   * output of the first implementation's preprocessing method will be passed
   * as input to the second algorithm's preprocessing method.
   *
   * @param reads Reads to preprocess.
   * @param reference Reference genome bases covering this target.
   * @param region The region covered by this target.
   * @return Preprocessed reads, as treated by all preprocessing methods..
   */
  def preprocessReadsForRealignment(
    reads: Iterable[RichAlignment],
    reference: String,
    region: ReferenceRegion): Iterable[RichAlignment] = {
    generators.foldRight(reads)(
      (g, r) => g.preprocessReadsForRealignment(r, reference, region))
  }

  /**
   * For all reads in this region and all methods, generates the consensus sequences.
   *
   * We do not guarantee that there are no duplicate consensuses in the output
   * of this method, since multiple underlying methods may generate a single
   * consensus sequence. Instead, we defer deduplication to the INDEL realigner.
   *
   * @param reads Reads to generate consensus sequences from.
   * @return Consensus sequences to use for realignment.
   */
  def findConsensus(reads: Iterable[RichAlignment]): Iterable[Consensus] = {
    generators.flatMap(_.findConsensus(reads))
  }
}
