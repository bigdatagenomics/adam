/*
 * Copyright (c) 2013-2014. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.algorithms.consensus

import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.IndelRealignmentTarget
import edu.berkeley.cs.amplab.adam.models.{Consensus, ReferenceRegion}
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import org.apache.spark.rdd.RDD

abstract class ConsensusGenerator extends Serializable {

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
  def preprocessReadsForRealignment(reads: Seq[RichADAMRecord],
                                    reference: String,
                                    region: ReferenceRegion): Seq[RichADAMRecord]

  /**
   * For all reads in this region, generates the list of consensus sequences for realignment.
   *
   * @param reads Reads to generate consensus sequences from.
   * @return Consensus sequences to use for realignment.
   */
  def findConsensus(reads: Seq[RichADAMRecord]): Seq[Consensus]
}
