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

import edu.berkeley.cs.amplab.adam.avro.{ADAMContig, ADAMVariant}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.variation.ADAMVariationContext._
import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.IndelRealignmentTarget
import edu.berkeley.cs.amplab.adam.models.{Consensus, 
                                           IndelTable,
                                           ADAMVariantContext, 
                                           ReferenceRegion}
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ConsensusGeneratorFromKnowns (file: String, sc: SparkContext) extends ConsensusGenerator {

  val indelTable = sc.broadcast(IndelTable(file, sc))

  /**
   * Generates targets to add to initial set of indel realignment targets, if additional
   * targets are necessary.
   *
   * @return Returns an option which wraps an RDD of indel realignment targets.
   */
  def targetsToAdd(): Option[RDD[IndelRealignmentTarget]] = {
    val rdd: RDD[ADAMVariantContext] = sc.adamVCFLoad(file)

    Some(rdd.map(_.variant.variant)
      .filter(v => v.getReferenceAllele.length != v.getVariantAllele.length)
      .map(v => ReferenceRegion(v.getContig.getContigId, v.getPosition, v.getPosition + v.getReferenceAllele.length))
      .map(r => new IndelRealignmentTarget(Some(r), r)))
  }

  /**
   * Performs any preprocessing specific to this consensus generation algorithm, e.g.,
   * indel normalization.
   *
   * @param reads Reads to preprocess.
   * @return Preprocessed reads.
   */
  def preprocessReadsForRealignment(reads: Seq[RichADAMRecord],
                                    reference: String,
                                    region: ReferenceRegion): Seq[RichADAMRecord] = {
    reads
  }

  /**
   * For all reads in this region, generates the list of consensus sequences for realignment.
   *
   * @param reads Reads to generate consensus sequences from.
   * @return Consensus sequences to use for realignment.
   */
  def findConsensus(reads: Seq[RichADAMRecord]): List[Consensus] = {
    val table = indelTable.value

    // get region
    val start = reads.map(_.record.getStart.toLong).reduce(_ min _)
    val end = reads.flatMap(_.end).reduce(_ max _)
    val refId = reads.head.record.getReferenceId

    val region = ReferenceRegion(refId, start, end + 1)

    // get reads
    table.getIndelsInRegion(region).toList
  }
}
