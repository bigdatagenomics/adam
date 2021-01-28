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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.read.realignment.IndelRealignmentTarget
import org.bdgenomics.adam.rich.RichAlignment
import org.bdgenomics.formats.avro.Variant
import scala.math.max
import scala.transient

/**
 * Generates consensus sequences from a set of variants.
 *
 * Generates a set of consensus sequences by loading variants and filtering on
 * INDEL variants. The INDEL variants are mapped directly into consensuses using
 * their alt allele string as the consensus string.
 *
 * @param file Path to file containing variants.
 * @param sc Spark context to use.
 */
private[adam] class ConsensusGeneratorFromKnowns(rdd: RDD[Variant],
                                                 val flankSize: Int) extends ConsensusGenerator {

  private val indelTable = rdd.context.broadcast(IndelTable(rdd))

  /**
   * Generates targets to add to initial set of indel realignment targets, if additional
   * targets are necessary.
   *
   * @return Returns an option which wraps an RDD of indel realignment targets.
   */
  def targetsToAdd(): Option[RDD[IndelRealignmentTarget]] = {

    Some(rdd.filter(v => v.getReferenceAllele.length != v.getAlternateAllele.length)
      .map(v => ReferenceRegion(v))
      .map(r => {
        new IndelRealignmentTarget(Some(r),
          ReferenceRegion(r.referenceName, max(0L, r.start - flankSize), r.end + flankSize))
      }))
  }

  /**
   * Performs any preprocessing specific to this consensus generation algorithm, e.g.,
   * indel normalization. This is a no-op for the knowns model.
   *
   * @param reads Reads to preprocess.
   * @return Preprocessed reads.
   */
  def preprocessReadsForRealignment(
    reads: Iterable[RichAlignment],
    reference: String,
    region: ReferenceRegion): Iterable[RichAlignment] = {
    reads
  }

  /**
   * For all reads in this region, generates the list of consensus sequences for realignment.
   *
   * @param reads Reads to generate consensus sequences from.
   * @return Consensus sequences to use for realignment.
   */
  def findConsensus(reads: Iterable[RichAlignment]): Iterable[Consensus] = {
    if (reads.isEmpty) {
      Iterable.empty
    } else {
      val table = indelTable.value

      // get region
      val start = reads.map(_.record.getStart).min
      val end = reads.map(_.getEnd).max
      val refId = reads.head.record.getReferenceName

      val region = ReferenceRegion(refId, start, end + 1)

      // get reads
      table.getIndelsInRegion(region)
    }
  }
}
