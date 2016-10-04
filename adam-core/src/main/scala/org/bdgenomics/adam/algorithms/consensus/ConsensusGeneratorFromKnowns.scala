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
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.Variant
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
private[adam] class ConsensusGeneratorFromKnowns(file: String,
                                                 sc: SparkContext) extends ConsensusGenerator {

  private val indelTable = sc.broadcast(IndelTable(file, sc))

  /**
   * Generates targets to add to initial set of indel realignment targets, if additional
   * targets are necessary.
   *
   * @return Returns an option which wraps an RDD of indel realignment targets.
   */
  def targetsToAdd(): Option[RDD[IndelRealignmentTarget]] = {
    val rdd: RDD[Variant] = sc.loadVariants(file).rdd

    Some(rdd.filter(v => v.getReferenceAllele.length != v.getAlternateAllele.length)
      .map(v => ReferenceRegion(v.getContigName, v.getStart, v.getStart + v.getReferenceAllele.length))
      .map(r => new IndelRealignmentTarget(Some(r), r)))
  }

  /**
   * Performs any preprocessing specific to this consensus generation algorithm, e.g.,
   * indel normalization. This is a no-op for the knowns model.
   *
   * @param reads Reads to preprocess.
   * @return Preprocessed reads.
   */
  def preprocessReadsForRealignment(
    reads: Iterable[RichAlignmentRecord],
    reference: String,
    region: ReferenceRegion): Iterable[RichAlignmentRecord] = {
    reads
  }

  /**
   * For all reads in this region, generates the list of consensus sequences for realignment.
   *
   * @param reads Reads to generate consensus sequences from.
   * @return Consensus sequences to use for realignment.
   */
  def findConsensus(reads: Iterable[RichAlignmentRecord]): Iterable[Consensus] = {
    val table = indelTable.value

    // get region
    val start = reads.map(_.record.getStart).min
    val end = reads.map(_.getEnd).max
    val refId = reads.head.record.getContigName

    val region = ReferenceRegion(refId, start, end + 1)

    // get reads
    table.getIndelsInRegion(region)
  }
}
