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
import org.bdgenomics.adam.models.{ MdTag, ReferencePosition, ReferenceRegion }
import org.bdgenomics.adam.ds.read.realignment.IndelRealignmentTarget
import org.bdgenomics.adam.rich.RichAlignment._
import org.bdgenomics.adam.rich.RichAlignment
import org.bdgenomics.formats.avro.Alignment

/**
 * Consensus generator that examines the read alignments.
 *
 * This consensus generation method preprocesses the reads by left normalizing
 * all INDELs seen in the reads, and then generates consensus sequences from
 * reads whose local alignments contain INDELs. These consensuses are deduped
 * before they are returned.
 */
private[adam] class ConsensusGeneratorFromReads extends ConsensusGenerator {

  /**
   * No targets to add if generating consensus targets from reads.
   *
   * @return Returns a None.
   */
  def targetsToAdd(): Option[RDD[IndelRealignmentTarget]] = None

  /**
   * Performs read preprocessing by normalizing indels for all reads that have evidence of one
   * indel.
   *
   * @param reads Reads to process.
   * @return Reads with indels normalized if they contain a single indel.
   */
  def preprocessReadsForRealignment(
    reads: Iterable[RichAlignment],
    reference: String,
    region: ReferenceRegion): Iterable[RichAlignment] = {
    reads.map(r => {
      // if there are two alignment blocks (sequence matches) then there is a single indel in the read
      if (numAlignmentBlocks(r.samtoolsCigar) == 2) {
        // left align this indel and update the mdtag
        val cigar = NormalizationUtils.leftAlignIndel(r)
        val mdTag = MdTag.moveAlignment(r, cigar)

        val newRead: RichAlignment = Alignment.newBuilder(r)
          .setCigar(cigar.toString)
          .setMismatchingPositions(mdTag.toString())
          .build()

        newRead
      } else {
        r
      }
    })
  }

  /**
   * Generates consensus sequences from reads with INDELs.
   *
   * Loops over provided reads. Filters all reads without an MD tag, and then
   * generates consensus sequences. If a read contains a single INDEL aligned to
   * the reference, we emit that INDEL. Else, we do not emit a consensus from
   * the read. We dedup the consensuses to remove any INDELs observed in
   * multiple reads and return.
   *
   * @param Reads to search for INDELs.
   * @return Consensuses generated from reads with a singel INDEL
   */
  def findConsensus(reads: Iterable[RichAlignment]): Iterable[Consensus] = {
    reads.filter(r => r.mdTag.isDefined)
      .flatMap(r => {
        // try to generate a consensus alignment - if a consensus exists, add it to our
        // list of consensuses to test
        Consensus.generateAlternateConsensus(
          r.getSequence,
          ReferencePosition(
            r.getReferenceName,
            r.getStart
          ),
          r.samtoolsCigar
        )
      })
      .toSeq
      .distinct
  }
}
