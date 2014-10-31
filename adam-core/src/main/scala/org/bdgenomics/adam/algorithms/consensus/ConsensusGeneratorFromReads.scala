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
import org.bdgenomics.adam.models.{ Consensus, ReferencePosition, ReferenceRegion }
import org.bdgenomics.adam.rdd.read.realignment.IndelRealignmentTarget
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.rich.RichAlignmentRecord._
import org.bdgenomics.adam.rich.RichCigar._
import org.bdgenomics.adam.util.ImplicitJavaConversions._
import org.bdgenomics.adam.util.MdTag
import org.bdgenomics.adam.util.NormalizationUtils._
import org.bdgenomics.formats.avro.AlignmentRecord

class ConsensusGeneratorFromReads extends ConsensusGenerator {

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
  def preprocessReadsForRealignment(reads: Iterable[RichAlignmentRecord],
                                    reference: String,
                                    region: ReferenceRegion): Iterable[RichAlignmentRecord] = {
    reads.map(r => {
      // if there are two alignment blocks (sequence matches) then there is a single indel in the read
      if (r.samtoolsCigar.numAlignmentBlocks == 2) {
        // left align this indel and update the mdtag
        val cigar = leftAlignIndel(r)
        val mdTag = MdTag.moveAlignment(r, cigar)

        val newRead: RichAlignmentRecord = AlignmentRecord.newBuilder(r)
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
   * Generates concensus sequences from reads with indels.
   */
  def findConsensus(reads: Iterable[RichAlignmentRecord]): Iterable[Consensus] = {
    reads.filter(r => r.mdTag.isDefined)
      .flatMap(r => {
        // try to generate a consensus alignment - if a consensus exists, add it to our
        // list of consensuses to test
        Consensus.generateAlternateConsensus(r.getSequence,
          ReferencePosition(r.getContig.getContigName,
            r.getStart),
          r.samtoolsCigar)
      })
      .toSeq
      .distinct
  }

}
