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
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.{Consensus, ReferenceRegion, ReferencePosition}
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.rich.RichCigar
import edu.berkeley.cs.amplab.adam.rich.RichCigar._
import edu.berkeley.cs.amplab.adam.util.MdTag
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions
import edu.berkeley.cs.amplab.adam.util.ImplicitJavaConversions._
import edu.berkeley.cs.amplab.adam.util.NormalizationUtils._
import net.sf.samtools.{Cigar, CigarOperator, CigarElement}
import org.apache.spark.rdd.RDD

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
  def preprocessReadsForRealignment(reads: Seq[RichADAMRecord],
                                    reference: String,
                                    region: ReferenceRegion): Seq[RichADAMRecord] = {
    reads.map(r => {
      // if there are two alignment blocks (sequence matches) then there is a single indel in the read
      if (r.samtoolsCigar.numAlignmentBlocks == 2) {
        // left align this indel and update the mdtag
        val cigar = leftAlignIndel(r)
        val mdTag = MdTag.moveAlignment(r, cigar)
        
        val newRead: RichADAMRecord = ADAMRecord.newBuilder(r)
          .setCigar(cigar.toString)
          .setMismatchingPositions(mdTag.toString)
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
  def findConsensus(reads : Seq[RichADAMRecord]): Seq[Consensus] = {
    reads.filter(r => r.mdTag.isDefined)
      .flatMap(r => {
        // try to generate a consensus alignment - if a consensus exists, add it to our
        // list of consensuses to test
        Consensus.generateAlternateConsensus(r.getSequence, 
                                             ReferencePosition(r.getReferenceId, 
                                                               r.getStart),
                                             r.samtoolsCigar)
      }).distinct
  }

}
