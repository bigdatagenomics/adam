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

import org.bdgenomics.adam.algorithms.smithwaterman.SmithWatermanConstantGapScoring
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rich.RichADAMRecord
import org.bdgenomics.adam.rich.RichADAMRecord._
import org.bdgenomics.adam.rich.RichCigar._
import org.bdgenomics.adam.util.MdTag
import org.bdgenomics.formats.avro.ADAMRecord

class ConsensusGeneratorFromSmithWaterman(wMatch: Double,
                                          wMismatch: Double,
                                          wInsert: Double,
                                          wDelete: Double) extends ConsensusGeneratorFromReads {

  /**
   * Attempts realignment of all reads using Smith-Waterman. Accepts all realignments that have one
   * or fewer indels.
   *
   * @param reads Reads to process.
   * @return Reads with indels normalized if they contain a single indel.
   */
  override def preprocessReadsForRealignment(reads: Iterable[RichADAMRecord],
                                             reference: String,
                                             region: ReferenceRegion): Iterable[RichADAMRecord] = {
    val rds: Iterable[RichADAMRecord] = reads.map(r => {

      val sw = new SmithWatermanConstantGapScoring(r.record.getSequence.toString,
        reference,
        wMatch,
        wMismatch,
        wInsert,
        wDelete)
      println("for " + r.record.getReadName + " sw to " + sw.xStart + " with " + sw.cigarX)

      // if we realign with fewer than three alignment blocks, then take the new alignment
      if (sw.cigarX.numAlignmentBlocks <= 2) {
        val mdTag = MdTag(r.record.getSequence.toString,
          reference.drop(sw.xStart),
          sw.cigarX,
          region.start)

        val newRead: RichADAMRecord = ADAMRecord.newBuilder(r)
          .setStart(sw.xStart + region.start)
          .setCigar(sw.cigarX.toString)
          .setMismatchingPositions(mdTag.toString())
          .build()

        newRead
      } else {
        r
      }
    })

    super.preprocessReadsForRealignment(rds, reference, region)
  }
}
