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
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord

class ConsensusGeneratorFromReadsSuite extends ADAMFunSuite {

  val cg = new ConsensusGeneratorFromReads

  def artificial_reads: RDD[AlignmentRecord] = {
    val path = resourcePath("artificial.sam")
    sc.loadAlignments(path)
  }

  sparkTest("checking search for consensus list for artificial reads") {
    val consensus = cg.findConsensus(artificial_reads.map(new RichAlignmentRecord(_))
      .collect()
      .toSeq)

    assert(consensus.size === 2)
  }
}

