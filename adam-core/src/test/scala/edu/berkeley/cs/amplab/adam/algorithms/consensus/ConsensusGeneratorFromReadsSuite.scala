/*
 * Copyright (c) 2014. Regents of the University of California
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

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import parquet.filter.UnboundRecordFilter
import edu.berkeley.cs.amplab.adam.models.Consensus
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord

class ConsensusGeneratorFromReadsSuite extends SparkFunSuite {

  val cg = new ConsensusGeneratorFromReads

  def artificial_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  sparkTest("checking search for consensus list for artitifical reads") {
    val consensus = cg.findConsensus(artificial_reads.map(new RichADAMRecord(_))
      .collect()
      .toSeq)
    
    assert(consensus.length === 2)
  }
}

