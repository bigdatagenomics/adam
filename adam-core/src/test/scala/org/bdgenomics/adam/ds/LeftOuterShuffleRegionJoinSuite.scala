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
package org.bdgenomics.adam.ds

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.formats.avro.Alignment

class LeftOuterShuffleRegionJoinSuite(partitionMap: Seq[Option[(ReferenceRegion, ReferenceRegion)]])
    extends OuterRegionJoinSuite {

  val partitionSize = 3
  var seqDict: SequenceDictionary = _

  before {
    seqDict = SequenceDictionary(
      SequenceRecord("chr1", 15, url = "test://chrom1"),
      SequenceRecord("chr2", 15, url = "test://chrom2"))
  }

  def runJoin(leftRdd: RDD[(ReferenceRegion, Alignment)],
              rightRdd: RDD[(ReferenceRegion, Alignment)]): RDD[(Option[Alignment], Alignment)] = {
    LeftOuterShuffleRegionJoin[Alignment, Alignment](rightRdd, leftRdd)
      .compute().map(_.swap)
  }
}
