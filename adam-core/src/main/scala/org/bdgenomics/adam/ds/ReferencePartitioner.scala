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

import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferenceRegion,
  SequenceDictionary
}

/**
 * Repartitions objects that are keyed by a ReferencePosition or ReferenceRegion
 * into a single partition per contig.
 */
case class ReferencePartitioner(sd: SequenceDictionary) extends Partitioner {

  // extract just the reference names
  private val referenceNames = sd.records.map(_.name)

  override def numPartitions: Int = referenceNames.length

  private def partitionFromName(name: String): Int = {
    // which reference is this in?
    val pIdx = referenceNames.indexOf(name)

    // provide debug info to user if key is bad
    assert(pIdx != -1, "Reference not found in " + sd + " for key " + name)

    pIdx
  }

  override def getPartition(key: Any): Int = key match {
    case rp: ReferencePosition => {
      partitionFromName(rp.referenceName)
    }
    case rr: ReferenceRegion => {
      partitionFromName(rr.referenceName)
    }
    case _ => throw new IllegalArgumentException("Only ReferencePositions or ReferenceRegions can be used as a key.")
  }
}
