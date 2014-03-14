/*
 * Copyright 2014 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.rich

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichRDDReferenceRecords._
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite

class RichRDDReferenceRecordsSuite extends SparkFunSuite {

  sparkTest("can convert referenceId values of RDD[ADAMRecord]") {

    val r1 = adamRecord(0, "foo", "read1", 1000L, true)
    val r2 = adamRecord(1, "bar", "read2", 2000L, true)

    val map = Map(r1.getReadName -> r1, r2.getReadName -> r2)

    val rdd = sc.parallelize(Seq(r1, r2))

    val remap = Map(0 -> 10, 1 -> 11)
    val remapped = rdd.remapReferenceId(remap)(sc)

    val seq = remapped.collect().sortBy(_.getReadName.toString)
    assert(seq.size === 2)
    assert(seq(0).getReadName.toString === "read1")
    assert(seq(1).getReadName.toString === "read2")
    assert(seq(0).getReferenceId === 10)
    assert(seq(1).getReferenceId === 11)
  }

  def adamRecord(referenceId: Int, referenceName: String, readName: String, start: Long, readMapped: Boolean) =
    ADAMRecord.newBuilder()
      .setReferenceId(referenceId)
      .setReferenceName(referenceName)
      .setReadName(readName)
      .setReadMapped(readMapped)
      .setStart(start)
      .build()
}
