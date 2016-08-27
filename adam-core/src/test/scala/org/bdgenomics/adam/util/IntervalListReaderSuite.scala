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
package org.bdgenomics.adam.util

import org.scalatest._
import java.io.File
import org.bdgenomics.adam.models.ReferenceRegion

class IntervalListReaderSuite extends ADAMFunSuite {

  test("Can read the simple GATK-supplied example interval list file") {
    val file = new File(testFile("example_intervals.list"))

    val reader = new IntervalListReader(file)

    val intervals: Seq[(ReferenceRegion, String)] = reader.toSeq

    assert(intervals.size === 6)

    (1 until 6).foreach {
      idx => assert(intervals(idx - 1)._2 === "target_%d".format(idx))
    }

    assert(reader.sequenceDictionary != null, "sequence dictionary was null")
    assert(reader.sequenceDictionary.records.size === 2)
    assert(reader.sequenceDictionary("1").get.length === 249250621)
    assert(reader.sequenceDictionary("2").get.length === 243199373)
  }
}
