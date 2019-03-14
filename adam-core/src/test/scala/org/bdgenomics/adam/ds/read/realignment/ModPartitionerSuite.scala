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
package org.bdgenomics.adam.ds.read.realignment

import org.scalatest.FunSuite

class ModPartitionerSuite extends FunSuite {

  val partitioner = ModPartitioner(123)

  test("report number of partitions correctly") {
    assert(partitioner.numPartitions === 123)
  }

  test("partition a number that is lower than the number of partitions and positive") {
    assert(partitioner.getPartition(12) == 12)
  }

  test("partition a number that is greater than the number of partitions and positive") {
    assert(partitioner.getPartition(321) == 75)
  }

  test("partition a number that is lower than the number of partitions and negative") {
    assert(partitioner.getPartition(-21) == 21)
  }

  test("partition a number that is greater than the number of partitions and negative") {
    assert(partitioner.getPartition(-1234) == 4)
  }

  test("fire an exception if input is not an integer") {
    intercept[IllegalArgumentException] {
      partitioner.getPartition("a string")
    }
  }
}
