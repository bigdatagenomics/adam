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
package org.bdgenomics.adam.ds.read

import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Alignment
import org.bdgenomics.adam.util.ADAMFunSuite

class RepairPartitionsSuite extends ADAMFunSuite {

  def makeRead(readName: String): Alignment = {
    Alignment.newBuilder()
      .setReadName(readName)
      .build()
  }

  test("don't pull from the first partition") {
    val iter = Iterator(makeRead("read1"),
      makeRead("read2"))

    val reads = RepairPartitions.getPairAtStart(0, iter)

    assert(reads.isEmpty)
  }

  test("properly handle pulling from an empty iterator") {
    assert(RepairPartitions.getPairAtStart(1, Iterator.empty).isEmpty)
  }

  test("gets a single read from the partition if there are no other reads in the pair") {
    val iter = Iterator(makeRead("read1"),
      makeRead("read2"))

    val reads = RepairPartitions.getPairAtStart(1, iter)
      .toSeq

    assert(reads.size === 1)
    assert(reads.head._1 === 1)
    assert(reads.head._2.size === 1)
    assert(reads.head._2.forall(_.getReadName == "read1"))
  }

  test("gets all the reads from a pair from the start of a partition") {
    val iter = Iterator(makeRead("read1"),
      makeRead("read1"),
      makeRead("read2"))

    val reads = RepairPartitions.getPairAtStart(1, iter)
      .toSeq

    assert(reads.size === 1)
    assert(reads.head._1 === 1)
    assert(reads.head._2.size === 2)
    assert(reads.head._2.forall(_.getReadName == "read1"))
  }

  test("properly handle dropping from an empty iterator") {
    assert(RepairPartitions.dropPairAtStart(1, Iterator.empty).isEmpty)
  }

  test("don't drop from the first partition") {
    val iter = Iterator(makeRead("read1"),
      makeRead("read2"))

    val reads = RepairPartitions.dropPairAtStart(0, iter)

    assert(reads.size === 2)
  }

  test("drop a single read from the partition if there are no other reads in the pair") {
    val iter = Iterator(makeRead("read1"),
      makeRead("read2"))

    val reads = RepairPartitions.dropPairAtStart(1, iter)
      .toSeq

    assert(reads.size === 1)
    assert(reads.forall(_.getReadName == "read2"))
  }

  test("drops all the reads from a pair from the start of a partition") {
    val iter = Iterator(makeRead("read1"),
      makeRead("read1"),
      makeRead("read2"))

    val reads = RepairPartitions.dropPairAtStart(1, iter)
      .toSeq

    assert(reads.size === 1)
    assert(reads.forall(_.getReadName == "read2"))
  }

  val readArray = Array(Seq(makeRead("read1")),
    Seq(makeRead("read2"), makeRead("read2")))

  test("only append to the first partition") {
    val iter = Iterator(makeRead("read1"))

    val reads = RepairPartitions.addPairsAtEnd(0, iter, readArray)
      .toSeq

    assert(reads.size === 2)
    assert(reads.forall(_.getReadName == "read1"))
  }

  test("drop a single read from the partition and append read when in the middle") {
    val iter = Iterator(makeRead("read1"),
      makeRead("read2"))

    val reads = RepairPartitions.addPairsAtEnd(1, iter, readArray)
      .toSeq

    assert(reads.size === 3)
    assert(reads.forall(_.getReadName == "read2"))
  }

  test("drop reads from the start and don't append when in the last partition") {
    val iter = Iterator(makeRead("read2"),
      makeRead("read2"),
      makeRead("read3"))

    val reads = RepairPartitions.addPairsAtEnd(2, iter, readArray)
      .toSeq

    assert(reads.size === 1)
    assert(reads.forall(_.getReadName == "read3"))
  }

  test("can't have more records than number of partitions") {
    val array = Array((1, Seq(makeRead("read2"), makeRead("read2"))),
      (0, Seq(makeRead("read1"))))

    intercept[AssertionError] {
      RepairPartitions.unrollArray(array, 2)
    }
  }

  test("unroll array for broadcast") {
    val array = Array((2, Seq(makeRead("read2"), makeRead("read2"))),
      (1, Seq(makeRead("read1"))))

    val unrolledArray = RepairPartitions.unrollArray(array, 3)

    assert(unrolledArray.length === 3)
    assert(unrolledArray(0).size === 1)
    assert(unrolledArray(0).forall(_.getReadName == "read1"))
    assert(unrolledArray(1).size === 2)
    assert(unrolledArray(1).forall(_.getReadName == "read2"))
    assert(unrolledArray(2).isEmpty)
  }

  sparkTest("move pairs around an rdd") {
    val rdd = sc.parallelize(Seq((0, makeRead("read1")),
      (1, makeRead("read1")),
      (1, makeRead("read2")),
      (2, makeRead("read2")),
      (2, makeRead("read2")),
      (2, makeRead("read3"))))
      .partitionBy(new Partitioner {
        def numPartitions: Int = 3

        def getPartition(key: Any): Int = {
          key.asInstanceOf[Int]
        }
      }).map(_._2)

    val repaired = RepairPartitions(rdd)

    val readNamesByPartition = repaired.mapPartitionsWithIndex((idx, iter) => {
      iter.map(r => (idx, r.getReadName))
    }).countByValue()

    assert(readNamesByPartition.size === 3)
    assert(readNamesByPartition((0, "read1")) === 2)
    assert(readNamesByPartition((1, "read2")) === 3)
    assert(readNamesByPartition((2, "read3")) === 1)
  }
}
