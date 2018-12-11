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
package org.bdgenomics.adam.rdd

import org.apache.spark.RangePartitioner
import org.bdgenomics.adam.models.{ ReferencePosition, SequenceRecord, SequenceDictionary }
import org.bdgenomics.adam.projections.Projection
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Reference }
import scala.util.Random

class GenomicPositionPartitionerSuite extends ADAMFunSuite {

  test("partitions the UNMAPPED ReferencePosition into the top partition") {
    val parter = GenomicPositionPartitioner(10, SequenceDictionary(record("foo", 1000)))

    assert(parter.numPartitions === 11)
    assert(parter.getPartition(ReferencePosition.UNMAPPED) === 10)
  }

  test("if we do not have a contig for a record, we throw an IAE") {
    val parter = GenomicPositionPartitioner(10, SequenceDictionary(record("foo", 1000)))

    assert(parter.numPartitions === 11)
    intercept[IllegalArgumentException] {
      parter.getPartition(ReferencePosition("chrFoo", 10))
    }
  }

  test("partitioning into N pieces on M total sequence length, where N > M, results in M partitions") {
    val parter = GenomicPositionPartitioner(10, SequenceDictionary(record("foo", 9)))
    assert(parter.numPartitions === 10)
  }

  test("correctly partitions a single dummy sequence into two pieces") {
    val parter = GenomicPositionPartitioner(2, SequenceDictionary(record("foo", 10)))
    assert(parter.getPartition(ReferencePosition("foo", 3)) === 0)
    assert(parter.getPartition(ReferencePosition("foo", 7)) === 1)
  }

  test("correctly counts cumulative lengths") {
    val parter = GenomicPositionPartitioner(3, SequenceDictionary(record("foo", 20), record("bar", 10)))

    assert(parter.cumulativeLengths("bar") === 0)
    assert(parter.cumulativeLengths("foo") === 10)
  }

  test("correctly partitions positions across two dummy sequences") {
    val parter = GenomicPositionPartitioner(3, SequenceDictionary(record("bar", 20), record("foo", 10)))
    // check easy examples
    assert(parter.getPartition(ReferencePosition("foo", 8)) === 2)
    assert(parter.getPartition(ReferencePosition("foo", 18)) === 3)
    assert(parter.getPartition(ReferencePosition("bar", 18)) === 1)
    assert(parter.getPartition(ReferencePosition("bar", 8)) === 0)

    // check edge cases
    assert(parter.getPartition(ReferencePosition("foo", 0)) === 2)
    assert(parter.getPartition(ReferencePosition("foo", 10)) === 3)
    assert(parter.getPartition(ReferencePosition("bar", 0)) === 0)
  }

  sparkTest("test that we can range partition ADAMRecords") {
    val rand = new Random(1000L)
    val count = 1000
    val pos = sc.parallelize((1 to count).map(i => adamRecord("chr1", "read_%d".format(i), rand.nextInt(100), readMapped = true)), 1)
    val parts = 200
    val pairs = pos.map(p => (ReferencePosition(p.getReferenceName, p.getStart), p))
    val parter = new RangePartitioner(parts, pairs)
    val partitioned = pairs.sortByKey().partitionBy(parter)

    assert(partitioned.count() === count)
    // check here to make sure that we have at least increased the number of partitions
    // as of spark 1.1.0, range partitioner does not guarantee that you will receive a 
    // number of partitions equal to the number requested
    assert(partitioned.partitions.length > 1)
  }

  sparkTest("test that we can range partition ADAMRecords indexed by sample") {
    val rand = new Random(1000L)
    val count = 1000
    val pos = sc.parallelize((1 to count).map(i => adamRecord("chr1", "read_%d".format(i), rand.nextInt(100), readMapped = true)), 1)
    val parts = 200
    val pairs = pos.map(p => ((ReferencePosition(p.getReferenceName, p.getStart), "sample"), p))
    val parter = new RangePartitioner(parts, pairs)
    val partitioned = pairs.sortByKey().partitionBy(parter)

    assert(partitioned.count() === count)
    assert(partitioned.partitions.length > 1)
  }

  sparkTest("test that simple partitioning works okay on a reasonable set of ADAMRecords") {
    val filename = testFile("reads12.sam")
    val parts = 1

    val p = {
      import org.bdgenomics.adam.projections.AlignmentRecordField._
      Projection(referenceName, start, readName, readMapped)
    }
    val gDataset = sc.loadAlignments(filename, optProjection = Some(p))
    val rdd = gDataset.rdd

    val parter = GenomicPositionPartitioner(parts, gDataset.sequences)

    assert(rdd.count() === 200)

    val keyed =
      rdd.map(rec => (ReferencePosition(rec.getReferenceName, rec.getStart), rec)).sortByKey()

    val keys = keyed.map(_._1).collect()
    assert(!keys.exists(rp => parter.getPartition(rp) < 0 || parter.getPartition(rp) >= parts))

    val partitioned = keyed.partitionBy(parter)
    assert(partitioned.count() === 200)

    val partSizes = partitioned.mapPartitions {
      itr =>
        List(itr.size).iterator
    }

    assert(partSizes.count() === parts + 1)
  }

  sparkTest("test indexed ReferencePosition partitioning works on a set of indexed ADAMRecords") {
    val filename = testFile("reads12.sam")
    val parts = 10

    val gDataset = sc.loadAlignments(filename)
    val rdd = gDataset.rdd

    val parter = GenomicPositionPartitioner(parts, gDataset.sequences)

    val p = {
      import org.bdgenomics.adam.projections.AlignmentRecordField._
      Projection(referenceName, start, readName, readMapped)
    }

    assert(rdd.count() === 200)

    val keyed =
      rdd.keyBy(rec => (ReferencePosition(rec.getReferenceName, rec.getStart), "sample")).sortByKey()

    val keys = keyed.map(_._1).collect()
    assert(!keys.exists(rp => parter.getPartition(rp) < 0 || parter.getPartition(rp) >= parts))

    val partitioned = keyed.partitionBy(parter)
    assert(partitioned.count() === 200)

    val partSizes = partitioned.mapPartitions {
      itr =>
        List(itr.size).iterator
    }

    assert(partSizes.count() === parts + 1)
  }

  def adamRecord(referenceName: String, readName: String, start: Long, readMapped: Boolean) = {
    val reference = Reference.newBuilder
      .setName(referenceName)
      .build

    AlignmentRecord.newBuilder()
      .setReferenceName(reference.getName)
      .setReadName(readName)
      .setReadMapped(readMapped)
      .setStart(start)
      .build()
  }

  def record(name: String, length: Long) = SequenceRecord(name.toString, length.toInt)
}

class PositionKeyed[U <: Serializable] extends Serializable {

}

class SerializableIterator[U](itr: Iterator[U]) extends Iterator[U] with Serializable {
  def hasNext: Boolean = itr.hasNext

  def next(): U = itr.next()
}
