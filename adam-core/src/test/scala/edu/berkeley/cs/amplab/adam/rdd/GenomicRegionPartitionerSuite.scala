/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.models.{ReferencePosition, SequenceRecord, SequenceDictionary}
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.projections.Projection

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner
import scala.util.Random

class GenomicRegionPartitionerSuite extends SparkFunSuite {

  test("partitions the UNMAPPED ReferencePosition into the top partition") {
    val parter = new GenomicRegionPartitioner(10, SequenceDictionary(record(0, "foo", 1000)))

    assert(parter.numPartitions === 11)
    assert(parter.getPartition(ReferencePosition.UNMAPPED) === 10)
  }

  test("partitioning into N pieces on M total sequence length, where N > M, results in M partitions") {
    val parter = new GenomicRegionPartitioner(10, SequenceDictionary(record(0, "foo", 9)))
    assert(parter.numPartitions === 10)
  }

  test("correctly partitions a single dummy sequence into two pieces") {
    val parter = new GenomicRegionPartitioner(2, SequenceDictionary(record(0, "foo", 10)))
    assert(parter.getPartition(ReferencePosition(0, 3)) === 0)
    assert(parter.getPartition(ReferencePosition(0, 7)) === 1)
  }

  test("correctly counts cumulative lengths") {
    val parter = new GenomicRegionPartitioner(3, SequenceDictionary(record(0, "foo", 20), record(1, "bar", 10)))

    assert(parter.cumulativeLengths(0) === 0)
    assert(parter.cumulativeLengths(1) === 20)
  }

  test("correctly partitions positions across two dummy sequences") {
    val parter = new GenomicRegionPartitioner(3, SequenceDictionary(record(0, "foo", 20), record(1, "bar", 10)))

    // check easy examples
    assert(parter.getPartition(ReferencePosition(0, 8)) === 0)
    assert(parter.getPartition(ReferencePosition(0, 18)) === 1)
    assert(parter.getPartition(ReferencePosition(1, 8)) === 2)

    // check edge cases
    assert(parter.getPartition(ReferencePosition(0, 0)) === 0)
    assert(parter.getPartition(ReferencePosition(0, 10)) === 1)
    assert(parter.getPartition(ReferencePosition(1, 0)) === 2)
  }

  sparkTest("test that we can range partition ADAMRecords") {
    val rand = new Random(1000L)
    val count = 1000
    val pos = sc.parallelize((1 to count).map(i => adamRecord(0, "1", "read_%d".format(i), rand.nextInt(100), readMapped = true)))
    val parts = 200
    val pairs = pos.map(p => (ReferencePosition(p.getReferenceId, p.getStart), p))
    val parter = new RangePartitioner(parts, pairs)
    val partitioned = pairs.sortByKey().partitionBy(parter)

    assert(partitioned.count() === count)

    val sizes: RDD[Int] = partitioned.mapPartitions {
      itr: Iterator[(ReferencePosition, ADAMRecord)] =>
        List(itr.size).iterator
    }

    assert(sizes.collect().size === parts)
  }

  sparkTest("test that simple partitioning works okay on a reasonable set of ADAMRecords") {
    val filename = ClassLoader.getSystemClassLoader.getResource("reads12.sam").getFile
    val parts = 1

    val dict = sc.adamDictionaryLoad[ADAMRecord](filename)
    val parter = new GenomicRegionPartitioner(parts, dict)

    val p = {
      import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField._
      Projection(referenceName, referenceId, start, readName, readMapped)
    }
    val rdd: RDD[ADAMRecord] = sc.adamLoad(filename, projection = Some(p))

    assert(rdd.count() === 200)

    val keyed =
      rdd.map(rec => (ReferencePosition(rec.getReferenceId, rec.getStart), rec)).sortByKey()

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

  def adamRecord(referenceId: Int, referenceName: String, readName: String, start: Long, readMapped: Boolean) =
    ADAMRecord.newBuilder()
      .setReferenceId(referenceId)
      .setReferenceName(referenceName)
      .setReadName(readName)
      .setReadMapped(readMapped)
      .setStart(start)
      .build()

  def record(id: Int, name: CharSequence, length: Long) =
    SequenceRecord(id, name, length, "")
}

class PositionKeyed[U <: Serializable] extends Serializable {

}

class SerializableIterator[U](itr: Iterator[U]) extends Iterator[U] with Serializable {
  def hasNext: Boolean = itr.hasNext

  def next(): U = itr.next()
}
