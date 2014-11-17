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
package org.bdgenomics.adam.rdd.read.comparisons

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.metrics.MappedPosition
import org.bdgenomics.adam.metrics.aggregators.HistogramAggregator
import org.bdgenomics.adam.projections.AlignmentRecordField
import org.bdgenomics.adam.util.{ Histogram, SparkFunSuite }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class ComparisonTraversalEngineSuite extends SparkFunSuite {

  sparkTest("generate works on a simple RDD") {
    val c0 = Contig.newBuilder
      .setContigName("chr0")
      .build

    val a0 = AlignmentRecord.newBuilder()
      .setContig(c0)
      .setRecordGroupName("group0")
      .setReadName("read0")
      .setStart(100)
      .setPrimaryAlignment(true)
      .setReadPaired(false)
      .setReadMapped(true)
      .build()
    val a1 = AlignmentRecord.newBuilder(a0)
      .setReadName("read1")
      .setStart(200)
      .build()

    val b0 = AlignmentRecord.newBuilder(a0)
      .setStart(105)
      .build()
    val b1 = AlignmentRecord.newBuilder(a1).build()

    val a = sc.parallelize(Seq(a0, a1))
    val b = sc.parallelize(Seq(b0, b1))

    import AlignmentRecordField._
    val fields = Seq(recordGroupId, readName, contig, start, primaryAlignment, readPaired, readMapped)

    val engine = new ComparisonTraversalEngine(fields, a, b)(sc)

    val generator = MappedPosition

    val generated: RDD[(String, Seq[Long])] = engine.generate(generator)

    val genMap = Map(generated.collect().map { case (key, value) => (key.toString, value) }: _*)

    assert(genMap.size === 2)
    assert(genMap.contains("read0"))
    assert(genMap.contains("read1"))
    assert(genMap("read0") === Seq(5))
    assert(genMap("read1") === Seq(0))
  }

  sparkTest("combine works on a simple RDD") {
    val c0 = Contig.newBuilder
      .setContigName("chr0")
      .build

    val a0 = AlignmentRecord.newBuilder()
      .setRecordGroupName("group0")
      .setReadName("read0")
      .setContig(c0)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setReadPaired(false)
      .setReadMapped(true)
      .build()
    val a1 = AlignmentRecord.newBuilder(a0)
      .setReadName("read1")
      .setStart(200)
      .build()
    val a2 = AlignmentRecord.newBuilder(a0)
      .setReadName("read2")
      .setStart(300)
      .build()

    val b0 = AlignmentRecord.newBuilder(a0).build()
    val b1 = AlignmentRecord.newBuilder(a1).build()
    val b2 = AlignmentRecord.newBuilder(a2).setStart(305).build()

    val a = sc.parallelize(Seq(a0, a1, a2))
    val b = sc.parallelize(Seq(b0, b1, b2))

    import AlignmentRecordField._
    val fields = Seq(recordGroupId, readName, contig, start, primaryAlignment, readPaired, readMapped)

    val engine = new ComparisonTraversalEngine(fields, a, b)(sc)

    val generator = MappedPosition
    val aggregator = new HistogramAggregator[Long]()

    val aggregated: Histogram[Long] =
      ComparisonTraversalEngine.combine(engine.generate(generator), aggregator)

    assert(aggregated.count() === 3)
    assert(aggregated.countIdentical() === 2)

    assert(aggregated.valueToCount(0L) === 2)
    assert(aggregated.valueToCount(5L) === 1)
  }

}
