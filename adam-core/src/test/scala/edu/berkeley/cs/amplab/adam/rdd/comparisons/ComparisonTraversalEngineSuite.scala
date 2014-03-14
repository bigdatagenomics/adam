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
package edu.berkeley.cs.amplab.adam.rdd.comparisons

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.metrics.MappedPosition
import edu.berkeley.cs.amplab.adam.metrics.aggregators.HistogramAggregator
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField
import edu.berkeley.cs.amplab.adam.util.{ Histogram, SparkFunSuite }
import org.apache.spark.rdd.RDD

class ComparisonTraversalEngineSuite extends SparkFunSuite {

  sparkTest("generate works on a simple RDD") {

    val a0 = ADAMRecord.newBuilder()
      .setRecordGroupName("group0")
      .setReadName("read0")
      .setReferenceId(0)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setReadPaired(false)
      .setReadMapped(true)
      .build()
    val a1 = ADAMRecord.newBuilder(a0)
      .setReadName("read1")
      .setStart(200)
      .build()

    val b0 = ADAMRecord.newBuilder(a0)
      .setStart(105)
      .build()
    val b1 = ADAMRecord.newBuilder(a1).build()

    val a = sc.parallelize(Seq(a0, a1))
    val b = sc.parallelize(Seq(b0, b1))

    import ADAMRecordField._
    val fields = Seq(recordGroupId, readName, referenceId, start, primaryAlignment, readPaired, readMapped)

    val engine = new ComparisonTraversalEngine(fields, a, b)(sc)

    val generator = MappedPosition

    val generated: RDD[(CharSequence, Seq[Long])] = engine.generate(generator)

    val genMap = Map(generated.collect().map { case (key, value) => (key.toString, value) }: _*)

    assert(genMap.size === 2)
    assert(genMap.contains("read0"))
    assert(genMap.contains("read1"))
    assert(genMap("read0") === Seq(5))
    assert(genMap("read1") === Seq(0))
  }

  sparkTest("combine works on a simple RDD") {
    val a0 = ADAMRecord.newBuilder()
      .setRecordGroupName("group0")
      .setReadName("read0")
      .setReferenceId(0)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setReadPaired(false)
      .setReadMapped(true)
      .build()
    val a1 = ADAMRecord.newBuilder(a0)
      .setReadName("read1")
      .setStart(200)
      .build()
    val a2 = ADAMRecord.newBuilder(a0)
      .setReadName("read2")
      .setStart(300)
      .build()

    val b0 = ADAMRecord.newBuilder(a0).build()
    val b1 = ADAMRecord.newBuilder(a1).build()
    val b2 = ADAMRecord.newBuilder(a2).setStart(305).build()

    val a = sc.parallelize(Seq(a0, a1, a2))
    val b = sc.parallelize(Seq(b0, b1, b2))

    import ADAMRecordField._
    val fields = Seq(recordGroupId, readName, referenceId, start, primaryAlignment, readPaired, readMapped)

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
