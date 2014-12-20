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
package org.bdgenomics.adam.metrics

import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
import org.bdgenomics.adam.models.SingleReadBucket
import org.bdgenomics.adam.instrumentation.Metrics

class ComparisonsSuite extends SparkFunSuite {
  var bucket: SingleReadBucket = null
  var bucketMapq: SingleReadBucket = null
  var bucketMapqUnset: SingleReadBucket = null
  var bucketDuplicate: SingleReadBucket = null
  var bucketQual: SingleReadBucket = null
  var bucketQualUnset: SingleReadBucket = null
  var bucketMovedChromosome: SingleReadBucket = null
  var bucketMovedStart: SingleReadBucket = null

  sparkBefore("Generators setup") {
    def srb(record: AlignmentRecord): SingleReadBucket = {
      val seq = Seq(record)
      val rdd = sc.makeRDD(seq)
      val srbRDD = SingleReadBucket(rdd)
      srbRDD.first()
    }

    val contig: Contig = Contig.newBuilder
      .setContigName("chr1")
      .build
    val contig2: Contig = Contig.newBuilder
      .setContigName("chr2")
      .build
    val record: AlignmentRecord = AlignmentRecord.newBuilder()
      .setContig(contig)
      .setReadName("test")
      .setDuplicateRead(false)
      .setMapq(10)
      .setQual("abcdef")
      .setStart(100)
      .setPrimaryAlignment(true)
      .setRecordGroupName("groupid")
      .setReadMapped(true)
      .build()

    bucket = srb(record)

    bucketMapq = srb(AlignmentRecord.newBuilder(record)
      .setMapq(11)
      .build())

    bucketMapqUnset = srb(AlignmentRecord.newBuilder()
      .setContig(contig)
      .setReadName("test")
      .setDuplicateRead(false)
      .setQual("abcdef")
      .setStart(100)
      .setPrimaryAlignment(true)
      .setRecordGroupName("groupid")
      .setReadMapped(true)
      .build())

    bucketDuplicate = srb(AlignmentRecord.newBuilder(record)
      .setDuplicateRead(true)
      .build())

    bucketQual = srb(AlignmentRecord.newBuilder(record)
      .setQual("fedcba")
      .build())

    bucketQualUnset = srb(AlignmentRecord.newBuilder()
      .setContig(contig)
      .setReadName("test")
      .setDuplicateRead(false)
      .setMapq(10)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setRecordGroupName("groupid")
      .setReadMapped(true)
      .build())

    bucketMovedChromosome = srb(AlignmentRecord.newBuilder(record)
      .setContig(contig2)
      .setStart(200)
      .build())

    bucketMovedStart = srb(AlignmentRecord.newBuilder(record)
      .setStart(200)
      .build())
  }

  sparkTest("Dupe mismatches found") {
    assert(DupeMismatch.matchedByName(bucket, bucket) === Seq((0, 0)))
    assert(DupeMismatch.matchedByName(bucket, bucketDuplicate) === Seq((0, 1)))
  }

  sparkTest("Mismatched mapped positions histogram generated") {
    import org.bdgenomics.adam.util.Histogram

    assert(Histogram(MappedPosition.matchedByName(bucket, bucket)).valueToCount(0) === 1)
    assert(Histogram(MappedPosition.matchedByName(bucket, bucketMovedChromosome)).valueToCount.get(0).isEmpty)
    assert(Histogram(MappedPosition.matchedByName(bucket, bucketMovedChromosome)).valueToCount(-1) === 1)
    assert(Histogram(MappedPosition.matchedByName(bucket, bucketMovedStart)).valueToCount.get(0).isEmpty)
    assert(Histogram(MappedPosition.matchedByName(bucket, bucketMovedStart)).valueToCount(100) === 1)
  }

  sparkTest("Test map quality scores") {
    assert(MapQualityScores.matchedByName(bucket, bucket).contains((10, 10)))
    assert({
      val points = MapQualityScores.matchedByName(bucket, bucketMapq)
      points.contains((10, 11)) || points.contains((11, 10))
    })
  }

  sparkTest("Test base quality scores") {
    assert(BaseQualityScores.matchedByName(bucket, bucket).forall(a => a._1 == a._2))
  }
}
