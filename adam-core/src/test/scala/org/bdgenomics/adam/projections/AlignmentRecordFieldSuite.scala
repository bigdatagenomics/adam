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
package org.bdgenomics.adam.projections

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.AlignmentRecordField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord

class AlignmentRecordFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet alignment records") {
    val path = tmpFile("alignmentRecords.parquet")
    val rdd = sc.parallelize(Seq(AlignmentRecord.newBuilder()
      .setContigName("6")
      .setStart(29941260L)
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      readInFragment,
      contigName,
      start,
      oldPosition,
      end,
      mapq,
      readName,
      sequence,
      qual,
      cigar,
      oldCigar,
      basesTrimmedFromStart,
      basesTrimmedFromEnd,
      readPaired,
      properPair,
      readMapped,
      mateMapped,
      failedVendorQualityChecks,
      duplicateRead,
      readNegativeStrand,
      mateNegativeStrand,
      primaryAlignment,
      secondaryAlignment,
      supplementaryAlignment,
      mismatchingPositions,
      origQual,
      attributes,
      recordGroupName,
      recordGroupSample,
      mateAlignmentStart,
      mateAlignmentEnd,
      mateContigName,
      inferredInsertSize
    )

    val alignmentRecords: RDD[AlignmentRecord] = sc.loadParquet(path, projection = Some(projection))
    assert(alignmentRecords.count() === 1)
    assert(alignmentRecords.first.getContigName === "6")
    assert(alignmentRecords.first.getStart === 29941260L)
  }
}
