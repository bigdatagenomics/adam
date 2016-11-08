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

import java.io.File
import java.util.logging.Level
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.{ ParquetLogger, ADAMFunSuite }
import org.bdgenomics.formats.avro.AlignmentRecord

class FieldEnumerationSuite extends ADAMFunSuite {

  test("Empty projections are illegal") {
    intercept[IllegalArgumentException] {
      Projection()
    }
  }

  sparkTest("Simple projection on Read works") {
    val readsFilepath = testFile("reads12.sam")
    val readsParquetFilepath = tmpFile("reads12.adam")

    // Convert the reads12.sam file into a parquet file
    val rRdd = sc.loadBam(readsFilepath)
    rRdd.saveAsParquet(TestSaveArgs(readsParquetFilepath))

    val p1 = Projection(AlignmentRecordField.readName)
    val reads1: RDD[AlignmentRecord] = sc.loadAlignments(readsParquetFilepath, projection = Some(p1)).rdd
    assert(reads1.count() === 200)

    val first1 = reads1.first()
    assert(first1.getReadName.toString === "simread:1:26472783:false")
    assert(first1.getReadMapped === false)

    val p2 = Projection(AlignmentRecordField.readName, AlignmentRecordField.readMapped)
    val reads2: RDD[AlignmentRecord] = sc.loadAlignments(readsParquetFilepath, projection = Some(p2)).rdd
    assert(reads2.count() === 200)

    val first2 = reads2.first()
    assert(first2.getReadName.toString === "simread:1:26472783:false")
    assert(first2.getReadMapped === true)
  }
}
