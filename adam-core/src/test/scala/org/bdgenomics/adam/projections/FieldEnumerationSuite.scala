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

import org.bdgenomics.adam.util.{ ParquetLogger, SparkFunSuite }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.ADAMRecord
import org.apache.spark.rdd.RDD
import java.io.File
import org.scalatest.BeforeAndAfter
import java.util.logging.Level

class FieldEnumerationSuite extends SparkFunSuite with BeforeAndAfter {

  var readsFilepath: String = null
  var readsParquetFile: File = null

  sparkBefore("fieldenumerationsuite_before") {
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    readsFilepath = ClassLoader.getSystemClassLoader.getResource("reads12.sam").getFile
    val file = new File(readsFilepath)

    readsParquetFile = new File(file.getParentFile, "test_reads12_parquet")

    // Erase old test files, if they exist.
    if (readsParquetFile.exists())
      cleanParquet(readsParquetFile)

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[ADAMRecord] = sc.adamLoad(readsFilepath)
    bamReads.adamSave(readsParquetFile.getAbsolutePath)
  }

  after {
    cleanParquet(readsParquetFile)
  }

  /**
   * We can't just file.delete() a parquet "file", since it's often a directory.
   * @param dir The directory (or, possibly, file) to delete
   */
  def cleanParquet(dir: File) {
    if (dir.isDirectory) {
      dir.listFiles().foreach(file =>
        file.delete())
      dir.delete()
    } else {
      dir.delete()
    }
  }

  test("Empty projections are illegal") {
    intercept[AssertionError] {
      Projection()
    }
  }

  sparkTest("Simple projection on ADAMRecord works") {

    val p1 = Projection(ADAMRecordField.readName)

    val reads1: RDD[ADAMRecord] = sc.adamLoad(readsParquetFile.getAbsolutePath, projection = Some(p1))

    assert(reads1.count() === 200)

    val first1 = reads1.first()
    assert(first1.getReadName === "simread:1:26472783:false")
    assert(first1.getReadMapped === false)

    val p2 = Projection(ADAMRecordField.readName, ADAMRecordField.readMapped)

    val reads2: RDD[ADAMRecord] = sc.adamLoad(readsParquetFile.getAbsolutePath, projection = Some(p2))

    assert(reads2.count() === 200)

    val first2 = reads2.first()
    assert(first2.getReadName === "simread:1:26472783:false")
    assert(first2.getReadMapped === true)
  }
}
