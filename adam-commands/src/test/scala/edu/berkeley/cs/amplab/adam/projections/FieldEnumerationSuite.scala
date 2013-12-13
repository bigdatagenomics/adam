/**
 * Copyright 2013 Genome Bridge LLC
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
package edu.berkeley.cs.amplab.adam.projections

import edu.berkeley.cs.amplab.adam.util.{ParquetLogger, SparkFunSuite}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.{ADAMVariant, ADAMRecord}
import org.apache.spark.rdd.RDD
import java.io.File
import org.scalatest.BeforeAndAfter
import java.util.logging.Level

class FieldEnumerationSuite extends SparkFunSuite with BeforeAndAfter {

  var readsFilepath : String = null
  var readsParquetFile : File = null
  var variantsParquetFile : File = null

  sparkBefore("fieldenumerationsuite_before") {
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    readsFilepath = ClassLoader.getSystemClassLoader.getResource("reads12.sam").getFile
    val file = new File(readsFilepath)

    readsParquetFile = new File(file.getParentFile, "test_reads12_parquet")
    variantsParquetFile = new File(file.getParentFile, "test_variants_parquet")

    // Erase old test files, if they exist.
    if(readsParquetFile.exists())
      cleanParquet(readsParquetFile)

    if(variantsParquetFile.exists())
      cleanParquet(variantsParquetFile)

    // Convert the reads12.sam file into a parquet file
    val bamReads : RDD[ADAMRecord] = sc.adamLoad(readsFilepath)
    bamReads.adamSave(readsParquetFile.getAbsolutePath)

    // Since we don't have a ready-made test variants file to convert, we hand-create our
    // own set of ADAMVariant object(s) and drop them into the corresponding parquet file.
    val creation : RDD[ADAMVariant] = sc.parallelize(List(
      ADAMVariant.newBuilder()
        .setReferenceId(0)
        .setReferenceName("1")
        .setPosition(1000)
        .setAlleleFrequency(0.5)
        .build()))

    creation.adamSave(variantsParquetFile.getAbsolutePath)
  }

  after {
    cleanParquet(readsParquetFile)
    cleanParquet(variantsParquetFile)
  }

  /**
   * We can't just file.delete() a parquet "file", since it's often a directory.
   * @param dir The directory (or, possibly, file) to delete
   */
  def cleanParquet(dir : File) {
    if(dir.isDirectory) {
      dir.listFiles().foreach(file =>
        file.delete()
      )
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

    val reads1 : RDD[ADAMRecord] = sc.adamLoad(readsParquetFile.getAbsolutePath, projection=Some(p1))

    assert(reads1.count() === 200)

    val first1 = reads1.first()
    assert(first1.getReadName === "simread:1:26472783:false")
    assert(first1.getReadMapped === null)

    val p2 = Projection(ADAMRecordField.readName, ADAMRecordField.readMapped)

    val reads2 : RDD[ADAMRecord] = sc.adamLoad(readsParquetFile.getAbsolutePath, projection=Some(p2))

    assert(reads2.count() === 200)

    val first2 = reads2.first()
    assert(first2.getReadName === "simread:1:26472783:false")
    assert(first2.getReadMapped === true)
  }

  sparkTest("Simple projection on ADAMVariant works") {

    val p1 = Projection(ADAMVariantField.referenceId, ADAMVariantField.referenceName, ADAMVariantField.position)

    var rdd : RDD[ADAMVariant] = sc.adamLoad(variantsParquetFile.getAbsolutePath, projection=Some(p1))
    var first = rdd.first()

    assert(first.getReferenceId === 0)
    assert(first.getReferenceName === "1")
    assert(first.getPosition === 1000)
    assert(first.getAlleleFrequency === null)

    val p2 = Projection(ADAMVariantField.referenceId, ADAMVariantField.referenceName, ADAMVariantField.position,
      ADAMVariantField.alleleFrequency)

    rdd = sc.adamLoad(variantsParquetFile.getAbsolutePath, projection=Some(p2))
    first = rdd.first()

    assert(first.getReferenceId === 0)
    assert(first.getReferenceName === "1")
    assert(first.getPosition === 1000)
    assert(first.getAlleleFrequency === 0.5)
  }
}
