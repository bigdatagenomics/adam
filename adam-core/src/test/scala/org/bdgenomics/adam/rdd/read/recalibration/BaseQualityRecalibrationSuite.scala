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
package org.bdgenomics.adam.rdd.read.recalibration

import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.SnpTable
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.io.Source

class BaseQualityRecalibrationSuite extends ADAMFunSuite {

  def testBqsr(optSl: Option[StorageLevel]) {
    val readsFilepath = testFile("bqsr1.sam")
    val snpsFilepath = testFile("bqsr1.vcf")
    val obsFilepath = testFile("bqsr1-ref.observed")

    val rdd = sc.loadAlignments(readsFilepath)
    val reads: RDD[AlignmentRecord] = rdd.rdd
    val variants = sc.loadVariants(snpsFilepath)
    val snps = sc.broadcast(SnpTable(variants))

    val bqsr = new BaseQualityRecalibration(reads,
      snps,
      rdd.readGroups,
      optStorageLevel = optSl)

    // Sanity checks
    assert(bqsr.result.count == reads.count)

    // Compare the ObservationTables
    val referenceObs: Seq[String] = Source.fromFile(new File(obsFilepath))
      .getLines()
      .filter(_.length > 0)
      .toSeq
      .sortWith((kv1, kv2) => kv1.compare(kv2) < 0)
    val testObs: Seq[String] = bqsr.observed
      .toCSV(rdd.readGroups)
      .split('\n')
      .filter(_.length > 0)
      .toSeq
      .sortWith((kv1, kv2) => kv1.compare(kv2) < 0)
    referenceObs.zip(testObs).foreach(p => assert(p._1 === p._2))
  }

  sparkTest("BQSR Test Input #1 w/ VCF Sites without caching") {
    testBqsr(None)
  }

  sparkTest("BQSR Test Input #1 w/ VCF Sites with caching") {
    testBqsr(Some(StorageLevel.MEMORY_ONLY))
  }

  sparkTest("BQSR Test Input #1 w/ VCF Sites with serialized caching") {
    testBqsr(Some(StorageLevel.MEMORY_ONLY))
  }
}
