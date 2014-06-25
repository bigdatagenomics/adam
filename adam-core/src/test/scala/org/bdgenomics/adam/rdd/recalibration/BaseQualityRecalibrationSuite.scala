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
package org.bdgenomics.adam.rdd.recalibration

import org.bdgenomics.adam.models.SnpTable
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import org.bdgenomics.adam.rich.DecadentRead._
import org.bdgenomics.adam.util.SparkFunSuite
import org.apache.spark.rdd.RDD
import java.io.File
import org.bdgenomics.adam.rich.RichADAMVariant

class BaseQualityRecalibrationSuite extends SparkFunSuite {
  sparkTest("BQSR Test Input #1") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("bqsr1.sam").getFile
    val snpsFilepath = ClassLoader.getSystemClassLoader.getResource("bqsr1.snps").getFile
    val obsFilepath = ClassLoader.getSystemClassLoader.getResource("bqsr1-ref.observed").getFile

    val reads: RDD[ADAMRecord] = sc.adamLoad(readsFilepath)
    val snps = sc.broadcast(SnpTable(new File(snpsFilepath)))

    val bqsr = new BaseQualityRecalibration(cloy(reads), snps)

    // Sanity checks
    assert(bqsr.result.count == reads.count)

    // Compare the ObservatonTables
    val referenceObs: Set[String] = scala.io.Source.fromFile(new File(obsFilepath)).getLines.filter(_.length > 0).toSet
    val testObs: Set[String] = bqsr.observed.toCSV.split('\n').filter(_.length > 0).toSet
    assert(testObs == referenceObs)

    // TODO: Compare `result` against the reference output
  }

  sparkTest("BQSR Test Input #1 w/ VCF Sites") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("bqsr1.sam").getFile
    val snpsFilepath = ClassLoader.getSystemClassLoader.getResource("bqsr1.vcf").getFile
    val obsFilepath = ClassLoader.getSystemClassLoader.getResource("bqsr1-ref.observed").getFile

    val reads: RDD[ADAMRecord] = sc.adamLoad(readsFilepath)
    val variants: RDD[RichADAMVariant] = sc.adamVCFLoad(snpsFilepath).map(_.variant)
    val snps = sc.broadcast(SnpTable(variants))

    val bqsr = new BaseQualityRecalibration(cloy(reads), snps)

    // Sanity checks
    assert(bqsr.result.count == reads.count)

    // Compare the ObservatonTables
    val referenceObs: Seq[String] = scala.io.Source.fromFile(new File(obsFilepath)).getLines.filter(_.length > 0).toSeq.sortWith((kv1, kv2) => kv1.compare(kv2) < 0)
    val testObs: Seq[String] = bqsr.observed.toCSV.split('\n').filter(_.length > 0).toSeq.sortWith((kv1, kv2) => kv1.compare(kv2) < 0)
    referenceObs.zip(testObs).foreach(p => assert(p._1 === p._2))
  }
}
