/*
 * Copyright (c) 2014 The Regents of the University of California
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

package edu.berkeley.cs.amplab.adam.rdd.recalibration

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.SnpTable
import edu.berkeley.cs.amplab.adam.rdd.ADAMContext._
import edu.berkeley.cs.amplab.adam.rich.DecadentRead._
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import java.io.File
import org.apache.spark.rdd.RDD

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
}
