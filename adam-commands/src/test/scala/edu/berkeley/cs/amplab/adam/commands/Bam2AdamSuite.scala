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
package edu.berkeley.cs.amplab.adam.commands

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import org.apache.spark.rdd.RDD

class Bam2AdamSuite extends SparkFunSuite {
  sparkTest("Bam2Adam should not fail on unmapped reads") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("unmapped.sam").getFile

    // Convert the reads12.sam file into a parquet file
    val bamReads : RDD[ADAMRecord] = sc.adamLoad(readsFilepath)
    assert (bamReads.count === 200)
  }
}
