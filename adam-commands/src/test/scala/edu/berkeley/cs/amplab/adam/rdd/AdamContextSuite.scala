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
package edu.berkeley.cs.amplab.adam.rdd

import parquet.filter.UnboundRecordFilter
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._

class AdamContextSuite extends SparkFunSuite {

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads : RDD[ADAMRecord] = sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
    assert( reads.count() === 20 )
  }
}


