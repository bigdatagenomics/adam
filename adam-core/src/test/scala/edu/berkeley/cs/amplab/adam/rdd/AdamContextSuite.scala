/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import parquet.filter.UnboundRecordFilter
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.util.PhredUtils._

class AdamContextSuite extends SparkFunSuite {

  sparkTest("sc.adamLoad should not fail on unmapped reads") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("unmapped.sam").getFile

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[ADAMRecord] = sc.adamLoad(readsFilepath)
    assert(bamReads.count === 200)
  }

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads: RDD[ADAMRecord] = sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
    assert(reads.count() === 20)
  }

  sparkTest("can read a small .VCF file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.vcf").getFile
    val variants: RDD[ADAMVariantContext] = sc.adamVariantContextLoad(path)
    assert(variants.count() === 4)
  }

  test("Can convert to phred") {
    assert(successProbabilityToPhred(0.9) === 10)
    assert(successProbabilityToPhred(0.99999) === 50)
  }

  test("Can convert from phred") {
    // result is floating point, so apply tolerance
    assert(phredToSuccessProbability(10) > 0.89 && phredToSuccessProbability(10) < 0.91)
    assert(phredToSuccessProbability(50) > 0.99998 && phredToSuccessProbability(50) < 0.999999)
  }

}

