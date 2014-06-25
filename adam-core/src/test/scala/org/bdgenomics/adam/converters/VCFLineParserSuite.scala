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
package org.bdgenomics.adam.converters

import org.scalatest.FunSuite
import org.bdgenomics.formats.avro.ADAMFlatGenotype

class VCFLineParserSuite extends FunSuite {

  test("can parse a simple VCF") {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream("small.vcf")
    val parser = new VCFLineParser(is)
    assert(parser.size === 5)
  }

  test("passing in the optional sampleSet parameter results in fewer columns being parsed") {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream("small.vcf")
    val parser = new VCFLineParser(is, Some(Set("NA12878")))

    parser.foreach {
      case vcfLine: VCFLine =>
        assert(vcfLine.samples === Array("NA12878"))
    }
  }

  test("can convert into ADMAFlatGenotype records") {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream("small.vcf")
    val parser = new VCFLineParser(is)
    val gts = parser.flatMap(VCFLineConverter.convert)
    assert(gts.size === 15)
  }

  test("passing in a sample subsets results in fewer ADAMFlatGenotype records being produced") {
    //NA12878 is artificially missing a call, in "small_missing.vcf"
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream("small_missing.vcf")
    val parser = new VCFLineParser(is, Some(Set("NA12878")))
    val gts = parser.flatMap(VCFLineConverter.convert).toSeq

    assert(gts.length === 4)
    gts.foreach {
      case gt: ADAMFlatGenotype => {
        assert(gt.getReferenceName === "1")
        assert(gt.getSampleId === "NA12878")
      }
    }
  }

}
