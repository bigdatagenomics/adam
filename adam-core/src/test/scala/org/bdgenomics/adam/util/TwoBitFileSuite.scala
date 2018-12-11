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
package org.bdgenomics.adam.util

import java.io.File
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.utils.io.LocalFileByteAccess

class TwoBitFileSuite extends ADAMFunSuite {
  test("correctly read sequence from .2bit file") {
    val file = new File(testFile("hg19.chrM.2bit"))
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    assert(twoBitFile.numSeq == 1)
    assert(twoBitFile.seqRecords.size == 1)
    assert(twoBitFile.extract(ReferenceRegion("hg19_chrM", 0, 10)) == "GATCACAGGT")
    assert(twoBitFile.extract(ReferenceRegion("hg19_chrM", 503, 513)) == "CATCCTACCC")
    assert(twoBitFile.extract(ReferenceRegion("hg19_chrM", 16561, 16571)) == "CATCACGATG")
  }

  test("correctly return masked sequences from .2bit file") {
    val file = new File(testFile("hg19.chrM.2bit"))
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    assert(twoBitFile.extract(ReferenceRegion("hg19_chrM", 0, 10), true) == "GATCACAGGT")
    assert(twoBitFile.extract(ReferenceRegion("hg19_chrM", 2600, 2610), true) == "taatcacttg")
  }

  test("correctly return Ns from .2bit file") {
    val file = new File(testFile("human_g1k_v37_chr1_59kb.2bit"))
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    assert(twoBitFile.extract(ReferenceRegion("1", 9990, 10010), true) == "NNNNNNNNNNTAACCCTAAC")
  }

  test("correctly calculates sequence dictionary") {
    val file = new File(testFile("hg19.chrM.2bit"))
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    val dict = twoBitFile.sequences
    assert(dict.records.length == 1)
    assert(dict.records.head.length == 16571)
    assert(dict.records.head.index.isDefined)
    assert(dict.records.head.index.get === 0)
  }
}
