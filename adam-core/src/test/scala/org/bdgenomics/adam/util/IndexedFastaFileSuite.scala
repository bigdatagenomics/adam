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

import htsjdk.samtools.{ ValidationStringency, SAMException }
import org.bdgenomics.adam.models.ReferenceRegion

class IndexedFastaFileSuite extends ADAMFunSuite {

  val filePath = resourceUrl("HLA_DQB1_05_01_01_02.fa").getPath

  sparkTest("correctly generates reference sequence dictionary from .dict file") {
    val indexedFasta = IndexedFastaFile(sc, filePath)
    assert(indexedFasta.references.records.length == 1)
  }

  sparkTest("correctly gets sequence") {
    val indexedFasta = IndexedFastaFile(sc, filePath)
    val region = ReferenceRegion("HLA-DQB1*05:01:01:02", 1, 50)
    val sequence = indexedFasta.extract(region)
    assert(sequence == "TTCTAAGACCTTTGCTCTTCTCCCCAGGACTTAAGGCTCTTCAGCGTGTC")
  }

  sparkTest("fails when fai index is not provided") {
    val pathWithoutIndex = resourceUrl("hs38DH_chr1_10.fa").getPath

    try {
      IndexedFastaFile(sc, pathWithoutIndex)
      assert(false)
    } catch {
      case s: SAMException => assert(true)
      case e: Throwable    => assert(false)
    }
  }

  sparkTest("passes when dict is not provided and ValidationStringency = LENIENT") {
    val pathWithoutDict = resourceUrl("artificial.fa").getPath

    try {
      IndexedFastaFile(sc, pathWithoutDict, ValidationStringency.LENIENT)
      assert(true)
    } catch {
      case s: SAMException => assert(false)
      case e: Exception    => assert(false)
    }
  }
}

