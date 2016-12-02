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
package org.bdgenomics.adam.cli

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite

class Fasta2ADAMSuite extends ADAMFunSuite {
  sparkTest("can load fasta records after conversion") {
    val inputPath = copyResource("chr20.250k.fa.gz")
    val convertPath = tmpFile("chr20.contig.adam")
    val cmd = Fasta2ADAM(Array(inputPath, convertPath)).run(sc)

    val contigFragments = sc.loadParquetContigFragments(convertPath)
    assert(contigFragments.rdd.count() === 26)
    val first = contigFragments.rdd.first()
    assert(first.getContig.getContigName === null)
    assert(first.getDescription === "gi|224384749|gb|CM000682.1| Homo sapiens chromosome 20, GRCh37 primary reference assembly")
    assert(first.getFragmentNumber === 0)
    assert(first.getFragmentSequence.length === 10000)
    assert(first.getFragmentStartPosition === 0L)
    assert(first.getFragmentEndPosition === 9999L)
    assert(first.getNumberOfFragmentsInContig === 26)
  }
}
