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

import org.bdgenomics.utils.io.LocalFileByteAccess
import org.bdgenomics.adam.models.ReferenceRegion
import org.scalatest.FunSuite

class TwoBitSuite extends FunSuite {
  test("correctly read sequence from .2bit file") {
    val file = new File(ClassLoader.getSystemClassLoader.getResource("hg19.chrM.2bit").getFile)
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    assert(twoBitFile.numSeq == 1)
    assert(twoBitFile.seqRecords.toSeq.length == 1)
    assert(twoBitFile.extract(ReferenceRegion("hg19_chrM", 0, 10)) == "GATCACAGGT")
    assert(twoBitFile.extract(ReferenceRegion("hg19_chrM", 503, 513)) == "CATCCTACCC")
    assert(twoBitFile.extract(ReferenceRegion("hg19_chrM", 16561, 16571)) == "CATCACGATG")
  }
}
