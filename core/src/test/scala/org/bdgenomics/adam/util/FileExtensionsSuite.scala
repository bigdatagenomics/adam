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

import org.scalatest.FunSuite

class FileExtensionsSuite extends FunSuite {

  test("ends in gzip extension") {
    assert(FileExtensions.isGzip("file.vcf.gz"))
    assert(FileExtensions.isGzip("file.fastq.bgz"))
    assert(!FileExtensions.isGzip("file.fastq.bgzf"))
    assert(!FileExtensions.isGzip("file.vcf"))
    assert(!FileExtensions.isGzip("file.fastq"))
  }

  test("is a vcf extension") {
    assert(FileExtensions.isVcfExt("file.vcf"))
    assert(FileExtensions.isVcfExt("file.vcf.bgz"))
    assert(!FileExtensions.isVcfExt("file.bcf"))
    assert(FileExtensions.isVcfExt("file.vcf.gz"))
    assert(!FileExtensions.isVcfExt("file.vcf.bgzf"))
  }
}
