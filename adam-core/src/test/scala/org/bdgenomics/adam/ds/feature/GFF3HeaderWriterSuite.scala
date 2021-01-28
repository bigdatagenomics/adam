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
package org.bdgenomics.adam.ds.feature

import org.bdgenomics.adam.util.ADAMFunSuite
import scala.io.Source

class GFF3HeaderWriterSuite extends ADAMFunSuite {

  sparkTest("write gff3 header pragma") {
    val tmp = tmpFile(".gff3")
    GFF3HeaderWriter(tmp, sc)
    val lines = Source.fromFile(tmp)
      .getLines
      .toSeq
    assert(lines.size === 1)
    assert(lines.head === GFF3HeaderWriter.HEADER_STRING)
  }
}
