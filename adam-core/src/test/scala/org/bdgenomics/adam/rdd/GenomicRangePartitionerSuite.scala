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
package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.models.ReferenceRegion
import org.scalatest.FunSuite

class GenomicRangePartitionerSuite extends FunSuite {

  test("check for lexicographic sort order on lex data") {
    val lexData = Seq("a", "b", "c", "c", "d").map(name => {
      ReferenceRegion(name, 0L, 1L)
    }).toArray
    assert(GenomicRangePartitioner.isLexSorted(lexData))
  }

  test("check for lexicographic sort order on non-lex sorted data") {
    val nonLexData = Seq("a", "c", "d", "c", "b").map(name => {
      ReferenceRegion(name, 0L, 1L)
    }).toArray
    assert(!GenomicRangePartitioner.isLexSorted(nonLexData))
  }
}
