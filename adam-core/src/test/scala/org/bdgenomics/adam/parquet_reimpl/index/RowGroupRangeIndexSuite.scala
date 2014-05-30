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
package org.bdgenomics.adam.parquet_reimpl.index

import java.io.File
import org.scalatest.FunSuite

class RowGroupRangeIndexSuite extends FunSuite {

  test("Can read simple test range index") {
    val filename = getClass.getClassLoader.getResource("test_rowgroup_rangeindex.1.txt").getFile
    val file = new File(filename)

    val rangeIndex = new RangeIndex(file)

    assert(rangeIndex.entries.size === 1)
    assert(rangeIndex.entries.head.ranges.size === 2)
  }
}
