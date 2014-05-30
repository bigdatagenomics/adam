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

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.ADAMFlatGenotype
import org.scalatest.FunSuite
import ReferenceFoldingContext._

class RangeIndexGeneratorSuite extends FunSuite {

  test("small_adam_A.fgenotype is indexed into one RangeIndexEntry") {

    val generator = new RangeIndexGenerator[ADAMFlatGenotype]

    val parquetPath = Thread.currentThread().getContextClassLoader.getResource("small_adam.fgenotype").getFile
    val entrySeq = generator.addParquetFile(parquetPath).toSeq

    assert(entrySeq.size === 1)

    entrySeq.head match {
      case RangeIndexEntry(path, index, ranges) =>
        assert(path === "small_adam.fgenotype")
        assert(index === 0)
        assert(ranges.size === 3)
        ranges.head match {
          case ReferenceRegion(refName, start, end) =>
            assert(refName === "chr1")
            assert(start === 14397)
            assert(end === 19191)
        }
    }

  }
}
