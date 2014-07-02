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
package org.bdgenomics.adam.rdd.correction

import org.bdgenomics.formats.avro.ADAMRecord
import org.scalatest.FunSuite
import scala.math.abs

class ErrorCorectionSuite extends FunSuite {

  val ec = new ErrorCorrection

  def fpCompare(a: Double, b: Double, epsilon: Double = 1e-3): Boolean = abs(a - b) < epsilon

  test("cut a short read into qmers") {
    val read = ADAMRecord.newBuilder
      .setSequence("ACTCATG")
      .setQual("??;957:")
      .build()

    val qmers = ec.readToQmers(read, 3).toMap

    // check that the qmer count is correct
    assert(qmers.size === 5)

    // find our qmers and check their values
    assert(fpCompare(qmers("ACT"), 0.995))
    assert(fpCompare(qmers("CTC"), 0.992))
    assert(fpCompare(qmers("TCA"), 0.983))
    assert(fpCompare(qmers("CAT"), 0.979))
    assert(fpCompare(qmers("ATG"), 0.980))
  }
}
