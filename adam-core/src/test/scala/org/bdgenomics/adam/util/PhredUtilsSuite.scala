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

class PhredUtilsSuite extends FunSuite {

  test("convert low phred score to log and back") {
    val logP = PhredUtils.phredToLogProbability(10)
    val phred = PhredUtils.logProbabilityToPhred(logP)
    assert(phred === 10)
  }

  test("convert high phred score to log and back") {
    val logP = PhredUtils.phredToLogProbability(1000)
    val phred = PhredUtils.logProbabilityToPhred(logP)
    assert(phred === 1000)
  }

  test("convert overflowing phred score to log and back and clip") {
    val logP = PhredUtils.phredToLogProbability(10000)
    val phred = PhredUtils.logProbabilityToPhred(logP)
    assert(phred === 3233)
  }

  test("convert negative zero log probability to phred and clip") {
    val phred = PhredUtils.logProbabilityToPhred(-0.0)
    assert(phred === 3233)
  }

  test("round trip log probabilities") {
    def roundTrip(i: Int): Int = {
      PhredUtils.logProbabilityToPhred(PhredUtils.phredToLogProbability(i))
    }

    (0 to 3228).foreach(i => {
      assert(i === roundTrip(i))
    })

    // there is roundoff above 3228 due to floating point underflow
    assert(3228 === roundTrip(3229))
    assert(3230 === roundTrip(3230))
    assert(3230 === roundTrip(3231))
    assert(3233 === roundTrip(3232))
    assert(3233 === roundTrip(3233))
  }
}
