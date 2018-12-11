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
package org.bdgenomics.adam.models

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Feature

class CoverageSuite extends ADAMFunSuite {
  sparkTest("Convert to coverage from valid Feature") {
    // create a valid feature
    val featureToConvert =
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(1)
        .setEnd(2)
        .setScore(100)
        .build()

    val coverageAfterConversion = Coverage(featureToConvert)

    // all fields should match between the two
    assert(coverageAfterConversion.start == featureToConvert.start)
    assert(coverageAfterConversion.end == featureToConvert.end)
    assert(coverageAfterConversion.referenceName == featureToConvert.getReferenceName)
    assert(coverageAfterConversion.count == featureToConvert.score)
  }

  sparkTest("Convert to coverage from valid Feature with sampleId") {
    // create a valid feature
    val featureToConvert =
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(1)
        .setEnd(2)
        .setScore(100)
        .setSampleId("sample")
        .build()

    val coverageAfterConversion = Coverage(featureToConvert)

    // all fields should match between the two
    assert(coverageAfterConversion.start == featureToConvert.start)
    assert(coverageAfterConversion.end == featureToConvert.end)
    assert(coverageAfterConversion.referenceName == featureToConvert.getReferenceName)
    assert(coverageAfterConversion.count == featureToConvert.score)
    assert(coverageAfterConversion.optSampleId == Some(featureToConvert.sampleId))
  }

  sparkTest("Convert to coverage from Feature with null/empty contigName fails with correct error") {
    // feature with empty contigname is not valid when converting to Coverage
    val featureWithEmptyContigName =
      Feature.newBuilder()
        .setReferenceName("")
        .setStart(1)
        .setEnd(2)
        .setScore(100)
        .build()

    val caughtWithEmptyContigName =
      intercept[IllegalArgumentException](Coverage(featureWithEmptyContigName))

    assert(caughtWithEmptyContigName.getMessage == "requirement failed: Features must have reference name to convert to Coverage")
    // feature without contigname is not valid when converting to Coverage
    val featureWithNullContigName =
      Feature.newBuilder()
        .setStart(1)
        .setEnd(2)
        .setScore(100)
        .build()

    val caughtWithNullContigName =
      intercept[IllegalArgumentException](Coverage(featureWithNullContigName))

    assert(caughtWithNullContigName.getMessage == "requirement failed: Features must have reference name to convert to Coverage")
  }

  sparkTest("Convert to coverage from Feature with no start/end position fails with correct error") {
    // feature without start position is invalid when converting to Coverage
    val featureWithoutStartPosition =
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setEnd(2)
        .setScore(100)
        .build()

    val caughtWithoutStartPosition =
      intercept[IllegalArgumentException](Coverage(featureWithoutStartPosition))

    assert(caughtWithoutStartPosition.getMessage == "requirement failed: Features must have valid position data to convert to Coverage")
    // feature without end position is invalid when converting to Coverage
    val featureWithoutEndPosition =
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(1)
        .setScore(100)
        .build()

    val caughtWithoutEndPosition =
      intercept[IllegalArgumentException](Coverage(featureWithoutEndPosition))

    assert(caughtWithoutEndPosition.getMessage == "requirement failed: Features must have valid position data to convert to Coverage")
  }

  sparkTest("Convert to coverage from Feature with no score fails with correct error") {
    // feature without score is invalid when converting to Coverage 
    val featureWithoutScore =
      Feature.newBuilder()
        .setReferenceName("chr1")
        .setStart(1)
        .setEnd(2)
        .build()

    val caughtWithoutScore =
      intercept[IllegalArgumentException](Coverage(featureWithoutScore))

    assert(caughtWithoutScore.getMessage == "requirement failed: Features must have valid score to convert to Coverage")
  }
}
