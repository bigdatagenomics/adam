/*
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rich

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

class ADAMRecordReferenceMappingSuite extends SparkFunSuite {

  sparkTest("test getReferenceId returns the right referenceId") {
    val rec = ADAMRecord.newBuilder().setReferenceId(12).build()
    assert(ADAMRecordReferenceMapping.getReferenceId(rec) === 12)
  }

  sparkTest("test that remapReferenceId really changes the referenceId") {
    val rec = ADAMRecord.newBuilder().setReferenceId(12).build()
    val rec2 = ADAMRecordReferenceMapping.remapReferenceId(rec, 15)

    assert(ADAMRecordReferenceMapping.getReferenceId(rec) === 12)
    assert(ADAMRecordReferenceMapping.getReferenceId(rec2) === 15)
  }
}
