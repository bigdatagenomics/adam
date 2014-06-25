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
package org.bdgenomics.adam.rich

import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ ADAMContig, ADAMRecord }
import org.bdgenomics.adam.rich.ReferenceMappingContext.ADAMRecordReferenceMapping

class ADAMRecordReferenceMappingSuite extends SparkFunSuite {

  sparkTest("test getReferenceId returns the right referenceId") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr12")
      .build

    val rec = ADAMRecord.newBuilder().setContig(contig).build()
    assert(ADAMRecordReferenceMapping.getReferenceName(rec) === "chr12")
  }
}
