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

package org.bdgenomics.adam.predicates

import org.bdgenomics.adam.projections.AlignmentRecordField
import org.bdgenomics.formats.avro.AlignmentRecord

object AlignmentRecordConditions {

  val isMapped = RecordCondition[AlignmentRecord](FieldCondition(AlignmentRecordField.readMapped.toString(), PredicateUtils.isTrue))
  val isUnique = RecordCondition[AlignmentRecord](FieldCondition(AlignmentRecordField.duplicateRead.toString(), PredicateUtils.isFalse))

  val isPrimaryAlignment = RecordCondition[AlignmentRecord](FieldCondition(AlignmentRecordField.primaryAlignment.toString(), PredicateUtils.isTrue))

  val passedVendorQualityChecks = RecordCondition[AlignmentRecord](FieldCondition(AlignmentRecordField.failedVendorQualityChecks.toString(), PredicateUtils.isFalse))

  def isHighQuality(minQuality: Int) = RecordCondition[AlignmentRecord](FieldCondition(AlignmentRecordField.mapq.toString(), (x: Int) => x > minQuality))

}
