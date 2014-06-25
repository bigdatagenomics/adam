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
/*
* Copyright (c) 2014. Mount Sinai School of Medicine
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

package org.bdgenomics.adam.predicates

import org.bdgenomics.adam.projections.ADAMRecordField
import org.bdgenomics.formats.avro.ADAMRecord

object ADAMRecordConditions {

  val isMapped = RecordCondition[ADAMRecord](FieldCondition(ADAMRecordField.readMapped.toString, PredicateUtils.isTrue))
  val isUnique = RecordCondition[ADAMRecord](FieldCondition(ADAMRecordField.duplicateRead.toString, PredicateUtils.isFalse))

  val isPrimaryAlignment = RecordCondition[ADAMRecord](FieldCondition(ADAMRecordField.primaryAlignment.toString, PredicateUtils.isTrue))

  val passedVendorQualityChecks = RecordCondition[ADAMRecord](FieldCondition(ADAMRecordField.failedVendorQualityChecks.toString, PredicateUtils.isFalse))

  def isHighQuality(minQuality: Int) = RecordCondition[ADAMRecord](FieldCondition(ADAMRecordField.mapq.toString, (x: Int) => x > minQuality))

}
