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

package edu.berkeley.cs.amplab.adam.predicates

import parquet.filter.{RecordFilter, UnboundRecordFilter}
import java.lang.Iterable
import parquet.column.ColumnReader
import parquet.filter.ColumnRecordFilter.column
import parquet.filter.OrRecordFilter.or
import parquet.filter.ColumnPredicates.equalTo

class GenotypeVarFilterPASSPredicate extends UnboundRecordFilter {
  def bind(readers: Iterable[ColumnReader]): RecordFilter = {
    or(column("varIsFiltered", equalTo(null.asInstanceOf[Boolean])), column("varIsFiltered", equalTo(false))).bind(readers)
  }
}
