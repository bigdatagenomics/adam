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

import parquet.filter.{ RecordFilter, UnboundRecordFilter }
import java.lang.Iterable
import parquet.column.ColumnReader
import org.apache.spark.rdd.RDD

/**
 *
 *  ADAMPredicate: Classes derived from ADAMPredicate can be used to set ParquetInputFormat.setUnboundRecordFilter
 *  for predicate pushdown, or alternatively, filter an already loaded RDD
 *
 */
trait ADAMPredicate[T] extends UnboundRecordFilter {
  val recordCondition: RecordCondition[T]

  final def apply(rdd: RDD[T]): RDD[T] = {
    rdd.filter(recordCondition.filter)
  }

  override def bind(readers: Iterable[ColumnReader]): RecordFilter = recordCondition.recordFilter.bind(readers)
}
