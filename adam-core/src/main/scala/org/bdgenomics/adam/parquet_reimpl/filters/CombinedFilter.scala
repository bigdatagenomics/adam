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
package org.bdgenomics.adam.parquet_reimpl.filters

import org.apache.avro.generic.IndexedRecord
import org.bdgenomics.adam.parquet_reimpl.index.{ IndexEntryPredicate, RowGroupIndexEntry }
import parquet.filter.UnboundRecordFilter

/**
 * CombinedFilter is (really) a hack -- what we _need_ is a way to translate 'filters' on records
 * that form an RDD into three roughly equivalent forms:
 * 1. an UnboundRecordFilter that can be used in interactions with a Parquet file directly,
 * 2. a predicate (RecordType => Boolean) function that can be used as an argument to RDD.filter(), and
 * 3. an IndexEntryPredicate, which filters only those row groups of a Parquet file which (according to
 *    an index) _could_ contain records which satisfy either of the two other forms.
 *
 * These three should be "equivalent," in the sense that (1) and (2) should accept exactly the same
 * records, and that (3) should accept index entry which indexes a row group that contains a record
 * satisfying either (1) or (2).
 *
 * The "right" way to do this, in the long run, is to have some kind of FILTERING DSL, which can be
 * translated into any of these three forms (or any other fourth form, that we decide we need in the
 * future) as necessary.
 *
 * In the meantime, however, we're simply providing an implementation as a FilterTuple below -- this
 * is just a triple of corresponding filters/predicates, and we leave it up to the user to ensure that
 * the semantics of the three arguments match the requirements outline above.
 *
 * @tparam RecordType The type of the record in the Parquet file or RDD to be filtered
 * @tparam IndexEntryType The type of the entry in the index which can be filtered.
 */
trait CombinedFilter[RecordType <: IndexedRecord, IndexEntryType <: RowGroupIndexEntry] extends Serializable {

  def recordFilter: UnboundRecordFilter
  def predicate: RecordType => Boolean
  def indexPredicate: IndexEntryPredicate[IndexEntryType]
}

trait SerializableUnboundRecordFilter extends UnboundRecordFilter with Serializable {}

class FilterTuple[R <: IndexedRecord, E <: RowGroupIndexEntry](filter: SerializableUnboundRecordFilter,
                                                               pred: R => Boolean,
                                                               indexPred: IndexEntryPredicate[E])
    extends CombinedFilter[R, E] {

  override def recordFilter: UnboundRecordFilter = filter
  override def predicate: (R) => Boolean = pred
  override def indexPredicate: IndexEntryPredicate[E] = indexPred
}
