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
package org.bdgenomics.adam.parquet_reimpl.index

/**
 * A predicate on the entries in an index-file.  The RDD which uses the index
 * will receive an IndexEntryPredicate as an argument, and use it (on the index)
 * to determine which row groups should be scanned.
 *
 * @tparam Entry The entry type
 */
trait IndexEntryPredicate[Entry <: RowGroupIndexEntry] extends Serializable {
  def accepts(entry: Entry): Boolean
}

object IndexEntryPredicate {

  def and[E <: RowGroupIndexEntry](preds: IndexEntryPredicate[E]*): IndexEntryPredicate[E] =
    AndIndexPredicate[E](preds: _*)

  def or[E <: RowGroupIndexEntry](preds: IndexEntryPredicate[E]*): IndexEntryPredicate[E] =
    OrIndexPredicate[E](preds: _*)
}

case class AndIndexPredicate[Entry <: RowGroupIndexEntry](indexPredicates: IndexEntryPredicate[Entry]*)
    extends IndexEntryPredicate[Entry] {
  override def accepts(entry: Entry): Boolean = indexPredicates.forall(_.accepts(entry))
}

case class OrIndexPredicate[Entry <: RowGroupIndexEntry](indexPredicates: IndexEntryPredicate[Entry]*)
    extends IndexEntryPredicate[Entry] {
  override def accepts(entry: Entry): Boolean = indexPredicates.exists(_.accepts(entry))
}

