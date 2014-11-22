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

import org.apache.avro.specific.SpecificRecord
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import parquet.filter.{ AndRecordFilter, OrRecordFilter, UnboundRecordFilter }

import org.bdgenomics.adam.util.ImplicitJavaConversions._
import scala.annotation.tailrec

object RecordCondition {

  // Convert predicate on single field to predicate on record
  def getRecordPredicate[T <: SpecificRecord: Manifest, U](condition: FieldCondition[U]): T => Boolean = {
    @tailrec
    def getFieldValue(record: SpecificRecord, fieldPath: Seq[String]): Any = {
      val schema = record.getSchema
      val field: Field = schema.getField(fieldPath.head)
      val fieldType = field.schema.getTypes.filter(_.getType != Schema.Type.NULL)(0)
      if (fieldType.getType == Schema.Type.RECORD) {
        getFieldValue(record.get(field.pos).asInstanceOf[SpecificRecord], fieldPath.tail)
      } else {
        record.get(field.pos)
      }
    }

    (record: T) => {
      val fieldName = condition.fieldName
      val filter: Any => Boolean = condition.apply
      val fieldValue = getFieldValue(record, fieldName.split("\\."))
      filter(fieldValue)
    }
  }

  // Create a record predicate from many individual field predicates
  def apply[T <: SpecificRecord: Manifest](conditions: FieldCondition[_]*): RecordCondition[T] = {
    conditions.map(c => {
      val fieldPredicate = getRecordPredicate(c)
      new RecordCondition(fieldPredicate, c.columnFilter)
    }).reduce(_ && _)
  }
}

/**
 *
 *  A RecordCondition is a filter on any Avro defined records and
 *  contains an UnboundRecordFilter that can be used for predicate pushdown
 *  with Parquet stored files
 *
 */
class RecordCondition[T <% SpecificRecord: Manifest](val filter: T => Boolean, val recordFilter: UnboundRecordFilter)
    extends Serializable {

  // Combine two predicates through an AND
  def and(other: RecordCondition[T]): RecordCondition[T] = &&(other)
  def &&(other: RecordCondition[T]): RecordCondition[T] = {

    // Local variables to avoid serialization
    val thisFilter = filter
    val otherFilter = other.filter

    new RecordCondition[T](filter = (r: T) => thisFilter(r) && otherFilter(r),
      recordFilter = AndRecordFilter.and(recordFilter, other.recordFilter))
  }

  // Combine two predicats through an OR
  def or(other: RecordCondition[T]): RecordCondition[T] = ||(other)
  def ||(other: RecordCondition[T]): RecordCondition[T] = {

    // Local variables to avoid serialization
    val thisFilter = filter
    val otherFilter = other.filter

    new RecordCondition[T](filter = (r: T) => thisFilter(r) || otherFilter(r),
      recordFilter = OrRecordFilter.or(recordFilter, other.recordFilter))
  }

  // Apply the predicate on a record
  def apply(record: T): Boolean = {
    filter(record)
  }
}
