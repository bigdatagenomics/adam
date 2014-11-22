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

import parquet.filter.ColumnPredicates.Predicate
import parquet.column.ColumnReader
import parquet.filter.UnboundRecordFilter
import parquet.filter.ColumnRecordFilter._
import org.bdgenomics.adam.predicates.ColumnReaderInput.ColumnReaderInput
import org.bdgenomics.adam.projections.AlignmentRecordField
import scala.Predef._

object ColumnReaderInput extends Serializable {
  trait ColumnReaderInput[T] extends Serializable {
    def convert(x: ColumnReader): T
  }
  implicit object ColumnReaderInputInt extends ColumnReaderInput[Int] {
    def convert(input: ColumnReader): Int = input.getInteger
  }
  implicit object ColumnReaderInputLong extends ColumnReaderInput[Long] {
    def convert(input: ColumnReader): Long = input.getLong
  }
  implicit object ColumnReaderInputString extends ColumnReaderInput[String] {
    def convert(input: ColumnReader): String = input.getBinary.toStringUsingUTF8
  }
  implicit object ColumnReaderInputDouble extends ColumnReaderInput[Double] {
    def convert(input: ColumnReader): Double = input.getDouble
  }
  implicit object ColumnReaderInputFloat extends ColumnReaderInput[Float] {
    def convert(input: ColumnReader): Float = input.getFloat
  }
  implicit object ColumnReaderInputBoolean extends ColumnReaderInput[Boolean] {
    def convert(input: ColumnReader): Boolean = input.getBoolean
  }
}

case class FieldCondition[T](fieldName: String, filter: T => Boolean)(implicit converter: ColumnReaderInput[T])
    extends Predicate {

  def apply(input: Any): Boolean = {
    filter(input.asInstanceOf[T])
  }

  override def apply(input: ColumnReader): Boolean = {
    filter(converter.convert(input))
  }

  def columnFilter: UnboundRecordFilter = column(fieldName, this)

}

object FieldCondition {

  def apply(field: AlignmentRecordField.Value,
            filterValue: Boolean)(implicit converter: ColumnReaderInput[Boolean]): FieldCondition[Boolean] = {
    FieldCondition(field.toString, PredicateUtils(filterValue))
  }
}
