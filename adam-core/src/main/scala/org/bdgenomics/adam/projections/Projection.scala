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
package org.bdgenomics.adam.projections

import org.apache.avro.Schema
import org.apache.avro.Schema.Field

import scala.collection.JavaConversions._

/**
 * Avro utility object to create a projection of a Schema
 */
object Projection {

  private def createProjection(fullSchema: Schema, fields: Set[String], exclude: Boolean = false): Schema = {
    val projectedSchema = Schema.createRecord(fullSchema.getName, fullSchema.getDoc, fullSchema.getNamespace, fullSchema.isError)
    projectedSchema.setFields(fullSchema.getFields.filter(createFilterPredicate(fields, exclude))
      .map(p => new Field(p.name, p.schema, p.doc, p.defaultValue, p.order)))
    projectedSchema
  }

  private def createFilterPredicate(fieldNames: Set[String], exclude: Boolean = false): Field => Boolean = {
    val filterPred = (f: Field) => fieldNames.contains(f.name)
    val includeOrExlcude = (contains: Boolean) => if (exclude) !contains else contains
    filterPred.andThen(includeOrExlcude)
  }

  // TODO: Unify these various methods
  def apply(includedFields: FieldValue*): Schema = {
    assert(!includedFields.isEmpty, "Can't project down to zero fields!")
    Projection(false, includedFields: _*)
  }

  def apply(includedFields: Traversable[FieldValue]): Schema = {
    assert(includedFields.size > 0, "Can't project down to zero fields!")
    val baseSchema = includedFields.head.schema
    createProjection(baseSchema, includedFields.map(_.toString).toSet, false)
  }

  def apply(exclude: Boolean, includedFields: FieldValue*): Schema = {
    val baseSchema = includedFields.head.schema
    createProjection(baseSchema, includedFields.map(_.toString).toSet, exclude)
  }
}

object Filter {

  def apply(excludeFields: FieldValue*): Schema = {
    Projection(true, excludeFields: _*)
  }
}
