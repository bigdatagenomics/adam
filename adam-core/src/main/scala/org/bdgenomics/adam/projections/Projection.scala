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
import scala.collection.JavaConversions._
import org.apache.avro.Schema.Field

/**
 * Avro utility object to create a projection of a Schema.
 */
object Projection {

  private def createProjection(fullSchema: Schema,
                               fields: Set[String],
                               exclude: Boolean = false): Schema = {
    val projectedSchema = Schema.createRecord(fullSchema.getName, fullSchema.getDoc, fullSchema.getNamespace, fullSchema.isError)
    projectedSchema.setFields(fullSchema.getFields.filter(createFilterPredicate(fields, exclude))
      .map(p => new Field(p.name, p.schema, p.doc, p.defaultValue, p.order)))
    projectedSchema
  }

  private def createFilterPredicate(fieldNames: Set[String],
                                    exclude: Boolean = false): Field => Boolean = {
    val filterPred = (f: Field) => fieldNames.contains(f.name)
    val includeOrExlcude = (contains: Boolean) => if (exclude) !contains else contains
    filterPred.andThen(includeOrExlcude)
  }

  /**
   * Creates a projection that includes a variable number of fields.
   *
   * @param includedFields Fields to include in the projection
   * @return Returns the specified schema with the fields predicated out.
   *
   * @note The schema is inferred from the provided FieldValues. Undefined
   *   behavior may result if you provide FieldValues from multiple different
   *   FieldEnumerations.
   *
   * @throws IllegalArgumentException if there are no fields included in the
   *   projection.
   */
  def apply(includedFields: FieldValue*): Schema = {
    require(!includedFields.isEmpty, "Can't project down to zero fields!")
    Projection(false, includedFields: _*)
  }

  /**
   * Creates a projection that includes a variable number of fields.
   *
   * @param includedFields Fields to include in the projection
   * @return Returns the specified schema with the fields predicated out.
   *
   * @note The schema is inferred from the provided FieldValues. Undefined
   *   behavior may result if you provide FieldValues from multiple different
   *   FieldEnumerations.
   *
   * @throws IllegalArgumentException if there are no fields included in the
   *   projection.
   */
  def apply(includedFields: Traversable[FieldValue]): Schema = {
    require(includedFields.size > 0, "Can't project down to zero fields!")
    val baseSchema = includedFields.head.schema
    createProjection(baseSchema, includedFields.map(_.toString).toSet, false)
  }

  /**
   * Creates a projection that either includes or excludes a variable number of
   * fields.
   *
   * @param exclude If false, includes the provided fields in the projection.
   *   If true, the provided fields will be excluded, and all other fields from
   *   the record will be included in the projection.
   * @param includedFields If exclude is false, fields to include in the
   *   projection. Else, fields to exclude from the projection.
   * @return Returns the specified schema with the fields predicated out.
   *
   * @note The schema is inferred from the provided FieldValues. Undefined
   *   behavior may result if you provide FieldValues from multiple different
   *   FieldEnumerations.
   *
   * @note Unlike the other apply methods, this method will allow you to project
   *   down to zero fields, which may cause an exception when loading from disk.
   */
  def apply(exclude: Boolean, includedFields: FieldValue*): Schema = {
    val baseSchema = includedFields.head.schema
    createProjection(baseSchema, includedFields.map(_.toString).toSet, exclude)
  }
}

/**
 * Helper object to create a projection that excludes fields from a schema.
 */
object Filter {

  /**
   * Creates a projection that excludes fields from a schema.
   *
   * @param excludedFields The fields to exclude from this projection.
   * @return Returns the specified schema with the fields predicated out.
   *
   * @note The schema is inferred from the provided FieldValues. Undefined
   *   behavior may result if you provide FieldValues from multiple different
   *   FieldEnumerations.
   *
   * @note Unlike the apply methods in Projection, this method will allow you to
   *   project down to zero fields, which may cause an exception when loading
   *   from disk.
   */
  def apply(excludeFields: FieldValue*): Schema = {
    Projection(true, excludeFields: _*)
  }
}
