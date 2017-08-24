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

/**
 * The FieldValue trait (and its sister abstract class, FieldEnumeration) are meant to help
 * clean up the use of Projection when dealing with field enumeration classes like
 * ADAMRecordField.
 *
 * Projection is a class for turning fields from an Enumeration into a projected Avro Schema object.
 *
 * In the old way of doing this, we used a "normal" Enumeration.  Projection would receive a
 * collection of Read.Value objects, and use their names (plus the Read.SCHEMA$
 * static field) to turn them into a Schema for ADAMRecords.  This worked fine for Read,
 * and not at all for generalizing to other Schemas over other field enumerations.
 *
 * In the new system, we embed the Avro Schema object as an argument *in* each enumeration Value.
 * We do this in two steps:
 * (1) instead of ADAMRecordField (e.g.) extending Enumeration, it extends FieldEnumeration and
 * provides the appropriate (static) Schema object as an argument.
 * (2) instead of using the (final, non-overrideable) Value method within Enumeration to provide
 * each enum value, it calls FieldEnumeration.SchemaValue instead, which embeds the corresponding
 * Schema in each value.
 *
 * Finally, Projection will extract the Schema value from the first FieldValue that is given to it
 * and produce the corresponding (projected) Schema.
 *
 * This means, of course, that Projection can't handle empty field lists -- but that was always
 * going to be an error-filled edge-case anyway (why would you want to project to zero fields?)
 *
 */
trait FieldValue {
  def schema: Schema
}

/**
 * Abstract class that all record-specific enumerations extend.
 *
 * @param recordSchema The schema that this enumeration represents.
 */
abstract class FieldEnumeration(val recordSchema: Schema) extends Enumeration {

  /**
   * An enumerated field that is part of this schema.
   */
  class SchemaVal extends Val with FieldValue {

    /**
     * The schema for the record that this field is in.
     */
    def schema = recordSchema
  }

  protected final def SchemaValue = new SchemaVal()
}
