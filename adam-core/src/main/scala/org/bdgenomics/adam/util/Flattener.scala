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
package org.bdgenomics.adam.util

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.{ GenericData, IndexedRecord }
import org.codehaus.jackson.node.NullNode
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Utility singleton for flattening down nested Avro records.
 *
 * When we refer to a schema as flat, we mean that there are no nested records.
 * We do not mean that the schema does not contain maps or arrays.
 */
object Flattener extends Serializable {

  /**
   * This separator is used to generate the flattened name of a nested
   * field. E.g., if a record is nested like so:
   *
   * - record Genotype
   * -- variant: Variant
   * --- start: Long
   * --- end: Long
   * -- genotypeState: Int
   *
   * With the SEPARATOR "__", the flat record will be:
   *
   * - record Genotype
   * -- variant__start: Long
   * -- variant__end: Long
   * -- genotypeState: Int
   */
  val SEPARATOR: String = "__";

  /**
   * Given a possibly nested schema, creates a flat schema.
   *
   * @param schema The schema to flatten.
   * @return Returns a flattened representation of the schema.
   */
  def flattenSchema(schema: Schema): Schema = {
    val flatSchema: Schema = Schema.createRecord(schema.getName + "_flat", schema.getDoc,
      schema.getNamespace, schema.isError)
    flatSchema.setFields(flatten(schema, "", new ListBuffer[Schema.Field]).asJava)
    flatSchema
  }

  private def flatten(schema: Schema, prefix: String,
                      accumulator: ListBuffer[Schema.Field],
                      makeOptional: Boolean = false): ListBuffer[Schema.Field] = {
    for (f: Schema.Field <- schema.getFields) {
      f.schema.getType match {
        case NULL | BOOLEAN | INT | LONG | FLOAT | DOUBLE | BYTES | STRING |
          FIXED | ENUM =>
          accumulator += copy(f, prefix, makeOptional)
        case RECORD =>
          flatten(f.schema, prefix + f.name + SEPARATOR, accumulator, makeOptional)
        case UNION =>
          val nested = f.schema.getTypes.filter(_.getType != Schema.Type.NULL)
          if (nested.size == 1) {
            val s: Schema = nested.head
            s.getType match {
              case NULL | BOOLEAN | INT | LONG | FLOAT | DOUBLE | BYTES | STRING |
                FIXED | ENUM =>
                accumulator += copy(f, prefix, makeOptional)
              case RECORD =>
                val opt = makeOptional || f.defaultValue.equals(NullNode.getInstance)
                flatten(s, prefix + f.name + SEPARATOR, accumulator, opt)
              case UNION | ARRAY | MAP | _ => // drop field
            }
          }
        case ARRAY | MAP | _ => // drop field
      }
    }
    accumulator
  }

  private def copy(f: Schema.Field, prefix: String, makeOptional: Boolean): Schema.Field = {
    val schema = if (makeOptional) optional(f.schema) else f.schema
    val defaultValue = if (f.defaultValue == null && makeOptional) NullNode.getInstance
    else f.defaultValue
    val copy: Schema.Field = new Schema.Field(prefix + f.name, schema, f.doc, defaultValue)
    import scala.collection.JavaConversions._
    for (prop <- f.getJsonProps.entrySet) {
      copy.addProp(prop.getKey, prop.getValue)
    }
    copy
  }

  private def optional(schema: Schema): Schema = {
    if (schema.getType eq Schema.Type.NULL) {
      return schema
    }

    if (schema.getType ne Schema.Type.UNION) {
      return Schema.createUnion(
        ListBuffer[Schema](Schema.create(Schema.Type.NULL), schema).asJava
      )
    }

    schema // TODO: what about unions that don't contain null?
  }

  /**
   * Takes a given nested record and flattens it to correspond to a flattened
   * schema.
   *
   * @param flatSchema The flat schema for the new record.
   * @param record The nested record to flatten.
   * @return Returns a new Avro IndexedRecord where every field from the initial
   *   record has been copied, but where the schema corresponds to the new flat
   *   schema.
   */
  def flattenRecord(flatSchema: Schema, record: IndexedRecord): IndexedRecord = {
    val flatRecord: GenericData.Record = new GenericData.Record(flatSchema)
    flatten(record.getSchema, record, flatRecord, 0)
    flatRecord
  }

  private def flatten(schema: Schema, record: IndexedRecord, flatRecord: IndexedRecord,
                      offset: Int): Int = {
    if (record == null)
      return offset + schema.getFields.size
    var off: Int = offset
    for (f: Schema.Field <- schema.getFields) {
      f.schema.getType match {
        case NULL | BOOLEAN | INT | LONG | FLOAT | DOUBLE | BYTES | STRING |
          FIXED | ENUM =>
          flatRecord.put(off, record.get(f.pos))
          off += 1
        case RECORD =>
          off = flatten(f.schema, record.get(f.pos).asInstanceOf[IndexedRecord],
            flatRecord, off)
        case UNION =>
          val nested = f.schema.getTypes.filter(_.getType != Schema.Type.NULL)
          if (nested.size == 1) {
            val s: Schema = nested.head
            s.getType match {
              case NULL | BOOLEAN | INT | LONG | FLOAT | DOUBLE | BYTES | STRING |
                FIXED | ENUM =>
                flatRecord.put(off, record.get(f.pos))
                off += 1
              case RECORD =>
                off = flatten(s, record.get(f.pos).asInstanceOf[IndexedRecord],
                  flatRecord, off)
              case UNION | ARRAY | MAP | _ => // drop field
            }
          }
        case ARRAY | MAP | _ => // drop field
      }
    }
    return off
  }
}
