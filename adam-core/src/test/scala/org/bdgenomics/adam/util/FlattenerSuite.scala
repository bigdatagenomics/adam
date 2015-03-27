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

import org.apache.avro.generic.GenericData
import org.apache.avro.{ SchemaBuilder, Schema }

import org.scalatest._

class FlattenerSuite extends FunSuite {

  test("Flatten schema and record") {
    val recordSchema: Schema = SchemaBuilder.record("Test").fields
      .requiredString("aString")
      .name("nested1").`type`.record("NestedType1").fields
      .optionalInt("anInt")
      .requiredLong("aLong")
      .nullableFloat("aFloat", 11.0f)
      .endRecord
      .noDefault
      .name("nested2").`type`.optional.record("NestedType2").fields
      .optionalInt("anInt")
      .requiredLong("aLong")
      .nullableFloat("aFloat", 12.0f)
      .endRecord
      .optionalInt("anInt")
      .optionalLong("aLong")
      .endRecord

    val expectedSchema: Schema = SchemaBuilder.record("Test_flat").fields
      .requiredString("aString")
      .optionalInt("nested1__anInt")
      .requiredLong("nested1__aLong")
      .nullableFloat("nested1__aFloat", 11.0f)
      .optionalInt("nested2__anInt")
      .optionalLong("nested2__aLong") // optional since nested2 is optional
      .nullableFloat("nested2__aFloat", 12.0f)
      .optionalInt("anInt")
      .optionalLong("aLong")
      .endRecord

    assert(Flattener.flattenSchema(recordSchema) === expectedSchema)

    val record: GenericData.Record = new GenericData.Record(recordSchema)
    record.put("aString", "triangle")
    val nested1: GenericData.Record = new GenericData.Record(
      recordSchema.getField("nested1").schema)
    nested1.put("anInt", 1)
    nested1.put("aLong", 2L)
    nested1.put("aFloat", 100.0f)
    record.put("nested1", nested1)
    record.put("nested2", null)
    record.put("anInt", null)
    record.put("aLong", 3L)

    val expected: GenericData.Record = new GenericData.Record(expectedSchema)
    expected.put("aString", "triangle")
    expected.put("nested1__anInt", 1)
    expected.put("nested1__aLong", 2L)
    expected.put("nested1__aFloat", 100.0f)
    expected.put("nested2__anInt", null)
    expected.put("nested2__aLong", null)
    expected.put("nested2__aFloat", null)
    expected.put("anInt", null)
    expected.put("aLong", 3L)

    assert(Flattener.flattenRecord(expectedSchema, record) === expected)
  }
}
