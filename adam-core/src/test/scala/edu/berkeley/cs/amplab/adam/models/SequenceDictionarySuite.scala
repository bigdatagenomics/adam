/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.models

import org.scalatest._
import edu.berkeley.cs.amplab.adam.avro.{ADAMReferenceRecord, ADAMReferenceDictionary}
import scala.collection.JavaConversions._

class SequenceDictionarySuite extends FunSuite {

  test("Can retrieve sequence by ID") {
    val rec1 = record(0, "foo")
    val rec2 = record(1, "bar")
    assert(SequenceDictionary(rec1, rec2)(rec1.id) === rec1)
  }

  test("Can retrieve sequence by Name") {
    val rec1 = record(0, "foo")
    val rec2 = record(1, "bar")
    assert(SequenceDictionary(rec1, rec2)(rec1.name) === rec1)
  }

  test("SequenceDictionaries with same single element are equal") {
    assert(SequenceDictionary(record(0, "foo")) === SequenceDictionary(record(0, "foo")))
  }

  test("SequenceDictionaries with same two elements are equals") {
    assert(SequenceDictionary(record(0, "foo"), record(1, "bar")) ===
      SequenceDictionary(record(0, "foo"), record(1, "bar")))
  }

  test("SequenceDictionaries with different elements are unequal") {
    assert(SequenceDictionary(record(0, "foo"), record(1, "bar")) !=
      SequenceDictionary(record(0, "foo"), record(1, "quux")))
  }

  test("SequenceDictionaries with same elements in different order are equal") {
    assert(SequenceDictionary(record(0, "foo"), record(1, "bar")) ===
      SequenceDictionary(record(1, "bar"), record(0, "foo")))
  }

  test("double referenceIds throws an exception") {
    intercept[AssertionError] {
      SequenceDictionary(record(0, "foo"), record(0, "bar"))
    }
  }

  test("double referenceNames throws an exception") {
    intercept[AssertionError] {
      SequenceDictionary(record(0, "foo"), record(1, "foo"))
    }
  }

  test("mapTo generates correct identifier mappings") {
    val fromDict = SequenceDictionary(
      record(0, "foo"),
      record(1, "bar"),
      record(2, "quux"))

    val toDict = SequenceDictionary(record(10, "bar"), record(20, "quux"))

    assert(fromDict.mapTo(toDict) === Map(0 -> 0, 1 -> 10, 2 -> 20))
  }

  test("isCompatible tests equality on overlap") {
    val s1 = SequenceDictionary(record(0, "foo"), record(1, "bar"))
    val s2 = SequenceDictionary(record(1, "bar"), record(2, "quux"))
    val s3 = SequenceDictionary(record(0, "foo"), record(2, "bar"))

    assert(s1 isCompatibleWith s2)
    assert(!(s1 isCompatibleWith s3))
  }

  test("remap and mapTo generate equality for dictionaries with the same names") {
    val s1 = SequenceDictionary(record(1, "foo"), record(2, "bar"))
    val s2 = SequenceDictionary(record(20, "bar"), record(10, "foo"))

    assert(s1.mapTo(s2) === Map(1 -> 10, 2 -> 20))
    assert(s1.remap(s1.mapTo(s2)) === s2)
  }

  test("all five cases for toMap") {
    val s1 = SequenceDictionary(record(1, "s1"), record(3, "s2"), record(4, "s4"), record(6, "s6"))
    val s2 = SequenceDictionary(record(1, "s1"), record(2, "s2"), record(4, "s3"), record(5, "s5"))

    val map = s1.mapTo(s2)

    assert(map(1) === 1)
    assert(!map.contains(2))
    assert(map(3) === 2)
    assert(map(4) === s2.nonoverlappingHash("s4"))
    assert(!map.contains(5))
    assert(map(6) === 6)
  }

  test("mapTo and remap produce a compatible dictionary") {
    val s1 = SequenceDictionary(record(1, "s1"), record(3, "s2"), record(2, "s3"), record(5, "s4"))
    val s2 = SequenceDictionary(record(1, "s1"), record(2, "s2"), record(3, "s3"), record(5, "s5"),
      record("s4".hashCode, "s6"))

    val map = s1.mapTo(s2)

    // double check that the linear probing for new sequence idx assignment is operational.
    // -- this should match up with SequenceDictionary.nonoverlappingHash
    assert(map(5) === "s4".hashCode + 1)

    assert(s1.remap(map).isCompatibleWith(s2))
  }

  test("toMap handles permutations correctly") {
    val s1 = SequenceDictionary(record(1, "s2"), record(2, "s3"), record(3, "s1"))
    val s2 = SequenceDictionary(record(1, "s1"), record(2, "s2"), record(3, "s3"))

    val map = s1.mapTo(s2)

    assert(map(1) === 2)
    assert(map(2) === 3)
    assert(map(3) === 1)
  }

  test("the additions + and += work correctly") {
    val s1 = SequenceDictionary()
    val s2 = SequenceDictionary(record(1, "foo"))
    val s3 = SequenceDictionary(record(1, "foo"), record(2, "bar"))

    assert(s1 + record(1, "foo") === s2)
    assert(s2 + record(1, "foo") === s2)
    assert(s2 + record(2, "bar") === s3)

    s1 += record(1, "foo")
    assert(s1 === s2)

    s1 += record(1, "foo")
    assert(s1 === s2)

    s1 += record(2, "bar")
    assert(s1 === s3)
  }

  test("the append operations ++ and ++= work correctly") {
    val s1 = SequenceDictionary()
    val s2a = SequenceDictionary(record(1, "foo"))
    val s2b = SequenceDictionary(record(2, "bar"))
    val s3 = SequenceDictionary(record(1, "foo"), record(2, "bar"))

    assert(s1 ++ s1 === s1)
    assert(s1 ++ s2a === s2a)
    assert(s1 ++ s2b === s2b)
    assert(s2a ++ s2b === s3)

    s1 ++= s2a
    assert(s1 === s2a)

    s1 ++= s2b
    assert(s1 === s3)

    s1 ++= s3
    assert(s1 === s3)
  }

  test("Can create sequence dictionary from ADAMReferenceDictionary") {
    val rd = ADAMReferenceDictionary.newBuilder().setReferenceRecords(List(
      ADAMReferenceRecord.newBuilder().setReferenceId(0).setReferenceName("1").setReferenceLength(249250621).build()
    )).build()

    val sd = SequenceDictionary(record(0, "1", 249250621))

    assert(SequenceDictionary.fromADAMReferenceDictionary(rd) === sd)
  }


  def record(id: Int, name: String, length: Int = 1000, url: String = null): SequenceRecord =
    SequenceRecord(id, name, length, url)
}
