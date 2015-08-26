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

import org.scalatest._
import htsjdk.samtools.SAMRecord.SAMTagAndValue
import htsjdk.samtools.TextTagCodec
import org.bdgenomics.adam.models.{ Attribute, TagType }

class AttributeUtilsSuite extends FunSuite {

  import AttributeUtils._

  test("parseTags returns a reasonable set of tagStrings") {
    val tags = parseAttributes("XT:i:3\tXU:Z:foo,bar")

    assert(tags.size === 2)

    assert(tags.head.tag === "XT")
    assert(tags.head.tagType === TagType.Integer)
    assert(tags.head.value === 3)

    assert(tags(1).tag === "XU")
    assert(tags(1).tagType === TagType.String)
    assert(tags(1).value === "foo,bar")
  }

  test("parseTags works with NumericSequence tagType") {
    val tags = parseAttributes("jM:B:c,-1\tjI:B:i,-1,1")

    assert(tags(0).tag === "jM")
    assert(tags(0).tagType === TagType.NumericByteSequence)
    assert(tags(0).value.asInstanceOf[Array[Number]].sameElements(Array(-1)))
    assert(tags(1).value.asInstanceOf[Array[Number]].sameElements(Array(-1, 1)))

  }

  test("empty string is parsed as zero tagStrings") {
    assert(parseAttributes("") === Seq[Attribute]())
  }

  test("incorrectly formatted tag throws an exception") {
    intercept[IllegalArgumentException] {
      assert(parseAttributes("XT:i") === Seq[Attribute]())
    }
  }

  test("string tag with a ':' in it is correctly parsed") {
    val string = "foo:bar"
    val tags = parseAttributes("XX:Z:%s".format(string))

    assert(tags.size === 1)
    assert(tags.head.value === string)
  }
}

class AttributeSuite extends FunSuite {

  import AttributeUtils._

  test("test SAMTagAndValue parsing") {
    // Build SAMTagAndValue using the same string parser as htsjdk
    def createSAMTagAndValue(tagString: String): SAMTagAndValue = {
      val tagMap = new TextTagCodec().decode(tagString)
      new SAMTagAndValue(tagMap.getKey(), tagMap.getValue())
    }
    // Simple tag types
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:i:3")) === Attribute("XY", TagType.Integer, 3))
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:Z:foo")) === Attribute("XY", TagType.String, "foo"))
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:f:3.0")) === Attribute("XY", TagType.Float, 3.0f))
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:A:a")) === Attribute("XY", TagType.Character, 'a'))

    // Array tag types
    val intArray = Array(1, 2, 3)
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:B:i,1,2,3")).toString ===
      Attribute("XY", TagType.NumericIntSequence, intArray).toString)

    val shortArray: Array[Short] = Array(1, 2, 3).map(_.toShort)
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:B:s,1,2,3")).toString ===
      Attribute("XY", TagType.NumericShortSequence, shortArray).toString)

    val floatArray = Array(1.0f, 2.0f, 3.0f)
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:B:f,1.0,2.0,3.0")).toString ===
      Attribute("XY", TagType.NumericFloatSequence, floatArray).toString)

    // Two forms of Byte arrays, type B:c and type H, indistinguishable by SAMTagAndValue
    val byteArray: Array[Byte] = Array(1, 2, 3).map(_.toByte)
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:B:c,1,2,3")).toString ===
      Attribute("XY", TagType.NumericByteSequence, byteArray).toString)
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:H:010203")).toString ===
      Attribute("XY", TagType.NumericByteSequence, byteArray).toString)

    // Unsigned int arrays, note the capitalized leading character in the value
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:B:C,1,2,3")).toString ===
      Attribute("XY", TagType.NumericUnsignedByteSequence, byteArray).toString)
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:B:I,1,2,3")).toString ===
      Attribute("XY", TagType.NumericUnsignedIntSequence, intArray).toString)
    assert(convertSAMTagAndValue(createSAMTagAndValue("XY:B:S,1,2,3")).toString ===
      Attribute("XY", TagType.NumericUnsignedShortSequence, shortArray).toString)
  }

  test("Attributes can be correctly re-encoded as text SAM tags") {
    assert(Attribute("XY", TagType.Integer, 3).toString === "XY:i:3")
    assert(Attribute("XY", TagType.String, "foo").toString === "XY:Z:foo")
    assert(Attribute("XY", TagType.Float, 3.0f).toString === "XY:f:3.0")
    assert(Attribute("XY", TagType.Character, 'a').toString === "XY:A:a")

    val intArray = Array(1, 2, 3)
    assert(Attribute("XY", TagType.NumericIntSequence, intArray).toString === "XY:B:i,1,2,3")

    val shortArray: Array[Short] = Array(1, 2, 3).map(_.toShort)
    assert(Attribute("XY", TagType.NumericShortSequence, shortArray).toString === "XY:B:s,1,2,3")

    val floatArray = Array(1.0f, 2.0f, 3.0f)
    assert(Attribute("XY", TagType.NumericFloatSequence, floatArray).toString === "XY:B:f,1.0,2.0,3.0")

    val byteArray: Array[Byte] = Array(1, 2, 3).map(_.toByte)
    assert(Attribute("XY", TagType.NumericByteSequence, byteArray).toString === "XY:B:c,1,2,3")

    // Unsigned int arrays, note the capitalized leading character in the value
    assert(Attribute("XY", TagType.NumericUnsignedByteSequence, byteArray).toString === "XY:B:C,1,2,3")
    assert(Attribute("XY", TagType.NumericUnsignedIntSequence, intArray).toString === "XY:B:I,1,2,3")
    assert(Attribute("XY", TagType.NumericUnsignedShortSequence, shortArray).toString === "XY:B:S,1,2,3")

  }
}
