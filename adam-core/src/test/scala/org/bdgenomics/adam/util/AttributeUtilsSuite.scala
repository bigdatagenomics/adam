/**
 * Copyright 2014 Genome Bridge LLC
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
package org.bdgenomics.adam.util

import org.scalatest._
import net.sf.samtools.SAMRecord.SAMTagAndValue
import org.bdgenomics.adam.models.{Attribute, TagType}

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
    assert(convertSAMTagAndValue(new SAMTagAndValue("XY", 3)) === Attribute("XY", TagType.Integer, 3))
    assert(convertSAMTagAndValue(new SAMTagAndValue("XY", "foo")) === Attribute("XY", TagType.String, "foo"))
    assert(convertSAMTagAndValue(new SAMTagAndValue("XY", 3.0f)) === Attribute("XY", TagType.Float, 3.0f))
    assert(convertSAMTagAndValue(new SAMTagAndValue("XY", 'a')) === Attribute("XY", TagType.Character, 'a'))

    val byteArray : Array[java.lang.Byte] = Seq(java.lang.Byte.valueOf("0"), java.lang.Byte.valueOf("1")).toArray
    assert(convertSAMTagAndValue(new SAMTagAndValue("XY", byteArray))=== Attribute("XY", TagType.ByteSequence, byteArray))
  }
}
