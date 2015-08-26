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
package org.bdgenomics.adam.models

/**
 * A wrapper around the attrTuple (key) and value pair.  Includes the attrTuple-type explicitly, rather than
 * embedding the corresponding information in the type of 'value', because otherwise it'd be difficult
 * to extract the correct type for Byte and NumericSequence values.
 *
 * Roughly analogous to Picards SAMTagAndValue.
 *
 * @param tag The string key associated with this pair.
 * @param tagType An enumerated value representing the type of the 'value' parameter.
 * @param value The 'value' half of the pair.
 */
case class Attribute(tag: String, tagType: TagType.Value, value: Any) {
  override def toString: String = {
    val byteSequenceTypes = Array(TagType.NumericByteSequence, TagType.NumericUnsignedByteSequence)
    val intSequenceTypes = Array(TagType.NumericIntSequence, TagType.NumericUnsignedIntSequence)
    val shortSequenceTypes = Array(TagType.NumericShortSequence, TagType.NumericUnsignedShortSequence)
    if (byteSequenceTypes contains tagType) {
      "%s:%s,%s".format(tag, tagType, value.asInstanceOf[Array[Byte]].mkString(","))
    } else if (shortSequenceTypes contains tagType) {
      "%s:%s,%s".format(tag, tagType, value.asInstanceOf[Array[Short]].mkString(","))
    } else if (intSequenceTypes contains tagType) {
      "%s:%s,%s".format(tag, tagType, value.asInstanceOf[Array[Int]].mkString(","))
    } else if (tagType == TagType.NumericFloatSequence) {
      "%s:%s,%s".format(tag, tagType, value.asInstanceOf[Array[Float]].mkString(","))
    } else {
      "%s:%s:%s".format(tag, tagType, value.toString)
    }
  }
}

object TagType extends Enumeration {

  class TypeVal(val abbreviation: String) extends Val(nextId, abbreviation) {
    override def toString(): String = abbreviation
  }
  def TypeValue(abbreviation: String): Val = new TypeVal(abbreviation)

  // These String values come from the SAM file format spec: http://samtools.sourceforge.net/SAMv1.pdf
  val Character = TypeValue("A")
  val Integer = TypeValue("i")
  val Float = TypeValue("f")
  val String = TypeValue("Z")
  val ByteSequence = TypeValue("H")
  val NumericByteSequence = TypeValue("B:c")
  val NumericIntSequence = TypeValue("B:i")
  val NumericShortSequence = TypeValue("B:s")
  val NumericUnsignedByteSequence = TypeValue("B:C")
  val NumericUnsignedIntSequence = TypeValue("B:I")
  val NumericUnsignedShortSequence = TypeValue("B:S")
  val NumericFloatSequence = TypeValue("B:f")

}
