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
 * A wrapper around the attrTuple (key) and value pair seen in many formats.
 *
 * Includes the attrTuple-type explicitly, rather than embedding the
 * corresponding information in the type of 'value', because otherwise it'd be
 * difficult to extract the correct type for Byte and NumericSequence values.
 *
 * This class is roughly analogous to htsjdk's SAMTagAndValue.
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
    val sb = new StringBuilder
    sb.append(tag)
    sb.append(":")
    sb.append(tagType)
    if (byteSequenceTypes contains tagType) {
      sb.append(",").append(value.asInstanceOf[Array[Byte]].mkString(",")).toString()
    } else if (shortSequenceTypes contains tagType) {
      sb.append(",").append(value.asInstanceOf[Array[Short]].mkString(",")).toString()
    } else if (intSequenceTypes contains tagType) {
      sb.append(",").append(value.asInstanceOf[Array[Int]].mkString(",")).toString()
    } else if (tagType == TagType.NumericFloatSequence) {
      sb.append(",").append(value.asInstanceOf[Array[Float]].mkString(",")).toString()
    } else {
      sb.append(":").append(value).toString()
    }
  }
}

/**
 * An enumeration that describes the different data types that can be stored in
 * an attribute.
 */
object TagType extends Enumeration {

  /**
   * A representation of the type of data stored in a tagged field.
   *
   * @param abbreviation A string describing the data type underlying the
   *   attribute. The string values that are stored with the attribute come from
   *   the SAM file format spec: http://samtools.sourceforge.net/SAMv1.pdf
   */
  class TypeVal(val abbreviation: String) extends Val(nextId, abbreviation) {
    override def toString(): String = abbreviation
  }

  private def TypeValue(abbreviation: String): Val = new TypeVal(abbreviation)

  /**
   * An attribute storing a character. SAM "A".
   */
  val Character = TypeValue("A")

  /**
   * An attribute storing an integer. SAM "i".
   */
  val Integer = TypeValue("i")

  /**
   * An attribute storing a floating point value. SAM "f".
   */
  val Float = TypeValue("f")

  /**
   * An attribute storing a string. SAM "Z".
   */
  val String = TypeValue("Z")

  /**
   * An attribute storing hex formatted bytes. SAM "H".
   */
  val ByteSequence = TypeValue("H")

  /**
   * An attribute storing a numeric array of signed bytes. SAM "B:c".
   */
  val NumericByteSequence = TypeValue("B:c")

  /**
   * An attribute storing a numeric array of signed ints. SAM "B:i".
   */
  val NumericIntSequence = TypeValue("B:i")

  /**
   * An attribute storing a numeric array of signed short ints. SAM "B:i".
   */
  val NumericShortSequence = TypeValue("B:s")

  /**
   * An attribute storing a numeric array of unsigned bytes. SAM "B:C".
   */
  val NumericUnsignedByteSequence = TypeValue("B:C")

  /**
   * An attribute storing a numeric array of unsigned ints. SAM "B:I".
   */
  val NumericUnsignedIntSequence = TypeValue("B:I")

  /**
   * An attribute storing a numeric array of unsigned short ints. SAM "B:i".
   */
  val NumericUnsignedShortSequence = TypeValue("B:S")

  /**
   * An attribute storing a numeric array of floats. SAM "B:f".
   */
  val NumericFloatSequence = TypeValue("B:f")
}
