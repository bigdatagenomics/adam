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

import htsjdk.samtools.SAMRecord.SAMTagAndValue
import htsjdk.samtools.TagValueAndUnsignedArrayFlag
import org.bdgenomics.adam.models.{ TagType, Attribute }

/**
 * AttributeUtils is a utility object for parsing optional fields from a BAM file, or
 * the attributes column from an ADAM file.
 */
object AttributeUtils {

  val attrRegex = """([^:]{2,4}):([AifZHB]):(.*)""".r
  val arrayRegex = """([cCiIsSf]{1},)(.*)""".r

  /**
   * Converts an htsjdk SAM Tag/Value into an ADAM attribute.
   *
   * @param attr htsjdk tag/value pair containing a tag value with a known type.
   * @return Returns an Attribute instance.
   */
  def convertSAMTagAndValue(attr: SAMTagAndValue): Attribute = {
    if (attr.value.isInstanceOf[TagValueAndUnsignedArrayFlag]) {
      attr.value.asInstanceOf[TagValueAndUnsignedArrayFlag].value match {
        case x: Array[Int]   => Attribute(attr.tag, TagType.NumericUnsignedIntSequence, x.asInstanceOf[Array[Int]])
        case x: Array[Byte]  => Attribute(attr.tag, TagType.NumericUnsignedByteSequence, x.asInstanceOf[Array[Byte]])
        case x: Array[Short] => Attribute(attr.tag, TagType.NumericUnsignedShortSequence, x.asInstanceOf[Array[Short]])
      }
    } else {
      attr.value match {
        case x: java.lang.Integer   => Attribute(attr.tag, TagType.Integer, x)
        case x: java.lang.Character => Attribute(attr.tag, TagType.Character, x)
        case x: java.lang.Float     => Attribute(attr.tag, TagType.Float, x)
        case x: java.lang.String    => Attribute(attr.tag, TagType.String, x)
        case x: Array[Int]          => Attribute(attr.tag, TagType.NumericIntSequence, x.asInstanceOf[Array[Int]])
        case x: Array[Byte]         => Attribute(attr.tag, TagType.NumericByteSequence, x.asInstanceOf[Array[Byte]])
        case x: Array[Short]        => Attribute(attr.tag, TagType.NumericShortSequence, x.asInstanceOf[Array[Short]])
        case x: Array[Float]        => Attribute(attr.tag, TagType.NumericFloatSequence, x.asInstanceOf[Array[Float]])
        // attr.value for type 'H' is indistinguishable from 'B:c', so both will be saved as NumericByteSequence.
      }
    }
  }

  /**
   * Parses a tab-separated string of attributes (tag:type:value) into a Seq of Attribute values.
   *
   * @param tagStrings The String to be parsed
   * @return The parsed Attributes
   */
  def parseAttributes(tagStrings: String): Seq[Attribute] =
    tagStrings.split("\t").filter(_.length > 0).map(parseAttribute)

  /**
   * Extract the Attribute value from the corresponding String fragment stored in the
   * Read.attributes field.
   *
   * @param encoded A three-part ':'-separated string, containing [attrTuple]:[type]:[value]
   * @return An Attribute value which has the three, extracted and correctly-typed parts of the encoded argument.
   * @throws IllegalArgumentException if the encoded string doesn't conform to the required regular
   *         expression ([A-Z]{2}:[AifZHB]:[^\t^]+)
   */
  def parseAttribute(encoded: String): Attribute = {
    attrRegex.findFirstMatchIn(encoded) match {
      case Some(m) => createAttribute((m.group(1), m.group(2), m.group(3)))
      case None =>
        throw new IllegalArgumentException(
          "attribute string \"%s\" doesn't match format attrTuple:type:value".format(encoded)
        )
    }
  }

  private def createAttribute(attrTuple: (String, String, String)): Attribute = {
    val tagName = attrTuple._1
    val tagTypeString = attrTuple._2

    val (fullTagString, valueStr) = if (tagTypeString == "B") {
      arrayRegex.findFirstMatchIn(attrTuple._3) match {
        case Some(m) => {
          ("%s:%s".format(tagTypeString, m.group(1)), m.group(2))
        }
        case None => {
          throw new IllegalArgumentException(
            "Array tags must define array format. For tag %s.".format(attrTuple))
        }
      }
    } else {
      (tagTypeString, attrTuple._3)
    }

    // partial match, but these letters should be (per the SAM file format spec)
    // the only legal values of the tagTypeString anyway.
    val tagType = fullTagString match {
      case "A"    => TagType.Character
      case "i"    => TagType.Integer
      case "f"    => TagType.Float
      case "Z"    => TagType.String
      case "H"    => TagType.ByteSequence
      case "B:c," => TagType.NumericByteSequence
      case "B:i," => TagType.NumericIntSequence
      case "B:s," => TagType.NumericShortSequence
      case "B:C," => TagType.NumericUnsignedByteSequence
      case "B:I," => TagType.NumericUnsignedIntSequence
      case "B:S," => TagType.NumericUnsignedShortSequence
      case "B:f," => TagType.NumericFloatSequence
    }

    Attribute(tagName, tagType, typedStringToValue(tagType, valueStr))
  }

  private def typedStringToValue(tagType: TagType.Value, valueStr: String): Any = {
    tagType match {
      case TagType.Character => valueStr(0)
      case TagType.Integer => Integer.valueOf(valueStr)
      case TagType.Float => java.lang.Float.valueOf(valueStr)
      case TagType.String => valueStr
      case TagType.ByteSequence => valueStr.map(c => java.lang.Byte.valueOf("" + c))
      case TagType.NumericByteSequence | TagType.NumericUnsignedByteSequence => valueStr.split(",").map(_.toByte)
      case TagType.NumericShortSequence | TagType.NumericUnsignedShortSequence => valueStr.split(",").map(_.toShort)
      case TagType.NumericIntSequence | TagType.NumericUnsignedIntSequence => valueStr.split(",").map(_.toInt)
      case TagType.NumericFloatSequence => valueStr.split(",").map(_.toFloat)
    }
  }
}
