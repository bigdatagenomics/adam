/*
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import net.sf.samtools.SAMRecord.SAMTagAndValue
import org.bdgenomics.adam.models.{ TagType, Attribute }

/**
 * AttributeUtils is a utility object for parsing optional fields from a BAM file, or
 * the attributes column from an ADAM file.
 *
 */
object AttributeUtils {

  val attrRegex = RegExp("([^:]{2}):([AifZHB]):(.*)")

  def convertSAMTagAndValue(attr: SAMTagAndValue): Attribute = {
    attr.value match {
      case x: java.lang.Integer => Attribute(attr.tag, TagType.Integer, attr.value.asInstanceOf[Int])
      case x: java.lang.Character => Attribute(attr.tag, TagType.Character, attr.value.asInstanceOf[Char])
      case x: java.lang.Float => Attribute(attr.tag, TagType.Float, attr.value.asInstanceOf[Float])
      case x: java.lang.String => Attribute(attr.tag, TagType.String, attr.value.asInstanceOf[String])
      case Array(_*) => Attribute(attr.tag, TagType.ByteSequence, attr.value.asInstanceOf[Array[java.lang.Byte]])
      // It appears from the code, that 'H' is encoded as a String as well? I'm not sure
      // how to pull that out here.
    }
  }

  /**
   * Parses a tab-separated string of attributes (tag:type:value) into a Seq of Attribute values.
   * @param tagStrings The String to be parsed
   * @return The parsed Attributes
   */
  def parseAttributes(tagStrings: String): Seq[Attribute] =
    tagStrings.split("\t").filter(_.length > 0).map(parseAttribute)

  /**
   * Extract the Attribute value from the corresponding String fragment stored in the
   * ADAMRecord.attributes field.
   *
   * @param encoded A three-part ':'-separated string, containing [attrTuple]:[type]:[value]
   * @return An Attribute value which has the three, extracted and correctly-typed parts of the encoded argument.
   * @throws IllegalArgumentException if the encoded string doesn't conform to the required regular
   *         expression ([A-Z]{2}:[AifZHB]:[^\t^]+)
   */
  def parseAttribute(encoded: String): Attribute = {
    attrRegex.matches(encoded) match {
      case Some(m) => createAttribute(m.group(1), m.group(2), m.group(3))
      case None =>
        throw new IllegalArgumentException(
          "attribute string \"%s\" doesn't match format attrTuple:type:value".format(encoded))
    }
  }

  private def createAttribute(attrTuple: (String, String, String)): Attribute = {
    val tagName = attrTuple._1
    val tagTypeString = attrTuple._2
    val valueStr = attrTuple._3

    // partial match, but these letters should be (per the SAM file format spec)
    // the only legal values of the tagTypeString anyway.
    val tagType = tagTypeString match {
      case "A" => TagType.Character
      case "i" => TagType.Integer
      case "f" => TagType.Float
      case "Z" => TagType.String
      case "H" => TagType.ByteSequence
      case "B" => TagType.NumericSequence
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
      case TagType.NumericSequence => valueStr.split(",").map(c => {
        if (c.contains(".")) java.lang.Float.valueOf(c)
        else Integer.valueOf(c)
      })
    }
  }
}

