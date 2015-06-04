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

import org.bdgenomics.formats.avro.{ Strand, Feature }

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.collection._

object BaseFeature {

  def strandChar(strand: Strand): Char =
    strand match {
      case Strand.Forward     => '+'
      case Strand.Reverse     => '-'
      case Strand.Independent => '.'
    }

  def frameChar(feature: Feature): Char = {
    val opt: Map[String, String] = feature.getAttributes
    opt.get("frame").map(_.charAt(0)).getOrElse('.')
  }

  def attributeString(feature: Feature): String =
    feature.getAttributes.mkString(",")

  def attrs(f: Feature): Map[String, String] =
    JavaConversions.mapAsScalaMap(f.getAttributes)

  def attributeString(feature: Feature, attrName: String): Option[String] =
    attrs(feature).get(attrName).map(_.toString)

  def attributeLong(feature: Feature, attrName: String): Option[Long] =
    attrs(feature).get(attrName).map(_.toString).map(_.toLong)

  def attributeDouble(feature: Feature, attrName: String): Option[Double] =
    attrs(feature).get(attrName).map(_.toString).map(_.toDouble)

  def attributeInt(feature: Feature, attrName: String): Option[Int] =
    attrs(feature).get(attrName).map(_.toString).map(_.toInt)
}

class BaseFeature(val feature: Feature) extends Serializable {
  override def toString: String = feature.toString
}

class GTFFeature(override val feature: Feature) extends BaseFeature(feature) {
  def getSeqname: String = feature.getContig.getContigName.toString

  def getSource: String = feature.getSource.toString

  def getFeature: String = feature.getFeatureType.toString

  def getStart: Long = feature.getStart

  def getEnd: Long = feature.getEnd

  def getScore: Double = feature.getValue

  def getStrand: Char = BaseFeature.strandChar(feature.getStrand)

  def getFrame: Char = BaseFeature.frameChar(feature)

  def getAttribute: String = BaseFeature.attributeString(feature)
}

object BEDFeature {

  def parseBlockSizes(attribute: Option[String]): Array[Int] =
    attribute.getOrElse("").split(",").map(_.toInt)

  def parseBlockStarts(attribute: Option[String]): Array[Int] =
    attribute.getOrElse("").split(",").map(_.toInt)
}

class BEDFeature(override val feature: Feature) extends BaseFeature(feature) {
  def getChrom: String = feature.getContig.getContigName.toString

  def getChromStart: Long = feature.getStart

  def getChromEnd: Long = feature.getEnd

  def getName: String = feature.getFeatureId.toString

  def getScore: Double = feature.getValue

  def getStrand: Char = BaseFeature.strandChar(feature.getStrand)

  def getThickStart: Option[Long] = BaseFeature.attributeLong(feature, "thickStart")

  def getThickEnd: Option[Long] = BaseFeature.attributeLong(feature, "thickEnd")

  def getItemRGB: Option[String] = BaseFeature.attributeString(feature, "itemRgb")

  def getBlockCount: Option[Int] = BaseFeature.attributeInt(feature, "blockCount")

  def getBlockSizes: Array[Int] = BEDFeature.parseBlockSizes(BaseFeature.attributeString(feature, "blockSizes"))

  def getBlockStarts: Array[Int] = BEDFeature.parseBlockStarts(BaseFeature.attributeString(feature, "blockStarts"))
}

/**
 * See: http://genome.ucsc.edu/FAQ/FAQformat.html#format12
 */
class NarrowPeakFeature(override val feature: Feature) extends BaseFeature(feature) {
  def getChrom: String = feature.getContig.getContigName.toString

  def getChromStart: Long = feature.getStart

  def getChromEnd: Long = feature.getEnd

  def getName: String = feature.getFeatureId.toString

  def getScore: Double = feature.getValue

  def getStrand: Char = BaseFeature.strandChar(feature.getStrand)

  def getSignalValue: Option[Double] = BaseFeature.attributeDouble(feature, "signalValue")

  def getPValue: Option[Double] = BaseFeature.attributeDouble(feature, "pValue")

  def getQValue: Option[Double] = BaseFeature.attributeDouble(feature, "qValue")

  def getPeak: Option[Long] = BaseFeature.attributeLong(feature, "peak")
}
