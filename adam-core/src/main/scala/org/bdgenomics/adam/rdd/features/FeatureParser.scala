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
package org.bdgenomics.adam.rdd.features

import org.bdgenomics.formats.avro.{ ADAMContig, ADAMFeature, Strand }
import org.bdgenomics.adam.models.{ BaseFeature, NarrowPeakFeature, BEDFeature }

trait FeatureParser[FT <: BaseFeature] extends Serializable {
  def parse(line: String): FT
}

class BEDParser extends FeatureParser[BEDFeature] {
  def parse(line: String): BEDFeature = {
    val fields = line.split("\t")
    assert(fields.length >= 3, "BED line had less than 3 fields")
    val fb = ADAMFeature.newBuilder()
    val cb = ADAMContig.newBuilder()
    cb.setContigName(fields(0))
    fb.setContig(cb.build())
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)
    if (fields.length > 3) fb.setTrackName(fields(3))
    if (fields.length > 4) fb.setValue(fields(4) match {
      case "." => null
      case _   => fields(4).toDouble
    })
    if (fields.length > 5) fb.setStrand(fields(5) match {
      case "+" => Strand.Forward
      case "-" => Strand.Reverse
      case _   => Strand.Independent
    })
    //    if (fields.length > 6) fb.setThickStart(fields(6).toLong)
    //    if (fields.length > 7) fb.setThickEnd(fields(7).toLong)
    //    if (fields.length > 8) fb.setItemRgb(fields(8))
    //    if (fields.length > 9) fb.setBlockCount(fields(9).toLong)
    //    if (fields.length > 10) fb.setBlockSizes(fields(10).split(",").map(new java.lang.Long(_)).toList)
    //    if (fields.length > 11) fb.setBlockStarts(fields(11).split(",").map(new java.lang.Long(_)).toList)
    new BEDFeature(fb.build())
  }
}

// TODO(laserson): finish narrowPeak parser
class NarrowPeakParser extends FeatureParser[NarrowPeakFeature] {
  def parse(line: String): NarrowPeakFeature = {
    val fields = line.split("\t")
    assert(fields.length >= 3, "narrowPeak line had less than 3 fields")
    val fb = ADAMFeature.newBuilder()
    val cb = ADAMContig.newBuilder()
    cb.setContigName(fields(0))
    fb.setContig(cb.build())
    fb.setStart(fields(1).toLong)
    fb.setEnd(fields(2).toLong)
    if (fields.length > 3) fb.setTrackName(fields(3))
    if (fields.length > 4) fb.setValue(fields(4) match {
      case "." => null
      case _   => fields(4).toDouble
    })
    if (fields.length > 5) fb.setStrand(fields(5) match {
      case "+" => Strand.Forward
      case "-" => Strand.Reverse
      case _   => Strand.Independent
    })
    if (fields.length > 6) fb.setSignalValue(fields(6).toDouble)
    if (fields.length > 7) fb.setPValue(fields(7).toDouble)
    if (fields.length > 8) fb.setQValue(fields(8).toDouble)
    if (fields.length > 9) fb.setPeak(fields(9).toLong)
    new NarrowPeakFeature(fb.build())
  }
}
