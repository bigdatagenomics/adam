/*
 * Copyright (c) 2014. Cloudera, Inc.
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

package org.bdgenomics.adam.rdd.features

import org.bdgenomics.adam.avro.{ ADAMContig, ADAMFeature }
import org.broad.tribble.bed.BEDCodec
import org.broad.tribble.bed.BEDCodec.StartOffset
import org.bdgenomics.adam.models.{ BaseFeature, NarrowPeakFeature, BEDFeature }

trait FeatureParser[FT <: BaseFeature] extends Serializable {
  def parse(line: String): FT
}

object BEDParser {
  val tribbleCodec = new BEDCodec(StartOffset.ZERO) // TODO(laserson): serializable?
}

class BEDParser extends FeatureParser[BEDFeature] {
  def parse(line: String): BEDFeature = {
    val tribbleFeature = BEDParser.tribbleCodec.decode(line)
    val fb = ADAMFeature.newBuilder()
    val cb = ADAMContig.newBuilder()
    cb.setContigName(tribbleFeature.getChr())
    fb.setContig(cb.build())
    fb.setStart(tribbleFeature.getStart())
    fb.setEnd(tribbleFeature.getEnd())
    fb.setTrackName(tribbleFeature.getName())
    fb.setValue(tribbleFeature.getScore())
    fb.setStrand(tribbleFeature.getStrand() match {
      case org.broad.tribble.annotation.Strand.NEGATIVE => org.bdgenomics.adam.avro.Strand.Reverse
      case org.broad.tribble.annotation.Strand.POSITIVE => org.bdgenomics.adam.avro.Strand.Forward
      case org.broad.tribble.annotation.Strand.NONE     => org.bdgenomics.adam.avro.Strand.Independent
    })
    // fb.setThickStart(...) -- Tribble BEDCodec doesn't parse thickStart
    // fb.setThickEnd(...) -- Tribble BEDCodec doesn't parse thickEnd
    fb.setItemRgb(tribbleFeature.getColor().toString())
    // TODO(laserson): decide how to parse these
    // fb.setBlockCount()
    // fb.setBlockSizes()
    // fb.setBlockStarts()
    new BEDFeature(fb.build())
  }
}

// TODO(laserson): finish narrowPeak parser
class NarrowPeakParser extends FeatureParser[NarrowPeakFeature] {

  def parse(line: String): NarrowPeakFeature = {
    val fb = ADAMFeature.newBuilder()
    new NarrowPeakFeature(fb.build())
  }
}
