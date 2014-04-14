package org.bdgenomics.adam.rdd.features

import org.bdgenomics.adam.avro.{ADAMContig, ADAMFeature}
import org.broad.tribble.bed.BEDCodec
import org.broad.tribble.bed.BEDCodec.StartOffset

trait FeatureParser[FT <: BaseFeature] extends Serializable {
  def parse(line: String) : FT
}

class BEDParser extends FeatureParser[BEDFeature] {
  val tribbleCodec = new BEDCodec(StartOffset.ZERO) // TODO(laserson): serializable?

  def parse(line: String): BEDFeature = {
    val tribbleFeature = tribbleCodec.decode(line)
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
      case org.broad.tribble.annotation.Strand.NONE => org.bdgenomics.adam.avro.Strand.Independent
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
