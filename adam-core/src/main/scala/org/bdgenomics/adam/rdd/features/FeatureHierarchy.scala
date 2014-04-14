package org.bdgenomics.adam.rdd.features

import org.bdgenomics.adam.avro.ADAMFeature

class BaseFeature(feature: ADAMFeature) {
  def featureId = feature.getFeatureId
  def contig = feature.getContig
  def start = feature.getStart
  def end = feature.getEnd
}

class BEDFeature(feature: ADAMFeature) extends BaseFeature(feature) {
  def name = feature.getTrackName
  def score = feature.getValue
  def strand = feature.getStrand
  def thickStart = feature.getThickStart
  def thickEnd = feature.getThickEnd
  def itemRgb = feature.getItemRgb
  def blockCount = feature.getBlockCount
  def blockSizes = feature.getBlockSizes
  def blockStarts = feature.getBlockStarts
}

class NarrowPeakFeature(feature: ADAMFeature) extends BaseFeature(feature) {
  def name = feature.getTrackName
  def score = feature.getValue
  def strand = feature.getStrand
  def signalValue = feature.getSignalValue
  def pValue = feature.getPValue
  def qValue = feature.getQValue
  def peak = feature.getPeak
}
