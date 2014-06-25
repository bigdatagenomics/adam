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

import org.bdgenomics.formats.avro.ADAMFeature

class BaseFeature(val feature: ADAMFeature) {
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
