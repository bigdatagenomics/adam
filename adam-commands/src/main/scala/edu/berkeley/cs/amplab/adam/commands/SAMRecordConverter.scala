/*
 * Copyright (c) 2013. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.commands

import net.sf.samtools.{SAMReadGroupRecord, SAMRecord}

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.adam.models.SequenceDictionary

class SAMRecordConverter extends Serializable {
  def convert(samRecord: SAMRecord, dict : SequenceDictionary): ADAMRecord = {

    val builder: ADAMRecord.Builder = ADAMRecord.newBuilder
      .setReferenceName(samRecord.getReferenceName)
      .setReferenceId(samRecord.getReferenceIndex)
      .setReferenceLength(dict(samRecord.getReferenceIndex).length)
      .setReferenceUrl(dict(samRecord.getReferenceIndex).url)
      .setReadName(samRecord.getReadName)
      .setSequence(samRecord.getReadString)
      .setCigar(samRecord.getCigarString)
      .setQual(samRecord.getBaseQualityString)

    val start: Int = samRecord.getAlignmentStart

    if (start != 0) {
      builder.setStart((start - 1).asInstanceOf[Long])
    }

    val mapq: Int = samRecord.getMappingQuality

    if (mapq != SAMRecord.UNKNOWN_MAPPING_QUALITY) {
      builder.setMapq(mapq)
    }

    // Position of the mate/next segment
    val mateReference: Int = samRecord.getMateReferenceIndex

    if (mateReference != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
      builder
        .setMateReferenceId(mateReference)
        .setMateReference(samRecord.getMateReferenceName)
        .setMateReferenceLength(dict(samRecord.getMateReferenceName).length)
        .setMateReferenceUrl(dict(samRecord.getMateReferenceName).url)

      val mateStart = samRecord.getMateAlignmentStart
      if (mateStart > 0) {
        // We subtract one here to be 0-based offset
        builder.setMateAlignmentStart(mateStart - 1)
      }
    }

    // The Avro scheme defines all flags as defaulting to 'false'. We only need to set the flags that are true.
    if (samRecord.getFlags != 0) {
      if (samRecord.getReadPairedFlag) {
        builder.setReadPaired(true)
        if (samRecord.getMateNegativeStrandFlag) {
          builder.setMateNegativeStrand(true)
        }
        if (!samRecord.getMateUnmappedFlag) {
          builder.setMateMapped(true)
        }
        if (samRecord.getProperPairFlag) {
          builder.setProperPair(true)
        }
        if (samRecord.getFirstOfPairFlag) {
          builder.setFirstOfPair(true)
        }
        if (samRecord.getSecondOfPairFlag) {
          builder.setSecondOfPair(true)
        }
      }
      if (samRecord.getDuplicateReadFlag) {
        builder.setDuplicateRead(true)
      }
      if (samRecord.getReadNegativeStrandFlag) {
        builder.setReadNegativeStrand(true)
      }
      if (!samRecord.getNotPrimaryAlignmentFlag) {
        builder.setPrimaryAlignment(true)
      }
      if (samRecord.getReadFailsVendorQualityCheckFlag) {
        builder.setFailedVendorQualityChecks(true)
      }
      if (!samRecord.getReadUnmappedFlag) {
        builder.setReadMapped(true)
      }
    }

    if (samRecord.getAttributes != null) {
      var attrs = List[String]()
      samRecord.getAttributes.asScala.foreach {
        attr =>
          if (attr.tag == "MD") {
            builder.setMismatchingPositions(attr.value.toString)
          } else {
            attrs ::= attr.tag + "=" + attr.value
          }
      }
      builder.setAttributes(attrs.mkString(","))
    }

    val recordGroup: SAMReadGroupRecord = samRecord.getReadGroup
    if (recordGroup != null) {
      Option(recordGroup.getRunDate) match {
        case Some(date) => builder.setRecordGroupRunDateEpoch(date.getTime)
        case None =>
      }
      recordGroup.getId
      builder.setRecordGroupId(recordGroup.getReadGroupId)
        .setRecordGroupSequencingCenter(recordGroup.getSequencingCenter)
        .setRecordGroupDescription(recordGroup.getDescription)
        .setRecordGroupFlowOrder(recordGroup.getFlowOrder)
        .setRecordGroupKeySequence(recordGroup.getKeySequence)
        .setRecordGroupLibrary(recordGroup.getLibrary)
        .setRecordGroupPredictedMedianInsertSize(recordGroup.getPredictedMedianInsertSize)
        .setRecordGroupPlatform(recordGroup.getPlatform)
        .setRecordGroupPlatformUnit(recordGroup.getPlatformUnit)
        .setRecordGroupSample(recordGroup.getSample)
    }

    builder.build
  }

}
