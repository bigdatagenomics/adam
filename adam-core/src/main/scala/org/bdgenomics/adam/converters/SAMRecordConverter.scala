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
package org.bdgenomics.adam.converters

import net.sf.samtools.{ SAMReadGroupRecord, SAMRecord }

import org.bdgenomics.formats.avro.ADAMRecord
import scala.collection.JavaConverters._
import org.bdgenomics.adam.models.{ SequenceRecord, Attribute, RecordGroupDictionary, SequenceDictionary }
import org.bdgenomics.adam.util.AttributeUtils

class SAMRecordConverter extends Serializable {
  def convert(samRecord: SAMRecord, dict: SequenceDictionary, readGroups: RecordGroupDictionary): ADAMRecord = {

    val cigar: String = samRecord.getCigarString
    val startTrim = if (cigar == "*") {
      0
    } else {
      val count = cigar.takeWhile(_.isDigit).toInt
      val operator = cigar.dropWhile(_.isDigit).head

      if (operator == 'H') {
        count
      } else {
        0
      }
    }
    val endTrim = if (cigar.endsWith("H")) {
      // must reverse string as takeWhile is not implemented in reverse direction
      cigar.dropRight(1).reverse.takeWhile(_.isDigit).reverse.toInt
    } else {
      0
    }

    val builder: ADAMRecord.Builder = ADAMRecord.newBuilder
      .setReadName(samRecord.getReadName)
      .setSequence(samRecord.getReadString)
      .setCigar(cigar)
      .setBasesTrimmedFromStart(startTrim)
      .setBasesTrimmedFromEnd(endTrim)
      .setQual(samRecord.getBaseQualityString)

    // Only set the reference information if the read is aligned, matching the mate reference
    // This prevents looking up a -1 in the sequence dictionary
    val readReference: Int = samRecord.getReferenceIndex
    if (readReference != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
      builder.setContig(SequenceRecord.toADAMContig(dict(samRecord.getReferenceName).get))

      // set read alignment flag
      val start: Int = samRecord.getAlignmentStart
      assert(start != 0, "Start cannot equal 0 if contig is set.")
      builder.setStart((start - 1).asInstanceOf[Long])

      // set mapping quality
      val mapq: Int = samRecord.getMappingQuality

      if (mapq != SAMRecord.UNKNOWN_MAPPING_QUALITY) {
        builder.setMapq(mapq)
      }

      // set mapping flags
      // oddly enough, it appears that reads can show up with mapping info (mapq, cigar, position)
      // even if the read unmapped flag is set...
      if (samRecord.getReadUnmappedFlag) {
        builder.setReadMapped(false)
      } else {
        builder.setReadMapped(true)
        if (samRecord.getReadNegativeStrandFlag) {
          builder.setReadNegativeStrand(true)
        }
        if (!samRecord.getNotPrimaryAlignmentFlag) {
          builder.setPrimaryAlignment(true)
        } else {
          // if the read is not a primary alignment, it can be either secondary or supplementary
          // - secondary: not the best linear alignment
          // - supplementary: part of a chimeric alignment
          builder.setSupplementaryAlignment(samRecord.getSupplementaryAlignmentFlag)
          builder.setSecondaryAlignment(!samRecord.getSupplementaryAlignmentFlag)
        }
      }
    }

    // Position of the mate/next segment
    val mateReference: Int = samRecord.getMateReferenceIndex

    if (mateReference != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
      builder.setMateContig(SequenceRecord.toADAMContig(dict(samRecord.getMateReferenceName).get))

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
      if (samRecord.getReadFailsVendorQualityCheckFlag) {
        builder.setFailedVendorQualityChecks(true)
      }
    }

    if (samRecord.getAttributes != null) {
      var tags = List[Attribute]()
      samRecord.getAttributes.asScala.foreach {
        attr =>
          if (attr.tag == "MD") {
            builder.setMismatchingPositions(attr.value.toString)
          } else if (attr.tag == "OQ") {
            builder.setOrigQual(attr.value.toString)
          } else {
            tags ::= AttributeUtils.convertSAMTagAndValue(attr)
          }
      }
      builder.setAttributes(tags.mkString("\t"))
    }

    val recordGroup: SAMReadGroupRecord = samRecord.getReadGroup
    if (recordGroup != null) {
      Option(recordGroup.getRunDate).foreach(date => builder.setRecordGroupRunDateEpoch(date.getTime))

      builder.setRecordGroupName(recordGroup.getReadGroupId)
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
