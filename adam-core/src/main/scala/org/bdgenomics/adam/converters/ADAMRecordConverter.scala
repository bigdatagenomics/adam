/*
 * Copyright (c) 2014. Regents of the University of California
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
package org.bdgenomics.adam.converters

import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.models.{
  Attribute,
  SAMFileHeaderWritable,
  SequenceDictionary,
  SequenceRecord,
  RecordGroupDictionary
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichADAMRecord
import net.sf.samtools.{ SAMReadGroupRecord, SAMRecord, SAMFileHeader }
import java.util.Date
import scala.collection.JavaConverters._

class ADAMRecordConverter extends Serializable {

  /**
   * Converts a single ADAM record into a SAM record.
   *
   * @param adamRecord ADAM formatted alignment record to convert.
   * @param dict Reference sequence dictionary to use for conversion.
   * @param readGroups Dictionary containing record groups.
   * @return Returns the record converted to SAMtools format. Can be used for output to SAM/BAM.
   */
  def convert(adamRecord: ADAMRecord, header: SAMFileHeaderWritable): SAMRecord = {

    assert(adamRecord.getRecordGroupName != null, "can't get record group name if not set")

    // start building read group data for header
    val readGroupFromADAM: SAMReadGroupRecord = new SAMReadGroupRecord(adamRecord.getRecordGroupName)

    Option(adamRecord.getRecordGroupSequencingCenter).foreach(v => readGroupFromADAM.setSequencingCenter(v.toString))
    Option(adamRecord.getRecordGroupRunDateEpoch).foreach(v => readGroupFromADAM.setRunDate(new Date(v)))
    Option(adamRecord.getRecordGroupDescription).foreach(v => readGroupFromADAM.setDescription(v))
    Option(adamRecord.getRecordGroupFlowOrder).foreach(v => readGroupFromADAM.setFlowOrder(v))
    Option(adamRecord.getRecordGroupKeySequence).foreach(v => readGroupFromADAM.setKeySequence(v))
    Option(adamRecord.getRecordGroupLibrary).foreach(v => readGroupFromADAM.setLibrary(v))
    Option(adamRecord.getRecordGroupPredictedMedianInsertSize).foreach(v => readGroupFromADAM.setPredictedMedianInsertSize(v))
    Option(adamRecord.getRecordGroupPlatform).foreach(v => readGroupFromADAM.setPlatform(v))
    Option(adamRecord.getRecordGroupPlatformUnit).foreach(v => readGroupFromADAM.setPlatformUnit(v))
    Option(adamRecord.getRecordGroupSample).foreach(v => readGroupFromADAM.setSample(v))

    // attach header
    val builder: SAMRecord = new SAMRecord(header.header)

    // set canonically necessary fields
    builder.setReadName(adamRecord.getReadName.toString)
    builder.setReadString(adamRecord.getSequence)
    builder.setBaseQualityString(adamRecord.getQual)

    // set the cigar, if provided
    Option(adamRecord.getCigar).map(_.toString).foreach(builder.setCigarString)

    // set the reference name, alignment position, and mapping quality, for this read and mate
    Option(adamRecord.getContig)
      .map(_.getContigName)
      .foreach(v => builder.setReferenceName(v))
    Option(adamRecord.getStart)
      .filter(_ > 0)
      .foreach(s => builder.setAlignmentStart(s.toInt + 1))
    Option(adamRecord.getMapq).foreach(v => builder.setMappingQuality(v))
    Option(adamRecord.getMateReference)
      .map(_.toString)
      .foreach(builder.setMateReferenceName)
    Option(adamRecord.getMateAlignmentStart)
      .filter(_ > 0)
      .foreach(s => builder.setMateAlignmentStart(s.toInt + 1))

    // set flags
    if (adamRecord.getReadPaired) {
      builder.setReadPairedFlag(true)
      if (adamRecord.getMateNegativeStrand) {
        builder.setMateNegativeStrandFlag(true)
      }
      if (!adamRecord.getMateMapped) {
        builder.setMateUnmappedFlag(true)
      }
      if (adamRecord.getProperPair) {
        builder.setProperPairFlag(true)
      }
      if (adamRecord.getFirstOfPair) {
        builder.setFirstOfPairFlag(true)
      }
      if (adamRecord.getSecondOfPair) {
        builder.setSecondOfPairFlag(true)
      }
    }
    if (adamRecord.getDuplicateRead) {
      builder.setDuplicateReadFlag(true)
    }
    if (adamRecord.getReadNegativeStrand) {
      builder.setReadNegativeStrandFlag(true)
    }
    if (!adamRecord.getPrimaryAlignment) {
      builder.setNotPrimaryAlignmentFlag(true)
    }
    if (adamRecord.getFailedVendorQualityChecks) {
      builder.setReadFailsVendorQualityCheckFlag(true)
    }
    if (!adamRecord.getReadMapped) {
      builder.setReadUnmappedFlag(true)
    }
    if (adamRecord.getMismatchingPositions != null) {
      builder.setAttribute("MD", adamRecord.getMismatchingPositions)
    }
    if (adamRecord.getAttributes != null) {
      val mp = RichADAMRecord(adamRecord).tags
      mp.foreach(a => {
        builder.setAttribute(a.tag, a.value)
      })
    }

    // return sam record 
    builder
  }

  /**
   * Creates a SAM formatted header. This can be used with SAM or BAM files.
   *
   * @param sd Reference sequence dictionary to use for conversion.
   * @param rgd Dictionary containing record groups.
   * @param rgfa SAM formatted read group record.
   * @return Converted SAM formatted record.
   */
  def createSAMHeader(sd: SequenceDictionary, rgd: RecordGroupDictionary): SAMFileHeader = {
    val samSequenceDictionary = sd.toSAMSequenceDictionary
    val samHeader = new SAMFileHeader
    samHeader.setSequenceDictionary(samSequenceDictionary)
    rgd.readGroups.foreach(kv => {
      val (_, name) = kv
      val nextMember = new SAMReadGroupRecord(name.toString)
      samHeader.addReadGroup(nextMember)
    })

    samHeader
  }
}
