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

import htsjdk.samtools.{ SAMFileHeader, SAMRecord }
import org.bdgenomics.adam.models.{ RecordGroupDictionary, SAMFileHeaderWritable, SequenceDictionary }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.AlignmentRecord

class AlignmentRecordConverter extends Serializable {

  /**
   * Converts a single record to FASTQ. FASTQ format is:
   *
   * @readName
   * sequence
   * +<optional readname>
   * ASCII quality scores
   *
   * @param adamRecord Read to convert to FASTQ.
   * @return Returns this read in string form.
   */
  def convertToFastq(adamRecord: AlignmentRecord): String = {
    "@" + adamRecord.getReadName + "\n" +
      adamRecord.getSequence + "\n" +
      "+\n" +
      adamRecord.getQual
  }

  /**
   * Converts a single ADAM record into a SAM record.
   *
   * @param adamRecord ADAM formatted alignment record to convert.
   * @param header SAM file header to use.
   * @return Returns the record converted to SAMtools format. Can be used for output to SAM/BAM.
   */
  def convert(adamRecord: AlignmentRecord, header: SAMFileHeaderWritable): SAMRecord = {

    // get read group dictionary from header
    val rgDict = header.header.getSequenceDictionary

    // attach header
    val builder: SAMRecord = new SAMRecord(header.header)

    // set canonically necessary fields
    builder.setReadName(adamRecord.getReadName.toString)
    builder.setReadString(adamRecord.getSequence)
    builder.setBaseQualityString(adamRecord.getQual)

    // set read group flags
    Option(adamRecord.getRecordGroupName)
      .map(_.toString)
      .map(rgDict.getSequenceIndex)
      .foreach(v => builder.setAttribute("RG", v.toString))
    Option(adamRecord.getRecordGroupLibrary)
      .foreach(v => builder.setAttribute("LB", v.toString))
    Option(adamRecord.getRecordGroupPlatformUnit)
      .foreach(v => builder.setAttribute("PU", v.toString))
    Option(adamRecord.getRecordGroupSample)
      .foreach(v => builder.setAttribute("SM", v.toString))

    // set the reference name, and alignment position, for mate
    Option(adamRecord.getMateContig)
      .map(_.getContigName)
      .map(_.toString)
      .foreach(builder.setMateReferenceName)
    Option(adamRecord.getMateAlignmentStart)
      .foreach(s => builder.setMateAlignmentStart(s.toInt + 1))

    // set flags
    Option(adamRecord.getReadPaired).foreach(p => {
      builder.setReadPairedFlag(p.booleanValue)

      // only set flags if read is paired
      if (p) {
        Option(adamRecord.getMateNegativeStrand)
          .foreach(v => builder.setMateNegativeStrandFlag(v.booleanValue))
        Option(adamRecord.getMateMapped)
          .foreach(v => builder.setMateUnmappedFlag(!v.booleanValue))
        Option(adamRecord.getProperPair)
          .foreach(v => builder.setProperPairFlag(v.booleanValue))
        Option(adamRecord.getFirstOfPair)
          .foreach(v => builder.setFirstOfPairFlag(v.booleanValue))
        Option(adamRecord.getSecondOfPair)
          .foreach(v => builder.setSecondOfPairFlag(v.booleanValue))
      }
    })
    Option(adamRecord.getDuplicateRead)
      .foreach(v => builder.setDuplicateReadFlag(v.booleanValue))
    Option(adamRecord.getReadMapped)
      .foreach(m => {
        builder.setReadUnmappedFlag(!m.booleanValue)

        // only set alignment flags if read is aligned
        if (m) {
          // if we are aligned, we must have a reference
          assert(adamRecord.getContig != null, "Cannot have null contig if aligned.")
          builder.setReferenceName(adamRecord.getContig.getContigName)

          // set the cigar, if provided
          Option(adamRecord.getCigar).map(_.toString).foreach(builder.setCigarString)
          // set the old cigar, if provided
          Option(adamRecord.getOldCigar).map(_.toString).foreach(v => builder.setAttribute("OC", v))
          // set mapping flags
          Option(adamRecord.getReadNegativeStrand)
            .foreach(v => builder.setReadNegativeStrandFlag(v.booleanValue))
          Option(adamRecord.getPrimaryAlignment)
            .foreach(v => builder.setNotPrimaryAlignmentFlag(!v.booleanValue))
          Option(adamRecord.getSupplementaryAlignment)
            .foreach(v => builder.setSupplementaryAlignmentFlag(v.booleanValue))
          Option(adamRecord.getStart)
            .foreach(s => builder.setAlignmentStart(s.toInt + 1))
          Option(adamRecord.getOldPosition)
            .foreach(s => builder.setAttribute("OP", s.toInt + 1))
          Option(adamRecord.getMapq).foreach(v => builder.setMappingQuality(v))
        } else {
          // mapping quality must be 0 if read is unmapped
          builder.setMappingQuality(0)
        }
      })
    Option(adamRecord.getFailedVendorQualityChecks)
      .foreach(v => builder.setReadFailsVendorQualityCheckFlag(v.booleanValue))
    Option(adamRecord.getMismatchingPositions)
      .map(_.toString)
      .foreach(builder.setAttribute("MD", _))

    // add all other tags
    if (adamRecord.getAttributes != null) {
      val mp = RichAlignmentRecord(adamRecord).tags
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
   * @return Converted SAM formatted record.
   */
  def createSAMHeader(sd: SequenceDictionary, rgd: RecordGroupDictionary): SAMFileHeader = {
    val samSequenceDictionary = sd.toSAMSequenceDictionary
    val samHeader = new SAMFileHeader
    samHeader.setSequenceDictionary(samSequenceDictionary)
    rgd.recordGroups.foreach(group => samHeader.addReadGroup(group.toSAMReadGroupRecord()))

    samHeader
  }
}
