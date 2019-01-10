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

import htsjdk.samtools.{
  SAMReadGroupRecord,
  SAMRecord,
  SAMUtils
}
import org.bdgenomics.utils.misc.Logging
import org.bdgenomics.adam.models.Attribute
import org.bdgenomics.adam.util.AttributeUtils
import org.bdgenomics.formats.avro.AlignmentRecord
import scala.collection.JavaConverters._

/**
 * Helper class for converting SAMRecords into AlignmentRecords.
 */
private[adam] class SAMRecordConverter extends Serializable with Logging {

  /**
   * Returns true if a tag should not be kept in the attributes field.
   *
   * The SAM/BAM format supports attributes, which is a key/value pair map. In
   * ADAM, we have promoted some of these fields to "primary" fields, so that we
   * can more efficiently access them. These include the MD tag, which describes
   * substitutions against the reference; the OQ tag, which describes the
   * original read base qualities; and the OP and OC tags, which describe the
   * original read alignment position and CIGAR.
   *
   * @param attrTag Tag name to check.
   * @return Returns true if the tag should be skipped.
   */
  private[converters] def skipTag(attrTag: String): Boolean = attrTag match {
    case "OQ" => true
    case "OP" => true
    case "OC" => true
    case "MD" => true
    case _    => false
  }

  /**
   * Converts a SAM record into an Avro AlignmentRecord.
   *
   * @param samRecord Record to convert.
   * @return Returns the original record converted into Avro.
   */
  def convert(samRecord: SAMRecord): AlignmentRecord = {
    try {
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

      val builder: AlignmentRecord.Builder = AlignmentRecord.newBuilder
        .setReadName(samRecord.getReadName)
        .setSequence(samRecord.getReadString)
        .setCigar(cigar)
        .setBasesTrimmedFromStart(startTrim)
        .setBasesTrimmedFromEnd(endTrim)
        .setOriginalQuality(SAMUtils.phredToFastq(samRecord.getOriginalBaseQualities))

      // if the quality string is "*", then we null it in the record
      // or, in other words, we only set the quality string if it is not "*"
      val qual = samRecord.getBaseQualityString
      if (qual != "*") {
        builder.setQuality(qual)
      }

      // Only set the reference information if the read is aligned, matching the mate reference
      // This prevents looking up a -1 in the sequence dictionary
      val readReference: Int = samRecord.getReferenceIndex
      if (readReference != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
        builder.setReferenceName(samRecord.getReferenceName)

        // set read alignment flag
        val start: Int = samRecord.getAlignmentStart
        assert(start != 0, "Start cannot equal 0 if reference is set.")
        builder.setStart(start - 1L)

        // set OP and OC flags, if applicable
        if (samRecord.getAttribute("OP") != null) {
          builder.setOriginalStart(samRecord.getIntegerAttribute("OP").toLong - 1)
          builder.setOriginalCigar(samRecord.getStringAttribute("OC"))
        }

        val end = start.toLong - 1 + samRecord.getCigar.getReferenceLength
        builder.setEnd(end)
        // set mapping quality
        val mapq: Int = samRecord.getMappingQuality

        if (mapq != SAMRecord.UNKNOWN_MAPPING_QUALITY) {
          builder.setMappingQuality(mapq)
        }

      }

      // set mapping flags
      // oddly enough, it appears that reads can show up with mapping
      // info (mapq, cigar, position)
      // even if the read unmapped flag is set...

      // While the meaning of the ReadMapped, ReadNegativeStand,
      // PrimaryAlignmentFlag and SupplementaryAlignmentFlag
      // are unclear when the read is not mapped or reference is not defined,
      // it is nonetheless favorable to set these flags in the ADAM file
      // in same way as they appear in the input BAM inorder to match exactly
      // the statistics output by other programs, specifically Samtools Flagstat

      builder.setReadMapped(!samRecord.getReadUnmappedFlag)
      builder.setReadNegativeStrand(samRecord.getReadNegativeStrandFlag)
      builder.setPrimaryAlignment(!samRecord.getNotPrimaryAlignmentFlag)
      builder.setSupplementaryAlignment(samRecord.getSupplementaryAlignmentFlag)

      // Position of the mate/next segment
      val mateReference: Int = samRecord.getMateReferenceIndex

      if (mateReference != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
        builder.setMateReferenceName(samRecord.getMateReferenceName)

        val mateStart = samRecord.getMateAlignmentStart
        if (mateStart > 0) {
          // We subtract one here to be 0-based offset
          builder.setMateAlignmentStart(mateStart - 1L)
        }
      }

      // The Avro scheme defines all flags as defaulting to 'false'. We only
      // need to set the flags that are true.
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
            builder.setReadInFragment(0)
          }
          if (samRecord.getSecondOfPairFlag) {
            builder.setReadInFragment(1)
          }
        }
        if (samRecord.getDuplicateReadFlag) {
          builder.setDuplicateRead(true)
        }
        if (samRecord.getReadFailsVendorQualityCheckFlag) {
          builder.setFailedVendorQualityChecks(true)
        }
      }

      var tags = List[Attribute]()
      val tlen = samRecord.getInferredInsertSize
      if (tlen != 0) {
        builder.setInsertSize(tlen.toLong)
      }
      if (samRecord.getAttributes != null) {
        samRecord.getAttributes.asScala.foreach {
          attr =>
            if (attr.tag == "MD") {
              builder.setMismatchingPositions(attr.value.toString)
            } else if (!skipTag(attr.tag)) {
              tags ::= AttributeUtils.convertSAMTagAndValue(attr)
            }
        }
      }
      if (tags.nonEmpty) {
        builder.setAttributes(tags.mkString("\t"))
      }

      val recordGroup: SAMReadGroupRecord = samRecord.getReadGroup
      if (recordGroup != null) {
        builder.setReadGroupId(recordGroup.getReadGroupId)
          .setReadGroupSampleId(recordGroup.getSample)
      }

      builder.build
    } catch {
      case t: Throwable => {
        log.error("Conversion of read: " + samRecord + " failed.")
        throw t
      }
    }
  }
}
