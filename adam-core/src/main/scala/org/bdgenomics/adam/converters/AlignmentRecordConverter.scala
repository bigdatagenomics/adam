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
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }
import scala.collection.JavaConversions._

/**
 * This class contains methods to convert AlignmentRecords to other formats.
 */
class AlignmentRecordConverter extends Serializable {

  /**
   * Converts a single record to FASTQ. FASTQ format is:
   *
   * {{{
   * @readName
   * sequence
   * +<optional readname>
   * ASCII quality scores
   * }}}
   *
   * If the base qualities are unknown (qual is null or equals "*"), the quality
   * scores will be a repeated string of 'B's that is equal to the read length.
   *
   * @param adamRecord Read to convert to FASTQ.
   * @param maybeAddSuffix If true, check if a "/%d" suffix is attached to the
   *   read. If there is no suffix, a slash and the number of the read in the
   *   sequenced fragment is appended to the readname. Default is false.
   * @param outputOriginalBaseQualities If true and the original base quality
   *   field is set (SAM "OQ" tag), outputs the original qualities. Else,
   *   output the qual field. Defaults to false.
   * @return Returns this read in string form.
   */
  def convertToFastq(
    adamRecord: AlignmentRecord,
    maybeAddSuffix: Boolean = false,
    outputOriginalBaseQualities: Boolean = false): String = {
    val readNameSuffix =
      if (maybeAddSuffix &&
        !AlignmentRecordConverter.readNameHasPairedSuffix(adamRecord) &&
        adamRecord.getReadPaired) {
        "/%d".format(adamRecord.getReadInFragment + 1)
      } else {
        ""
      }

    //"B" is used to represent "unknown quality score"
    //https://en.wikipedia.org/wiki/FASTQ_format#cite_note-7
    //FastQ format quality score string must be the same length as the sequence string
    //https://en.wikipedia.org/wiki/FASTQ_format#Format
    val seqLength =
      if (adamRecord.getSequence == null)
        0
      else
        adamRecord.getSequence.length
    val qualityScores =
      if (outputOriginalBaseQualities && adamRecord.getOrigQual != null)
        if (adamRecord.getOrigQual == "*")
          "B" * seqLength
        else
          adamRecord.getOrigQual
      else if (adamRecord.getQual == null)
        "B" * seqLength
      else
        adamRecord.getQual

    "@%s%s\n%s\n+\n%s".format(
      adamRecord.getReadName,
      readNameSuffix,
      if (adamRecord.getReadNegativeStrand)
        Alphabet.dna.reverseComplement(adamRecord.getSequence)
      else
        adamRecord.getSequence,
      if (adamRecord.getReadNegativeStrand)
        qualityScores.reverse
      else
        qualityScores
    )
  }

  /**
   * Converts a single ADAM record into a SAM record.
   *
   * @param adamRecord ADAM formatted alignment record to convert.
   * @param header SAM file header to attach to the record.
   * @param rgd Dictionary describing the read groups that are in the RDD that
   *   this read is from.
   * @return Returns the record converted to htsjdk format. Can be used for output to SAM/BAM.
   */
  def convert(adamRecord: AlignmentRecord,
              header: SAMFileHeaderWritable,
              rgd: RecordGroupDictionary): SAMRecord = ConvertToSAMRecord.time {

    // attach header
    val builder: SAMRecord = new SAMRecord(header.header)

    // set canonically necessary fields
    builder.setReadName(adamRecord.getReadName)
    builder.setReadString(adamRecord.getSequence)
    adamRecord.getQual match {
      case null      => builder.setBaseQualityString("*")
      case s: String => builder.setBaseQualityString(s)
    }

    // set read group flags
    Option(adamRecord.getRecordGroupName)
      .foreach(v => {
        builder.setAttribute("RG", v)
        val rg = rgd(v)
        rg.library.foreach(v => builder.setAttribute("LB", v))
        rg.platformUnit.foreach(v => builder.setAttribute("PU", v))
      })

    // set the reference name, and alignment position, for mate
    Option(adamRecord.getMateContigName)
      .foreach(builder.setMateReferenceName)
    Option(adamRecord.getMateAlignmentStart)
      .foreach(s => builder.setMateAlignmentStart(s.toInt + 1))

    // set template length
    Option(adamRecord.getInferredInsertSize)
      .foreach(s => builder.setInferredInsertSize(s.toInt))

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
        Option(adamRecord.getReadInFragment == 0)
          .foreach(v => builder.setFirstOfPairFlag(v.booleanValue))
        Option(adamRecord.getReadInFragment == 1)
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
          require(adamRecord.getContigName != null, "Cannot have null contig if aligned.")
          builder.setReferenceName(adamRecord.getContigName)

          // set the cigar, if provided
          Option(adamRecord.getCigar).foreach(builder.setCigarString)
          // set the old cigar, if provided
          Option(adamRecord.getOldCigar).foreach(v => builder.setAttribute("OC", v))
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
      .foreach(builder.setAttribute("MD", _))
    Option(adamRecord.getOrigQual)
      .map(s => s.getBytes.map(v => (v - 33).toByte)) // not ascii, but short int
      .foreach(builder.setOriginalBaseQualities(_))
    Option(adamRecord.getOldCigar)
      .foreach(builder.setAttribute("OC", _))
    Option(adamRecord.getOldPosition)
      .foreach(i => builder.setAttribute("OP", i + 1))

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
  def createSAMHeader(sd: SequenceDictionary,
                      rgd: RecordGroupDictionary): SAMFileHeader = {
    val samSequenceDictionary = sd.toSAMSequenceDictionary
    val samHeader = new SAMFileHeader
    samHeader.setSequenceDictionary(samSequenceDictionary)
    rgd.recordGroups.foreach(group => samHeader.addReadGroup(group.toSAMReadGroupRecord()))

    samHeader
  }

  /**
   * Converts a fragment to a set of reads.
   *
   * @param fragment Fragment to convert.
   * @return The collection of alignments described by the fragment. If the fragment
   *         doesn't contain any alignments, this method will return one unaligned
   *         AlignmentRecord per sequence in the Fragment.
   */
  def convertFragment(fragment: Fragment): Iterable[AlignmentRecord] = {
    asScalaBuffer(fragment.getAlignments).toIterable
  }
}

/**
 * Singleton object to assist with converting AlignmentRecords.
 *
 * Singleton object exists due to cross reference from
 * org.bdgenomics.adam.rdd.read.AlignmentRecordRDDFunctions.
 */
private[adam] object AlignmentRecordConverter extends Serializable {

  /**
   * Checks to see if a read name has a index suffix.
   *
   * Read names frequently end in a "/%d" suffix, where the digit at the end
   * signifies the number of this read in the sequenced fragment. E.g., for an
   * Illumina paired-end protocol, the first read in the pair will have a "/1"
   * suffix, while the second read in the pair will have a "/2" suffix.
   *
   * @param adamRecord Record to check.
   * @return True if the read ends in a read number suffix.
   */
  def readNameHasPairedSuffix(adamRecord: AlignmentRecord): Boolean = {
    adamRecord.getReadName.length() > 2 &&
      adamRecord.getReadName.charAt(adamRecord.getReadName.length() - 2) == '/' &&
      (adamRecord.getReadName.charAt(adamRecord.getReadName.length() - 1) == '1' ||
        adamRecord.getReadName.charAt(adamRecord.getReadName.length() - 1) == '2')
  }
}
