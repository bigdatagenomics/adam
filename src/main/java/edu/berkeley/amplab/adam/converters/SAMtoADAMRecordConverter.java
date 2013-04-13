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

package edu.berkeley.amplab.adam.converters;

import edu.berkeley.amplab.adam.avro.ADAMRecord;
import edu.berkeley.amplab.adam.avro.ADAMReferencePosition;
import net.sf.samtools.Cigar;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMSequenceRecord;

/**
 * Converts SAMRecord to ADAMRecord
 */
public class SAMtoADAMRecordConverter implements Converter<SAMRecord, ADAMRecord> {

  @Override
  public ADAMRecord convert(SAMRecord samRecord) {
    int referenceIdx = samRecord.getReferenceIndex();
    SAMSequenceDictionary sequenceDictionary = samRecord.getHeader().getSequenceDictionary();
    SAMSequenceRecord sequenceRecord = sequenceDictionary.getSequence(referenceIdx);

    ADAMRecord.Builder recordBuilder =
        ADAMRecord.newBuilder()
                  .setReferenceName(sequenceRecord.getSequenceName())
                  .setReferenceLength(sequenceRecord.getSequenceLength())
                  .setStart(samRecord.getAlignmentStart())
                  .setEnd(samRecord.getAlignmentEnd())
                  .setMapq(samRecord.getMappingQuality())
                  .setTlen(samRecord.getAlignmentEnd());

    // optional fields
    // The read name
    String readName = samRecord.getReadName();
    if (readName != null && !"*".equals(readName)) {
      recordBuilder.setReadName(readName);
    } else {
      recordBuilder.setReadName(null);
    }

    // The read string
    String readBases = samRecord.getReadString();
    if (readBases != null && !"*".equals(readBases)) {
      recordBuilder.setSequence(readBases);
    }

    // The quality string
    String quality = samRecord.getBaseQualityString();
    if (quality != null && !"*".equals(quality)) {
      recordBuilder.setQual(quality);
    }

    // Position of the mate/next segment
    Integer mateReference = samRecord.getMateReferenceIndex();
    if (mateReference != -1) {
      recordBuilder.setNextReference(
          ADAMReferencePosition.newBuilder()
                               .setPosition(samRecord.getMateAlignmentStart())
                               .setReferenceName(samRecord.getMateReferenceName())
                               .build());
    }

    // Flags
    SAMtoADAMFlagConverter flagConverter = new SAMtoADAMFlagConverter();
    recordBuilder.setFlags(flagConverter.convert(samRecord));

    // Cigar
    Cigar cigar = samRecord.getCigar();
    if (cigar != null) {
      SAMtoADAMCigarConverter cigarConverter = new SAMtoADAMCigarConverter();
      recordBuilder.setCigar(cigarConverter.convert(cigar));
    }

    // TODO: Optional Attributes -- samRecord.getAttributes()

    return recordBuilder.build();
  }
}
