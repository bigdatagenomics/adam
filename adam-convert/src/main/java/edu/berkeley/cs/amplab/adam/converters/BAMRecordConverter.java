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

package edu.berkeley.cs.amplab.adam.converters;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord;
import net.sf.samtools.SAMReadGroupRecord;
import net.sf.samtools.SAMRecord;

import java.util.Map;

/**
 * Converts SAMRecord to ADAMRecord
 */
public class BAMRecordConverter implements Function<SAMRecord, ADAMRecord> {

    @Override
    public ADAMRecord apply(SAMRecord samRecord) {
        ADAMRecord.Builder builder = ADAMRecord.newBuilder()
                .setReferenceName(samRecord.getReferenceName())
                .setReadName(samRecord.getReadName())
                .setSequence(samRecord.getReadString())
                .setQual(samRecord.getBaseQualityString())
                .setCigar(samRecord.getCigarString());

        int start = samRecord.getAlignmentStart();
        if (start != 0) {
            // Alignment start is 1-based. Sigh.
            builder.setStart((long) (start - 1));
        }
        int end = samRecord.getAlignmentEnd();
        if (end != 0) {
            // Alignment end is 1-based. Sigh.
            builder.setEnd((long) (end - 1));
        }
        int mapq = samRecord.getMappingQuality();
        if (mapq != SAMRecord.UNKNOWN_MAPPING_QUALITY) {
            builder.setMapq((long) mapq);
        }

        // Position of the mate/next segment
        Integer mateReference = samRecord.getMateReferenceIndex();
        if (mateReference != -1) {
            builder.setMateReference(samRecord.getMateReferenceName())
                    .setMateAlignmentStart((long) samRecord.getMateAlignmentStart());
        }

        // Flags
        if (samRecord.getFlags() != 0) {
            if (samRecord.getReadPairedFlag()) {
                builder.setReadPairedFlag(true);
                // Check the flags that makes sense only when read-paired
                if (samRecord.getMateNegativeStrandFlag()) {
                    builder.setMateNegativeStrandFlag(true);
                }
                if (samRecord.getMateUnmappedFlag()) {
                    builder.setMateUnmappedFlag(true);
                }
                if (samRecord.getProperPairFlag()) {
                    builder.setProperPairFlag(true);
                }
                if (samRecord.getFirstOfPairFlag()) {
                    builder.setFirstOfPairFlag(true);
                }
                if (samRecord.getSecondOfPairFlag()) {
                    builder.setSecondOfPairFlag(true);
                }
            }

            if (samRecord.getDuplicateReadFlag()) {
                builder.setDuplicateReadFlag(true);
            }
            if (samRecord.getReadNegativeStrandFlag()) {
                builder.setReadNegativeStrandFlag(true);
            }
            if (samRecord.getNotPrimaryAlignmentFlag()) {
                builder.setNotPrimaryAlignmentFlag(true);
            }
            if (samRecord.getReadFailsVendorQualityCheckFlag()) {
                builder.setReadFailedVendorQualityCheckFlag(true);
            }
            if (samRecord.getReadUnmappedFlag()) {
                builder.setReadUnmappedFlag(true);
            }
        }

        if (samRecord.getAttributes() != null) {
            // Flatten the attributes into a string for now...e.g. one=1;two=2
            Joiner.MapJoiner attrJoiner = Joiner.on(";").withKeyValueSeparator("=");
            Map<String, String> tagValueMap = Maps.newHashMap();
            for (SAMRecord.SAMTagAndValue tv : samRecord.getAttributes()) {
                tagValueMap.put(tv.tag, tv.value.toString());
            }
            builder.setAttributes(attrJoiner.join(tagValueMap));
        }

        SAMReadGroupRecord recordGroup = samRecord.getReadGroup();
        if (recordGroup != null) {
            builder.setRecordGroupId(recordGroup.getReadGroupId());
        }

        return builder.build();
    }

}
