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

package edu.berkeley.amplab.adam.modules.pileup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import edu.berkeley.amplab.adam.avro.PileupFragment;
import edu.berkeley.amplab.adam.avro.PileupFragmentId;
import edu.berkeley.amplab.adam.avro.ReadFragment;
import edu.berkeley.amplab.adam.avro.SequenceFragment;
import edu.berkeley.amplab.adam.modules.SequencePartitioner;
import net.sf.samtools.SAMRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import parquet.hadoop.util.ContextUtil;

import java.io.IOException;
import java.util.Map;

public class PileupReadMapper extends Mapper<Void, GenericRecord,
        AvroKey<PileupFragmentId>, AvroValue<PileupFragment>> {

    private SequencePartitioner sequencePartitioner;
    private AvroKey<PileupFragmentId> key = new AvroKey<PileupFragmentId>();
    private AvroValue<PileupFragment> value = new AvroValue<PileupFragment>();

    private Long minMapq;
    private Map<String, PileupCounters> checkFlags = ImmutableMap.of(
            "duplicateReadFlag", PileupCounters.DUPLICATE_READS_IGNORED,
            "notPrimaryAlignmentFlag", PileupCounters.NOT_PRIMARY_ALIGNMENT_IGNORED,
            "readUnmappedFlag", PileupCounters.UNMAPPED_READS_IGNORED,
            "readFailedVendorQualityCheckFlag", PileupCounters.FAILED_QUALITY_CHECK_READS_IGNORED);

    private Map<String, String> referenceMap = Maps.newHashMap();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = ContextUtil.getConfiguration(context);
        minMapq = config.getLong(Pileup.MIN_MAPQ_CONFIG, Pileup.MIN_MAPQ_DEFAULT);
        Long step = config.getLong(Pileup.PILEUP_STEP, Pileup.PILEUP_STEP_DEFAULT);
        sequencePartitioner = new SequencePartitioner(step);

        String referenceMapConfig = config.get(Pileup.PILEUP_REFERENCE_MAP);
        if (referenceMapConfig != null) {
            referenceMap = Pileup.REFERENCE_NAME_SPLITTER.split(referenceMapConfig);
        }
    }

    private boolean checkFlags(GenericRecord record, Context context) {
        boolean flagsGood = true;
        for (Map.Entry<String, PileupCounters> entry : checkFlags.entrySet()) {
            Boolean flag = (Boolean) record.get(entry.getKey());
            if (flag != null && flag) {
                context.getCounter(entry.getValue()).increment(1);
                // Check all flags before returning in order to ensure counters are accurate...
                flagsGood = false;
            }
        }
        return flagsGood;
    }

    @Override
    protected void map(Void ignore, GenericRecord record, Context context)
            throws IOException, InterruptedException {
        if (!checkFlags(record, context)) {
            return;
        }
        Long mapq = (Long) record.get("mapq");
        if (mapq == null || mapq == SAMRecord.UNKNOWN_MAPPING_QUALITY || mapq < minMapq) {
            context.getCounter(PileupCounters.LOW_MAPQ_RECORDS_IGNORED).increment(1);
            return;
        }
        CharSequence sequence = (CharSequence) record.get("sequence");
        if (sequence == null) {
            context.getCounter(PileupCounters.MISSING_READ_SEQUENCE).increment(1);
            return;
        }
        CharSequence qual = (CharSequence) record.get("qual");
        if (qual == null) {
            context.getCounter(PileupCounters.MISSING_READ_QUALITY).increment(1);
            // Note it but keep on processing...
        }
        if (qual != null && sequence.length() != qual.length()) {
            context.getCounter(PileupCounters.INVALID_READS_RECORDS).increment(1);
            return;
        }
        Long start = (Long) record.get("start");
        if (start == null) {
            context.getCounter(PileupCounters.MISSING_READ_START).increment(1);
            return;
        }
        CharSequence referenceName = (CharSequence) record.get("referenceName");
        if (referenceName == null) {
            context.getCounter(PileupCounters.MISSING_REFERENCE_NAME).increment(1);
            return;
        }

        // Check if we remap this reference name to another
        CharSequence remapName = referenceMap.get(referenceName.toString());
        if (remapName != null) {
            referenceName = remapName;
        }

        CharSequence readName = (CharSequence) record.get("readName");
        if (readName == null) {
            context.getCounter(PileupCounters.MISSING_READ_NAME).increment(1);
            return;
        }

        Map<Long, SequenceFragment> sequenceFragments = sequencePartitioner.partition(start, sequence.toString());
        Map<Long, SequenceFragment> qualFragments = null;
        if (qual != null) {
            qualFragments = sequencePartitioner.partition(start, qual.toString());
        }

        PileupFragmentId.Builder fragmentIdBuilder = PileupFragmentId.newBuilder()
                .setReferenceName(referenceName);

        for (Long bucketId : sequenceFragments.keySet()) {
            ReadFragment.Builder readFragmentBuilder = ReadFragment.newBuilder()
                    .setReadName(readName)
                    .setSequence(sequenceFragments.get(bucketId));
            if (qualFragments != null) {
                readFragmentBuilder.setBaseQuals(qualFragments.get(bucketId));
            }
            PileupFragment fragment = PileupFragment.newBuilder()
                    .setReadFragment(readFragmentBuilder.build())
                    .build();

            fragmentIdBuilder.setBucketNum(bucketId);
            key.datum(fragmentIdBuilder.build());
            value.datum(fragment);
            context.write(key, value);
        }
    }
}
