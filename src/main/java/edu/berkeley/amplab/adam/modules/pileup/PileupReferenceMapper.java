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

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import edu.berkeley.amplab.adam.avro.PileupFragment;
import edu.berkeley.amplab.adam.avro.PileupFragmentId;
import edu.berkeley.amplab.adam.avro.ReferenceFragment;
import edu.berkeley.amplab.adam.avro.SequenceFragment;
import edu.berkeley.amplab.adam.modules.SequencePartitioner;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Mapper;
import parquet.hadoop.util.ContextUtil;

import java.io.IOException;
import java.util.Map;

public class PileupReferenceMapper extends Mapper<Void, GenericRecord,
        AvroKey<PileupFragmentId>, AvroValue<PileupFragment>> {

    private SequencePartitioner sequencePartitioner;
    private AvroKey<PileupFragmentId> key = new AvroKey<PileupFragmentId>();
    private AvroValue<PileupFragment> value = new AvroValue<PileupFragment>();
    private Splitter descriptionSplitter = Splitter.on("|");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        long step = ContextUtil.getConfiguration(context).getLong(Pileup.PILEUP_STEP, Pileup.PILEUP_STEP_DEFAULT);
        sequencePartitioner = new SequencePartitioner(step);
    }

    @Override
    protected void map(Void ignore, GenericRecord record, Context context) throws IOException, InterruptedException {
        CharSequence description = (CharSequence) record.get("description");
        if (description == null) {
            context.getCounter(PileupCounters.MISSING_REFERENCE_DESCRIPTION).increment(1);
            return;
        }
        String referenceName = Iterables.getFirst(descriptionSplitter.split(description), null);
        if (referenceName == null) {
            context.getCounter(PileupCounters.MALFORMED_REFERENCE_DESCRIPTION).increment(1);
            return;
        }

        Long start = (Long) record.get("start");
        if (start == null) {
            context.getCounter(PileupCounters.MISSING_REFERENCE_START).increment(1);
            return;
        }
        Long end = (Long) record.get("end");
        if (end == null) {
            context.getCounter(PileupCounters.MISSING_REFERENCE_END).increment(1);
            return;
        }
        CharSequence sequence = (CharSequence) record.get("sequence");
        if (sequence == null) {
            context.getCounter(PileupCounters.MISSING_REFERENCE_SEQUENCE).increment(1);
            return;
        }

        Map<Long, SequenceFragment> sequenceFragmentMap = sequencePartitioner.partition(start, sequence.toString());
        for (Long bucketId : sequenceFragmentMap.keySet()) {
            PileupFragmentId fragmentId = PileupFragmentId.newBuilder()
                    .setBucketNum(bucketId)
                    .setReferenceName(referenceName)
                    .build();
            ReferenceFragment referenceFragment = ReferenceFragment.newBuilder()
                    .setReferenceSequence(sequenceFragmentMap.get(bucketId))
                    .build();
            PileupFragment fragment = PileupFragment.newBuilder()
                    .setReferenceFragment(referenceFragment)
                    .build();
            key.datum(fragmentId);
            value.datum(fragment);
            context.write(key, value);
        }
    }
}
