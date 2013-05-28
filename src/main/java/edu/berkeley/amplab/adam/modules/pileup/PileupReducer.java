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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import edu.berkeley.amplab.adam.avro.*;
import edu.berkeley.amplab.adam.converters.SpecificRecordConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class PileupReducer extends Reducer<AvroKey<PileupFragmentId>, AvroValue<PileupFragment>,
        Void, GenericRecord> {

    private SpecificRecordConverter<ADAMPileup> pileupConverter = new SpecificRecordConverter<ADAMPileup>();

    private class PileupBuildSupport {
        private ADAMPileup.Builder pileupBuilder;
        private StringBuilder pileupStringBuilder;
        private StringBuilder qualitiesStringBuilder;

        private PileupBuildSupport(long position) {
            pileupBuilder = ADAMPileup.newBuilder();
            pileupBuilder.setPosition(position);
            pileupStringBuilder = new StringBuilder();
            qualitiesStringBuilder = new StringBuilder();
        }

        // TODO: keep the read base position constant
        private void appendBase(CharSequence base) {
            pileupStringBuilder.append(base);
        }

        // TODO: keep the read base position constant
        private void appendQual(CharSequence qual) {
            qualitiesStringBuilder.append(qual);
        }

        private void setReferenceBase(CharSequence referenceBase) {
            Preconditions.checkArgument(referenceBase.length() == 1);
            pileupBuilder.setReferenceBase(referenceBase);
        }

        private void setReferenceName(CharSequence referenceName) {
            pileupBuilder.setReferenceName(referenceName);
        }

        private ADAMPileup getPileup() {
            return pileupBuilder
                    .setQualities(qualitiesStringBuilder.toString())
                    .setPileup(pileupStringBuilder.toString())
                    .build();
        }
    }

    @Override
    protected void reduce(AvroKey<PileupFragmentId> id, Iterable<AvroValue<PileupFragment>> fragments,
                          Context context) throws IOException, InterruptedException {
        // Map of position -> pileup
        Map<Long, PileupBuildSupport> positionPileupMap = Maps.newTreeMap();
        for (AvroValue<PileupFragment> avroFragment : fragments) {
            PileupFragment fragment = avroFragment.datum();
            ReadFragment readFragment = fragment.getReadFragment();
            ReferenceFragment referenceFragment = fragment.getReferenceFragment();
            if (readFragment != null) {
                // Process the read fragment by position
                SequenceFragment readSequence = readFragment.getSequence();
                SequenceFragment readQuals = readFragment.getBaseQuals();
                if (!readSequence.getStart().equals(readQuals.getStart())) {
                    context.getCounter(PileupCounters.FRAGMENT_READ_QUALITY_START_MISMATCH).increment(1);
                    return;
                }
                if (readSequence.getFragment().length() != readQuals.getFragment().length()) {
                    context.getCounter(PileupCounters.FRAGMENT_READ_QUALITY_LENGTH_MISMATCH).increment(1);
                    return;
                }
                Long start = readSequence.getStart();
                int fragmentLength = readSequence.getFragment().length();
                for (int i = 0; i < fragmentLength; i++) {
                    long position = start + i;
                    PileupBuildSupport pileupBuildSupport = positionPileupMap.get(position);
                    if (pileupBuildSupport == null) {
                        pileupBuildSupport = new PileupBuildSupport(position);
                        positionPileupMap.put(position, pileupBuildSupport);
                    }
                    pileupBuildSupport.appendBase(readSequence.getFragment().subSequence(i, i + 1));
                    pileupBuildSupport.appendQual(readQuals.getFragment().subSequence(i, i + 1));
                }
            } else if (referenceFragment != null) {
                // Process the reference fragment by position
                SequenceFragment referenceSequence = referenceFragment.getReferenceSequence();
                Long start = referenceSequence.getStart();
                for (int i = 0; i < referenceSequence.getFragment().length(); i++) {
                    long position = start + i;
                    PileupBuildSupport pileupBuildSupport = positionPileupMap.get(position);
                    if (pileupBuildSupport == null) {
                        pileupBuildSupport = new PileupBuildSupport(position);
                        positionPileupMap.put(position, pileupBuildSupport);
                    }
                    pileupBuildSupport.setReferenceBase(referenceSequence.getFragment().subSequence(i, i + 1));
                    pileupBuildSupport.setReferenceName(id.datum().getReferenceName());
                }
            } else {
                context.getCounter(PileupCounters.FRAGMENT_WITHOUT_READ_OR_REFERENCE).increment(1);
            }
        }
        for (Map.Entry<Long, PileupBuildSupport> entry : positionPileupMap.entrySet()) {
            GenericRecord record = pileupConverter.apply(entry.getValue().getPileup());
            context.write(null, record);
        }
    }
}
