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

package edu.berkeley.amplab.adam.modules;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import edu.berkeley.amplab.adam.avro.SequenceFragment;

import java.util.Map;

/**
 * This partitioner is used to split sequences into partitions consistent buckets
 * based on position.
 */
public class SequencePartitioner {

    private long step;
    private Splitter sequenceSplitter;

    /**
     * @param step Defines the number of sequence positions to include in a single bucket
     */
    public SequencePartitioner(long step) {
        Preconditions.checkArgument(step > 0, "Step much be greater than zero");
        this.step = step;
        this.sequenceSplitter = Splitter.fixedLength(Ints.checkedCast(step)).omitEmptyStrings();
    }

    /**
     * Partitions a sequence
     *
     * @param start    The start position of the sequence
     * @param sequence The sequence to partition
     * @return a map of bucket IDs and sequence fragments
     */
    public Map<Long, SequenceFragment> partition(long start, String sequence) {
        Preconditions.checkArgument(start >= 0, "Start cannot be negative");
        long boundaryOffset = start % step;
        long bucketNum = start - boundaryOffset;
        Map<Long, SequenceFragment> answer = Maps.newHashMap();
        String subSequence;
        SequenceFragment.Builder fragmentBuilder = SequenceFragment.newBuilder();
        if (boundaryOffset != 0) {
            int partialFragmentLength = Math.min(sequence.length(),
                    Ints.checkedCast(step - boundaryOffset));
            answer.put(bucketNum, fragmentBuilder
                    .setFragment(sequence.substring(0, partialFragmentLength))
                    .setStart(start)
                    .build());
            bucketNum += step;
            subSequence = sequence.substring(partialFragmentLength);
        } else {
            subSequence = sequence;
        }
        Iterable<String> partitions = sequenceSplitter.split(subSequence);
        for (String partition : partitions) {
            answer.put(bucketNum, fragmentBuilder
                    .setFragment(partition)
                    .setStart(bucketNum)
                    .build());
            bucketNum += step;
        }
        return answer;
    }

}
