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

import com.google.common.collect.ImmutableMap;
import edu.berkeley.amplab.adam.avro.SequenceFragment;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class SequencePartitionerTest {

    private void runTest(int step, long start, String sequence, Map<Long, SequenceFragment> expectedResult) {
        SequencePartitioner sp = new SequencePartitioner(step);
        Map<Long, SequenceFragment> answer = sp.partition(start, sequence);
        Assert.assertTrue(String.format("Expected: %s Found: %s", expectedResult.toString(), answer.toString()),
                expectedResult.equals(answer));
    }

    public SequenceFragment sf(long start, String sequence) {
        return SequenceFragment.newBuilder()
                .setFragment(sequence)
                .setStart(start)
                .build();
    }

    @Test
    public void testPartitioner() {
        runTest(10, 0, "", Collections.<Long, SequenceFragment>emptyMap());
        runTest(3, 0, "abcd", ImmutableMap.<Long, SequenceFragment>of(0L, sf(0L, "abc"), 3L, sf(3L, "d")));
        runTest(3, 1, "abcd", ImmutableMap.<Long, SequenceFragment>of(0L, sf(1L, "ab"), 3L, sf(3L, "cd")));
        runTest(3, 2, "abcd", ImmutableMap.<Long, SequenceFragment>of(0L, sf(2L, "a"), 3L, sf(3L, "bcd")));
        runTest(3, 3, "abcd", ImmutableMap.<Long, SequenceFragment>of(3L, sf(3L, "abc"), 6L, sf(6L, "d")));
        runTest(4, 0, "abcd", ImmutableMap.<Long, SequenceFragment>of(0L, sf(0L, "abcd")));
        runTest(10, 0, "abcd", ImmutableMap.<Long, SequenceFragment>of(0L, sf(0L, "abcd")));
        runTest(10, 1, "abcd", ImmutableMap.<Long, SequenceFragment>of(0L, sf(1L, "abcd")));
        runTest(1, 2, "abcd", ImmutableMap.<Long, SequenceFragment>of(
                2L, sf(2L, "a"), 3L, sf(3L, "b"), 4L, sf(4L, "c"), 5L, sf(5L, "d")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroStep() {
        SequencePartitioner sp = new SequencePartitioner(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeStep() {
        SequencePartitioner sp = new SequencePartitioner(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeStart() {
        SequencePartitioner sp = new SequencePartitioner(1);
        sp.partition(-1, "abcd");
    }
}
