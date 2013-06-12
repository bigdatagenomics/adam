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

package edu.berkeley.cs.amplab.adam.modules.pileup;

import com.google.common.base.Optional;
import com.google.common.collect.RangeMap;
import junit.framework.Assert;
import org.junit.Test;

public class MdOpTest {

    @Test(expected = NullPointerException.class)
    public void testNullString() {
        MdOp.tokenize(null);
    }

    @Test
    public void testZeroLength() {
        Assert.assertTrue("Failed to get zero tokens for zero-length string",
                MdOp.tokenize("").asMapOfRanges().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonDigitIntialValue() {
        MdOp.tokenize("ACTG0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidBase() {
        MdOp.tokenize("0ACTZ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoDigitAtEnd() {
        MdOp.tokenize("0ACTG");
    }

    private void isType(MdOp.Type type, MdOp token) {
        Assert.assertEquals(type, token.getOpType());
    }

    private void isMatch(MdOp token) {
        isType(MdOp.Type.MATCH, token);
    }

    private void isMismatch(MdOp token, String mismatchedBase) {
        isType(MdOp.Type.MISMATCH, token);
        Assert.assertEquals(Optional.of(mismatchedBase), token.getBase());
    }

    private void isMissing(MdOp token, String missingBase) {
        isType(MdOp.Type.DELETION, token);
        Assert.assertEquals(Optional.of(missingBase), token.getBase());
    }

    @Test
    public void testValidStrings() {
        RangeMap<Long, MdOp> rangeMap = MdOp.tokenize("0A0");
        Assert.assertEquals(1, rangeMap.asMapOfRanges().size());
        MdOp token = rangeMap.get(0L);
        isMismatch(token, "A");

        Long numMatches = 100L;
        rangeMap = MdOp.tokenize(numMatches.toString());
        Assert.assertEquals(1, rangeMap.asMapOfRanges().size());
        for (int i = 0; i < numMatches; i++) {
            token = rangeMap.get((long)i);
            isMatch(token);
        }
        Assert.assertEquals(null, rangeMap.get(numMatches));

        long referenceStartPosition = 42;
        rangeMap = MdOp.tokenize(numMatches.toString(), referenceStartPosition);
        Assert.assertEquals(1, rangeMap.asMapOfRanges().size());
        for (int i = 0; i < numMatches; i++) {
            token = rangeMap.get(referenceStartPosition + i);
            isMatch(token);
        }

        rangeMap = MdOp.tokenize("100C2");
        Assert.assertEquals(3, rangeMap.asMapOfRanges().size());
        for (int i = 0; i < 100; i++) {
            isMatch(rangeMap.get((long)i));
        }
        isMismatch(rangeMap.get(100L), "C");
        isMatch(rangeMap.get(101L));
        isMatch(rangeMap.get(102L));

        rangeMap = MdOp.tokenize("100C0^C20");
        Assert.assertEquals(4, rangeMap.asMapOfRanges().size());
        for (int i = 0; i < 100; i++) {
            isMatch(rangeMap.get((long)i));
        }
        isMismatch(rangeMap.get(100L), "C");
        isMissing(rangeMap.get(101L), "C");
        isMatch(rangeMap.get(102L));
        isMatch(rangeMap.get(103L));

        String mismatchString = "ACGTACGTACGT";
        rangeMap = MdOp.tokenize("123" + mismatchString + "0");
        Assert.assertEquals(mismatchString.length() + 1, rangeMap.asMapOfRanges().size());
        for (int i = 0; i < 123; i++) {
            isMatch(rangeMap.get((long)i));
        }
        for (int i = 0; i < mismatchString.length(); i++) {
            isMismatch(rangeMap.get(123L + i), mismatchString.substring(i, i + 1));
        }
    }


}
