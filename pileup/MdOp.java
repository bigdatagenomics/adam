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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class parses the MD optional attribute in BAM files
 */
public class MdOp {

    static final Pattern DIGITS_PATTERN = Pattern.compile("\\d+");
    static final Pattern MISMATCHED_BASES_PATTERN = Pattern.compile("[AGCTN]+");
    static final Pattern DELETION_PATTERN = Pattern.compile("\\^[AGCTN]+");

    public static enum Type {
        MATCH,
        MISMATCH,
        DELETION
    }

    private Type opType;
    private Optional<String> base;

    private MdOp(Type opType, Optional<String> base) {
        Preconditions.checkArgument(base.isPresent() == (opType != Type.MATCH),
                "MD token type='%s' had a defined sequence? %s", opType, base.isPresent());
        this.opType = opType;
        this.base = base;
    }

    private static MdOp newMatchToken() {
        return new MdOp(Type.MATCH, Optional.<String>absent());
    }

    private static MdOp newMismatchToken(String mismatchedBase) {
        Preconditions.checkArgument(mismatchedBase.length() == 1);
        return new MdOp(Type.MISMATCH, Optional.of(mismatchedBase));
    }

    private static MdOp newMissingToken(String missingBase) {
        Preconditions.checkArgument(missingBase.length() == 1);
        return new MdOp(Type.DELETION, Optional.of(missingBase));
    }

    public Type getOpType() {
        return opType;
    }

    public Optional<String> getBase() {
        return base;
    }

    public String toString() {
        return Objects.toStringHelper(this)
                .add("type", opType)
                .add("base", base)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(opType, base);
    }

    private static void skipToNextToken(Matcher m, int end) {
        m.region(m.end(), end);
    }

    public static RangeMap<Long, MdOp> tokenize(String mdTagInput) {
        return tokenize(mdTagInput, 0);
    }

    public static RangeMap<Long, MdOp> tokenize(String mdTagInput, long referenceStartPosition) {
        Preconditions.checkNotNull(mdTagInput, "mdTagInput");
        // Normalize MD tag to uppercase...
        String mdTag = mdTagInput.toUpperCase();
        RangeMap<Long, MdOp> rangeMap = TreeRangeMap.create();
        if (mdTag.length() == 0) {
            return rangeMap;
        }
        int end = mdTag.length();

        Matcher m = DIGITS_PATTERN.matcher(mdTag);
        Preconditions.checkArgument(m.lookingAt(), "MD tag must start with a digit");

        int length = Integer.parseInt(m.group());
        long offset = referenceStartPosition;

        // Because the MD tag must start with a digit and you can have e.g. base mismatches
        // at the first position, the attribute is encoded with zero ('0') meaning "no matches".
        // We just ignore those.
        if (length > 0) {
            rangeMap.put(Range.<Long>closedOpen(offset, offset + length), MdOp.newMatchToken());
            offset += length;
        }
        skipToNextToken(m, end);

        while (m.regionStart() < end) {
            m.usePattern(MISMATCHED_BASES_PATTERN);
            if (m.lookingAt()) {
                // We found some bases that didn't match the reference
                String mismatchedBases = m.group();
                for (int i = 0; i < mismatchedBases.length(); i++) {
                    rangeMap.put(Range.<Long>closedOpen(offset, offset + 1), newMismatchToken(mismatchedBases.substring(i, i + 1)));
                    offset++;
                }
                skipToNextToken(m, end);
            } else {
                // Didn't find mismatched bases, try missing bases?
                m.usePattern(DELETION_PATTERN);
                if (m.lookingAt()) {
                    // Found missing bases.. string off the '^' character from group...
                    String missingBases = m.group().substring(1);
                    for (int i = 0; i < missingBases.length(); i++) {
                        rangeMap.put(Range.<Long>closedOpen(offset, offset + 1), newMissingToken(missingBases.substring(i, i + 1)));
                        offset++;
                    }
                    skipToNextToken(m, end);
                }
            }

            m.usePattern(DIGITS_PATTERN);
            Preconditions.checkArgument(m.lookingAt(),
                    "MD attribute should have matching bases after mismatched or missing bases. Found '%s' instead",
                    mdTag.substring(m.regionStart()));
            length = Integer.parseInt(m.group());
            // If the last elements are e.g. mismatched bases, then the last token will be a
            // zero representing... "no matching bases" at the end. Ignore it.
            if (length > 0) {
                rangeMap.put(Range.<Long>closedOpen(offset, offset + length), MdOp.newMatchToken());
                offset += length;
            }
            skipToNextToken(m, end);
        }
        return rangeMap;
    }

}
