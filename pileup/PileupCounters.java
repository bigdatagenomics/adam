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

public enum PileupCounters {
    MALFORMED_REFERENCE_DESCRIPTION,
    MISSING_REFERENCE_DESCRIPTION,
    MISSING_REFERENCE_START,
    MISSING_REFERENCE_END,
    MISSING_REFERENCE_SEQUENCE,
    INVALID_REFERENCE_SEQUENCE_POSITION,
    MISSING_REFERENCE_NAME,
    MISSING_READ_NAME,
    MISSING_READ_START,
    MISSING_READ_SEQUENCE,
    MISSING_READ_QUALITY,
    UNKNOWN_MAPPING_QUALITY,
    MISSING_READ_MAPQ,
    MISSING_READ_CIGAR,
    INVALID_READ_CIGAR,
    MISSING_READ_MD_TAG,
    INVALID_READ_MD_TAG,
    CIGAR_MD_TAG_MISMATCH,
    READS_IGNORED,
    LOW_MAPQ_READS_IGNORED,
    INVALID_READS,
    FRAGMENT_WITHOUT_READ_OR_REFERENCE,
    FRAGMENT_READ_QUALITY_START_MISMATCH,
    FRAGMENT_READ_QUALITY_LENGTH_MISMATCH,
}