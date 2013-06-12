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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.RangeMap;
import edu.berkeley.cs.amplab.adam.avro.*;
import net.sf.samtools.*;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import parquet.hadoop.util.ContextUtil;

import java.io.IOException;

public class PileupReadMapper extends Mapper<Void, ADAMRecord,
        AvroKey<ReferencePosition>, AvroValue<ADAMPileup>> {

    @VisibleForTesting
    static final TextCigarCodec CIGAR_CODEC = TextCigarCodec.getSingleton();

    private AvroKey<ReferencePosition> key = new AvroKey<ReferencePosition>();
    private AvroValue<ADAMPileup> value = new AvroValue<ADAMPileup>();

    private Long minMapq;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = ContextUtil.getConfiguration(context);
        minMapq = config.getLong(Pileup.MIN_MAPQ_CONFIG, Pileup.MIN_MAPQ_DEFAULT);
    }

    // Simple validation checks
    private boolean validRecord(ADAMRecord record, Context context) {
        // Record will be null if job predicate returns false
        if (record == null) {
            context.getCounter(PileupCounters.READS_IGNORED).increment(1);
            return false;
        }
        if (record.getMapq() == null) {
            context.getCounter(PileupCounters.MISSING_READ_MAPQ).increment(1);
            return false;
        }
        if (record.getMapq() == SAMRecord.UNKNOWN_MAPPING_QUALITY) {
            context.getCounter(PileupCounters.UNKNOWN_MAPPING_QUALITY).increment(1);
            return false;
        }
        if (record.getMapq() < minMapq) {
            context.getCounter(PileupCounters.LOW_MAPQ_READS_IGNORED).increment(1);
            return false;
        }
        if (record.getSequence() == null) {
            context.getCounter(PileupCounters.MISSING_READ_SEQUENCE).increment(1);
            return false;
        }
        if (record.getQual() == null) {
            context.getCounter(PileupCounters.MISSING_READ_QUALITY).increment(1);
            return false;
        }
        if (record.getSequence().length() != record.getQual().length()) {
            context.getCounter(PileupCounters.INVALID_READS).increment(1);
            return false;
        }
        if (record.getStart() == null) {
            context.getCounter(PileupCounters.MISSING_READ_START).increment(1);
            return false;
        }
        if (record.getReferenceName() == null) {
            context.getCounter(PileupCounters.MISSING_REFERENCE_NAME).increment(1);
            return false;
        }
        if (record.getReadName() == null) {
            context.getCounter(PileupCounters.MISSING_READ_NAME).increment(1);
            return false;
        }
        if (record.getCigar() == null) {
            context.getCounter(PileupCounters.MISSING_READ_CIGAR).increment(1);
            return false;
        }
        if (record.getMismatchingPositions() == null) {
            context.getCounter(PileupCounters.MISSING_READ_MD_TAG).increment(1);
            return false;
        }
        return true;
    }

    private Base stringToBase(String sequence, int index) {
        String base = sequence.substring(index, index + 1);
        return Base.valueOf(base.toUpperCase());
    }

    private int sangerQuality(String qualities, int index) {
        return qualities.charAt(index) - 33;
    }

    @Override
    protected void map(Void ignore, ADAMRecord record, Context context)
            throws IOException, InterruptedException {
        if (!validRecord(record, context)) {
            return;
        }

        // Parse the CIGAR string
        Cigar cigar;
        try {
            cigar = CIGAR_CODEC.decode((String) record.getCigar());
        } catch (IllegalArgumentException e) {
            context.getCounter(PileupCounters.INVALID_READ_CIGAR).increment(1);
            return;
        }

        // Parse the MD tag
        RangeMap<Long, MdOp> mdOps;
        try {
            mdOps = MdOp.tokenize((String) record.getMismatchingPositions(), record.getStart());
        } catch (IllegalArgumentException e) {
            context.getCounter(PileupCounters.INVALID_READ_MD_TAG).increment(1);
            return;
        }

        long referencePosition = record.getStart();
        boolean isReverseStrand = record.getReadNegativeStrand();
        String sequence = (String) record.getSequence();
        String qualities = (String) record.getQual();

        // Initialize builders and set info that is constant across the read
        ReferencePosition.Builder positionBuilder = ReferencePosition.newBuilder()
                .setId(record.getReferenceId());
        ADAMPileup.Builder pileupBuilder = ADAMPileup.newBuilder()
                //.setReferenceName(record.getReferenceName())
                .setReferenceId(record.getReferenceId())
                //.setReadName(record.getReadName())
                .setMapQuality(record.getMapq());

        int readPosition = 0;
        for (CigarElement element : cigar.getCigarElements()) {

            switch (element.getOperator()) {

                case I: // insertion to the reference
                    positionBuilder
                            .setPosition(referencePosition);
                    pileupBuilder
                            .setPosition(referencePosition)
                            .setInsertedSequence(sequence.substring(readPosition, readPosition + element.getLength()))
                            .setOp(ADAMPileupOp.INSERTION)
                            .setReferenceBase(null)
                            .setSangerQuality(sangerQuality(qualities, readPosition));
                    key.datum(positionBuilder.build());
                    value.datum(pileupBuilder.build());
                    context.write(key, value);
                    // Clear the insert sequence after writing
                    pileupBuilder.setInsertedSequence(null);
                    // Consume the read bases but NOT the reference bases
                    readPosition += element.getLength();
                    break;

                case M: // alignment match (sequence match or mismatch)
                    for (int i = 0; i < element.getLength(); i++) {
                        positionBuilder.setPosition(referencePosition);
                        pileupBuilder.setPosition(referencePosition);
                        MdOp mdOp = mdOps.get(referencePosition);
                        if (mdOp == null) {
                            throw new IllegalStateException("CIGAR match with no MD tag information");
                        }
                        if (mdOp.getOpType() == MdOp.Type.MATCH) {
                            pileupBuilder
                                    .setOp(isReverseStrand ? ADAMPileupOp.MATCH_REVERSE_STRAND : ADAMPileupOp.MATCH)
                                    .setReferenceBase(stringToBase(sequence, readPosition));
                        } else if (mdOp.getOpType() == MdOp.Type.MISMATCH) {
                            pileupBuilder
                                    .setOp(isReverseStrand ? ADAMPileupOp.MISMATCH_REVERSE_STRAND : ADAMPileupOp.MISMATCH)
                                    .setReferenceBase(stringToBase(mdOp.getBase().get(), 0))
                                    .setReadBase(stringToBase(sequence, readPosition));
                        } else {
                            // MD tag should only a MATCH/MISMATCH when the cigar is a match
                            throw new IllegalStateException("CIGAR match with MD op: " + mdOp);
                        }
                        pileupBuilder.setSangerQuality(sangerQuality(qualities, readPosition));
                        key.datum(positionBuilder.build());
                        value.datum(pileupBuilder.build());
                        context.write(key, value);
                        // Consume the read bases and the reference bases
                        readPosition++;
                        referencePosition++;
                    }
                    break;

                case D: // deletion from the reference
                    for (int i = 0; i < element.getLength(); i++) {
                        MdOp mdOp = mdOps.get(referencePosition);
                        if (mdOp.getOpType() != MdOp.Type.DELETION) {
                            throw new IllegalStateException("CIGAR delete but MD op: " + mdOp);
                        }
                        positionBuilder
                                .setPosition(referencePosition);
                        pileupBuilder
                                .setPosition(referencePosition)
                                .setOp(ADAMPileupOp.DELETION)
                                .setReferenceBase(stringToBase(mdOp.getBase().get(), 0))
                                .setSangerQuality(null);
                        key.datum(positionBuilder.build());
                        value.datum(pileupBuilder.build());
                        context.write(key, value);
                        // Consume reference bases but not read bases
                        referencePosition++;
                    }
                    break;

                case EQ: // sequence match
                case X: // sequence mismatch
                    throw new UnsupportedOperationException("TODO: support =/X MD tags");

                case S: // soft clipping
                case P: // padding (silent deletion from padded reference)
                case N: // skipped region from the reference
                case H: // hard clipping
                    if (element.getOperator().consumesReadBases()) {
                        readPosition += element.getLength();
                    }
                    if (element.getOperator().consumesReferenceBases()) {
                        referencePosition += element.getLength();
                    }
                    break;
            }
        }
    }
}
